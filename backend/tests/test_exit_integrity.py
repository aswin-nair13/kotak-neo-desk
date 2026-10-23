"""Plan 06 WP4: cancel/exit integrity (no stuck CLOSING, no double exit).

The market exit is no longer gated solely on the feed's `cancelled` frame: the
SL is confirmed terminal in the order book (with retries/backoff) before exactly
one market exit is placed. A cancel that never confirms ends ERRORED with
EXIT_BLOCKED_SL_STILL_WORKING and never fires an exit while an SL may still be
working. Time is fake so the confirmation loop is instant.
"""
import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings
from app.routes import orders as orders_routes
from app.schemas import ForceExitBody
from app.synthetic_engine import SyntheticEngine


class StreamReset(RuntimeError):
    pass


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def sleep(self, sec):
        self.t += max(0.0, float(sec))


def _book_order(oid, st="open", prc="100.00"):
    return {"nOrdNo": oid, "ordSt": st, "trdSym": "X", "trnsTp": "S",
            "qty": 20, "prc": prc, "fldQty": 0, "rejRsn": "--"}


def _leg(status="CLOSING", sl="SL1", **over):
    d = {"entry_order_id": "E1", "sl_order_id": sl,
         "trading_symbol": "X", "status": status, "strategy": "market_exit",
         "target_price": "120.00", "stop_loss_price": "90.00",
         "trail_distance": 5, "high_water_mark": 0.0, "low_water_mark": 0.0,
         "quantity": "20", "exchange_segment": "nse_fo",
         "instrument_token": "tok", "side": "B", "product": "MIS",
         "target_points": "20", "stop_points": "10", "fill_price": "100.00",
         "sl_quantity": 20, "sl_placed": True}
    d.update(over)
    return d


class ExitBroker:
    def __init__(self, book, on_cancel=None, exit_status="open"):
        self.book = book
        self.on_cancel = on_cancel
        self.exit_status = exit_status
        self.cancel_calls = []
        self.exits = []
        self.place_calls = 0

    def cancel(self, oid):
        self.cancel_calls.append(oid)
        if self.on_cancel:
            self.on_cancel(self, oid)
        return {"stCode": 200}

    def place_market_exit(self, exchange, qty, symbol, **kwargs):
        self.place_calls += 1
        oid = f"X{len(self.exits) + 1}"
        self.exits.append(oid)
        self.book.append(_book_order(oid, st=self.exit_status))
        return {"stCode": 200, "nOrdNo": oid}

    def place_stop_loss(self, *_a, **_kw):  # pragma: no cover - not used here
        return {"stCode": 200, "nOrdNo": "SLX"}

    def average_fill_price(self, _oid):
        return None


@pytest.fixture()
def fast_exit(monkeypatch):
    monkeypatch.setattr(settings, "exit_confirm_timeout_sec", 5.0)
    monkeypatch.setattr(settings, "exit_confirm_poll_sec", 1.0)
    monkeypatch.setattr(settings, "exit_confirm_retries", 2)


def _engine(broker, clock):
    return SyntheticEngine(broker, book_fetcher=lambda: (broker.book, True),
                           sleep_fn=clock.sleep, clock_fn=clock)


def test_cancel_never_confirms_blocks_without_exit(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="open")]

    def always_fail(_broker, _oid):
        raise StreamReset("HTTP/2 stream reset")

    broker = ExitBroker(book, on_cancel=always_fail)
    engine = _engine(broker, clock)
    engine.add(_leg())

    # Non-blocking: each reconcile advances the backoff schedule by one step.
    summary = engine.reconcile(book, {})
    for _ in range(6):
        if engine.orders["E1"]["status"] == "ERRORED":
            break
        clock.sleep(1.0)
        summary = engine.reconcile(book, {})
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "EXIT_BLOCKED_SL_STILL_WORKING"
    assert broker.exits == []                      # never exited while SL may work
    assert len(broker.cancel_calls) == 3           # 1 attempt + 2 retries
    assert summary["errored"] == ["E1"]


def test_stream_reset_retry_then_confirmed_cancel_exits_once(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="open")]

    def reset_once_then_cancel(broker, _oid):
        if len(broker.cancel_calls) == 1:
            raise StreamReset("first cancel reset")
        for row in book:
            if row["nOrdNo"] == "SL1":
                row["ordSt"] = "cancelled"

    broker = ExitBroker(book, on_cancel=reset_once_then_cancel)
    engine = _engine(broker, clock)
    engine.add(_leg())

    engine.reconcile(book, {})
    assert engine.orders["E1"]["status"] == "CLOSING"   # first cancel reset
    clock.sleep(1.0)
    engine.reconcile(book, {})
    leg = engine.orders["E1"]
    assert len(broker.exits) == 1
    assert leg["exit_order_id"] == "X1"
    assert leg["status"] == "CLOSING"

    # Duplicate cancelled frames (confirm + snapshot) must not add an exit.
    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "cancelled"})
    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "cancelled"})
    assert len(broker.exits) == 1
    assert broker.place_calls == 1


def test_sl_completes_during_cancel_no_market_exit(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="open")]

    def completes(broker, _oid):
        for row in broker.book:
            if row["nOrdNo"] == "SL1":
                row["ordSt"] = "complete"

    broker = ExitBroker(book, on_cancel=completes)
    engine = _engine(broker, clock)
    engine.add(_leg())

    engine.reconcile(book, {})
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_reason"] == "SL_HIT"
    assert broker.exits == []
    assert broker.place_calls == 0


def test_duplicate_cancelled_frames_fire_exactly_one_exit():
    engine = SyntheticEngine(ExitBroker([]))
    engine.add(_leg())
    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "cancelled"})
    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "cancelled"})
    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "cancelled"})
    assert engine.broker.place_calls == 1
    assert engine.orders["E1"]["exit_order_id"] == "X1"


def test_stuck_closing_driven_by_reconcile_then_exit_completes(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="open")]

    def cancels(broker, _oid):
        for row in broker.book:
            if row["nOrdNo"] == "SL1":
                row["ordSt"] = "cancelled"

    broker = ExitBroker(book, on_cancel=cancels)
    engine = _engine(broker, clock)
    engine.add(_leg())

    engine.reconcile(book, {})
    assert len(broker.exits) == 1
    assert engine.orders["E1"]["exit_order_id"] == "X1"

    # Exit fills later; the next reconcile resolves CLOSING -> CLOSED.
    for row in book:
        if row["nOrdNo"] == "X1":
            row["ordSt"] = "complete"
    engine.reconcile(book, {})
    assert engine.orders["E1"]["status"] == "CLOSED"


def test_force_exit_endpoint_is_audited_and_idempotent(fast_exit, monkeypatch, caplog):
    clock = FakeClock()
    book = [_book_order("SL1", st="open")]

    def cancels(broker, _oid):
        for row in broker.book:
            if row["nOrdNo"] == "SL1":
                row["ordSt"] = "cancelled"

    broker = ExitBroker(book, on_cancel=cancels)
    engine = _engine(broker, clock)
    engine.add(_leg(status="ERRORED", blocked_reason="EXIT_BLOCKED_SL_STILL_WORKING",
                    naked_position=True, error="blocked"))
    monkeypatch.setattr(orders_routes, "engine", engine)
    monkeypatch.setattr(orders_routes, "worker", None)

    caplog.set_level(logging.WARNING)
    body = ForceExitBody(entry_order_id="E1", reason="operator confirmed flatten")
    out = orders_routes.force_exit(body, user="alice")
    assert out["ok"] is True
    assert out["result"] in ("exit_placed", "exit_pending")
    assert broker.place_calls == 1
    leg = engine.orders["E1"]
    audit = leg["force_exit_audit"]
    assert audit["user"] == "alice"
    assert audit["reason"] == "operator confirmed flatten"
    assert audit["entry_order_id"] == "E1"
    assert audit["at_ms"] > 0
    assert any("AUDIT force_exit" in r.message for r in caplog.records)

    # Second operator click while the exit is still working: no second exit,
    # and pending is not reported as success.
    out2 = orders_routes.force_exit(body, user="alice")
    assert out2.status_code == 409
    assert broker.place_calls == 1
    assert engine.orders["E1"]["exit_order_id"] == "X1"


# ---------- independent review follow-ups ----------
def test_exit_cancelled_marks_errored_not_stuck_closing(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="cancelled"), _book_order("X1", st="cancelled")]
    broker = ExitBroker(book)
    engine = _engine(broker, clock)
    engine.add(_leg(exit_order_id="X1", exit_pending=True))

    engine.reconcile(book, {})
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "EXIT_CANCELLED"
    assert leg["naked_position"] is True
    assert broker.place_calls == 0


def test_force_exit_retries_once_after_rejected_exit(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="cancelled"),
            _book_order("X9", st="rejected")]
    broker = ExitBroker(book)
    engine = _engine(broker, clock)
    engine.add(_leg(status="ERRORED", blocked_reason="EXIT_REJECTED",
                    naked_position=True, exit_order_id="X9",
                    exit_client_tag="old-tag"))

    out = engine.force_exit("E1", "alice", "retry rejected exit")
    leg = engine.orders["E1"]
    assert out["force_exit_result"] in ("placed", "exit_pending")
    assert broker.place_calls == 1
    assert leg["exit_order_id"] == "X1"
    assert leg["force_exit_audit"]["exit_order_id"] == "X1"
    assert leg["naked_position"] is False


def test_force_exit_does_not_double_exit_working_order(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="cancelled"), _book_order("X1", st="open")]
    broker = ExitBroker(book)
    engine = _engine(broker, clock)
    engine.add(_leg(exit_order_id="X1", exit_pending=True))

    out = engine.force_exit("E1", "alice", "already working")
    assert out["force_exit_result"] == "exit_pending"
    assert broker.place_calls == 0
    assert engine.orders["E1"]["naked_position"] is False


def test_ambiguous_exit_adopts_tag_instead_of_duplicating(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="open")]

    class TimeoutBroker(ExitBroker):
        def place_market_exit(self, exchange, qty, symbol, **kwargs):
            self.place_calls += 1
            oid = f"X{len(self.exits) + 1}"
            self.exits.append(oid)
            row = _book_order(oid, st="complete")
            row["guiOrdId"] = kwargs.get("tag")
            self.book.append(row)
            raise TimeoutError("response lost after accept")

    def cancels(broker, _oid):
        for row in broker.book:
            if row["nOrdNo"] == "SL1":
                row["ordSt"] = "cancelled"

    broker = TimeoutBroker(book, on_cancel=cancels)
    engine = _engine(broker, clock)
    engine.add(_leg())

    engine.reconcile(book, {})
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "EXIT_AMBIGUOUS"
    assert leg.get("exit_order_id") is None

    # The accepted order is found by its client tag: adopt it, never re-fire.
    engine.reconcile(book, {})
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_order_id"] == "X1"
    assert broker.place_calls == 1


def test_force_after_unresolved_ambiguous_uses_fresh_tag(fast_exit, monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    clock = FakeClock()
    tags = []

    class TagExitBroker(ExitBroker):
        def place_market_exit(self, exchange, qty, symbol, **kwargs):
            tags.append(kwargs.get("tag"))
            return super().place_market_exit(exchange, qty, symbol, **kwargs)

    broker = TagExitBroker([_book_order("SL1", st="cancelled")])
    engine = _engine(broker, clock)
    engine.add(_leg(status="ERRORED", blocked_reason="EXIT_AMBIGUOUS",
                    exit_ambiguous=True, exit_client_tag="XE1-1",
                    exit_attempts=1, naked_position=False))

    out = engine.force_exit("E1", "alice", "resolve ambiguous")
    assert out["force_exit_result"] in ("placed", "exit_pending")
    assert broker.place_calls == 1
    assert tags == ["XE1-2"]


def test_exit_absent_from_book_errors_after_confirmations(fast_exit, monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 2)
    clock = FakeClock()
    book = [_book_order("SL1", st="cancelled")]
    broker = ExitBroker(book)
    engine = _engine(broker, clock)
    engine.add(_leg(exit_order_id="X9", exit_pending=True))

    engine.reconcile(book, {})
    assert engine.orders["E1"]["status"] == "CLOSING"
    engine.reconcile(book, {})
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "EXIT_UNKNOWN"
    assert leg["naked_position"] is True
    assert broker.place_calls == 0


def test_bridge_book_unavailable_errors_closing_exit_after_n(fast_exit, monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 2)
    clock = FakeClock()
    broker = ExitBroker([])
    engine = _engine(broker, clock)
    engine.add(_leg(exit_order_id="X1", exit_pending=True))

    engine.note_book_unavailable()
    assert engine.orders["E1"]["exit_unknown_streak"] == 1
    assert engine.orders["E1"].get("exit_unknown_alerted_at_ms") is None
    assert engine.orders["E1"]["status"] == "CLOSING"
    engine.note_book_unavailable()
    leg = engine.orders["E1"]
    assert leg["exit_unknown_streak"] == 2
    assert leg["exit_unknown_alerted_at_ms"] > 0
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "EXIT_UNKNOWN"
    assert leg["naked_position"] is True
    assert broker.place_calls == 0
    engine.note_book_unavailable()
    assert engine.orders["E1"]["blocked_reason"] == "EXIT_UNKNOWN"


def test_force_resets_exhausted_cancel_budget(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="open")]
    broker = ExitBroker(book)
    engine = _engine(broker, clock)
    engine.add(_leg(status="ERRORED",
                    blocked_reason="EXIT_BLOCKED_SL_STILL_WORKING",
                    exit_cancel_attempts=3, exit_next_action_s=clock.t,
                    exit_deadline_s=clock.t - 10, naked_position=True))

    out = engine.force_exit("E1", "alice", "retry after exhaustion")
    leg = engine.orders["E1"]
    assert out["force_exit_result"] == "cancel_pending"
    assert broker.cancel_calls == ["SL1"]
    assert leg["exit_cancel_attempts"] == 1
    assert leg["exit_deadline_s"] > clock.t


def test_late_exit_after_closed_is_cancelled_not_attached(fast_exit):
    clock = FakeClock()
    broker = ExitBroker([])
    engine = _engine(broker, clock)
    engine.add(_leg(status="CLOSED", exit_client_tag="XE1-1",
                    exit_attempts=1, exit_order_id=None))

    engine.on_order_update({
        "nOrdNo": "X9", "ordSt": "open", "trdSym": "X", "trnsTp": "S",
        "qty": 20, "prc": "0", "fldQty": 0, "guiOrdId": "XE1-1"})
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_order_id"] is None
    assert broker.cancel_calls == ["X9"]


def test_terminal_exit_update_after_closed_does_not_reopen_leg(fast_exit):
    clock = FakeClock()
    broker = ExitBroker([])
    engine = _engine(broker, clock)
    engine.add(_leg(status="CLOSED", exit_order_id="X1", exit_pending=False))

    engine.on_order_update({"nOrdNo": "X1", "ordSt": "cancelled", "trdSym": "X",
                            "trnsTp": "S", "qty": 20, "prc": "0", "fldQty": 0})
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["blocked_reason"] is None
    assert broker.cancel_calls == []


def test_book_unavailable_alerts_then_errors_after_n(fast_exit, monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 2)
    clock = FakeClock()
    broker = ExitBroker([])
    engine = SyntheticEngine(broker, book_fetcher=lambda: (None, False),
                             sleep_fn=clock.sleep, clock_fn=clock)
    engine.add(_leg(exit_order_id="X1", exit_pending=True))

    engine._drive_exit(engine.orders["E1"])
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSING"
    assert leg.get("exit_unknown_alerted_at_ms") is None
    engine._drive_exit(engine.orders["E1"])
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "EXIT_UNKNOWN"
    assert leg["exit_unknown_alerted_at_ms"] > 0
    assert broker.place_calls == 0


def test_late_sl_complete_detected_when_exit_working(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="complete"), _book_order("X1", st="open")]
    broker = ExitBroker(book)
    engine = _engine(broker, clock)
    engine.add(_leg(exit_order_id="X1", exit_pending=True))

    engine.reconcile(book, {})
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_reason"] == "SL_HIT"
    assert broker.cancel_calls == ["X1"]


def test_cancel_pending_does_not_wait_and_feed_frames_still_apply(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="open")]
    broker = ExitBroker(book)
    engine = _engine(broker, clock)
    engine.add(_leg())
    engine.add(_leg(status="PENDING_ENTRY", entry_order_id="E2", sl=None, exit_order_id=None,
                    exit_pending=False, sl_placed=False))

    engine.reconcile(book, {})
    assert engine.orders["E1"]["status"] == "CLOSING"
    assert engine.orders["E1"]["exit_state"] == "CANCEL_PENDING"
    assert broker.exits == []
    assert clock.t == 1000.0                       # no sleep/poll loop

    engine.on_order_update({"nOrdNo": "E2", "ordSt": "complete", "prc": "100.00",
                            "fldQty": 20})
    assert engine.orders["E2"]["status"] == "ACTIVE"
    assert engine.orders["E2"]["sl_order_id"] == "SLX"


def test_closing_missing_tracker_fields_errors_instead_of_keyerror(fast_exit):
    clock = FakeClock()
    broker = ExitBroker([])
    engine = _engine(broker, clock)
    leg = _leg()
    del leg["exchange_segment"]
    engine.add(leg)

    engine.reconcile([], {})
    out = engine.orders["E1"]
    assert out["status"] == "ERRORED"
    assert out["blocked_reason"] == "LEG_INVALID"
    assert broker.place_calls == 0


def test_reconcile_summary_includes_order_feed_errored_legs(monkeypatch):
    clock = FakeClock()
    broker = ExitBroker([])
    engine = _engine(broker, clock)
    engine.add(_leg(status="PENDING_ENTRY", entry_order_id="E1", sl=None, exit_order_id=None,
                    exit_pending=False))
    summary = engine.reconcile(
        [{"nOrdNo": "E1", "ordSt": "rejected", "rejRsn": "RMS"}], {})
    assert engine.orders["E1"]["status"] == "ERRORED"
    assert summary["errored"] == ["E1"]

def test_force_exit_never_flattens_an_unfilled_entry(fast_exit, monkeypatch):
    clock = FakeClock()
    broker = ExitBroker([])
    engine = _engine(broker, clock)
    engine.add(_leg(status="PENDING_ENTRY", sl=None, exit_order_id=None,
                    exit_pending=False, sl_placed=False, fill_price=None,
                    sl_quantity=None))
    monkeypatch.setattr(orders_routes, "engine", engine)

    out = orders_routes.force_exit(
        ForceExitBody(entry_order_id="E1", reason="operator click"), user="alice")
    assert out.status_code == 409
    assert b"no_position" in out.body
    assert broker.place_calls == 0


def test_dead_feed_cancel_rearms_poll_and_retries_before_periodic(fast_exit):
    clock = FakeClock()
    book = [_book_order("SL1", st="open")]
    broker = ExitBroker(book)
    engine = _engine(broker, clock)
    polls = []
    engine.exit_poll_requester = polls.append
    engine.add(_leg(status="CLOSING", exit_order_id=None, exit_pending=False))

    def step():
        return engine._drive_exit(engine.orders["E1"], book, True)

    assert step() == "cancel_pending"
    assert broker.cancel_calls == ["SL1"]
    assert polls and polls[-1] > 0.4            # poll at the backoff, not now

    clock.sleep(polls[-1])
    assert step() == "cancel_pending"
    assert len(broker.cancel_calls) == 2        # second attempt without 30s wait

    clock.sleep(polls[-1])
    assert step() == "cancel_pending"
    assert len(broker.cancel_calls) == 3

    clock.sleep(polls[-1])
    assert step() == "blocked"
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "EXIT_BLOCKED_SL_STILL_WORKING"
    assert leg["exit_cancel_attempts"] == 3
    assert all(delay > 0 for delay in polls)


def test_book_unavailable_errors_closing_without_exit_id(monkeypatch, caplog):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 2)
    clock = FakeClock()
    broker = ExitBroker([])
    engine = _engine(broker, clock)
    engine.add(_leg(status="CLOSING", exit_order_id=None, exit_pending=False))
    caplog.set_level(logging.ERROR)

    engine.note_book_unavailable()
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSING"
    assert leg["exit_unknown_streak"] == 1
    assert leg.get("exit_unknown_alerted_at_ms") is None

    engine.note_book_unavailable()
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "EXIT_UNKNOWN"
    assert leg["exit_unknown_alerted_at_ms"] > 0
    assert leg["naked_position"] is True
    assert broker.place_calls == 0
    messages = [r.message for r in caplog.records]
    assert any("no exit id yet" in m for m in messages)
    assert not any("no new exit will be placed" in m for m in messages)


def test_force_exit_on_closed_leg_without_exit_id_is_success(fast_exit, monkeypatch):
    clock = FakeClock()
    broker = ExitBroker([])
    engine = _engine(broker, clock)
    engine.add(_leg(status="CLOSED", exit_order_id=None, exit_pending=False))
    monkeypatch.setattr(orders_routes, "engine", engine)
    monkeypatch.setattr(orders_routes, "worker", None)

    out = orders_routes.force_exit(
        ForceExitBody(entry_order_id="E1", reason="already flat"), user="alice")
    assert out["ok"] is True
    assert out["result"] == "closed"
    assert out["leg"]["force_exit_result"] == "closed"
    assert broker.place_calls == 0


def test_reconcile_cancels_tagged_working_exit_for_closed(fast_exit, caplog):
    clock = FakeClock()
    row = _book_order("X9", st="open")
    row["guiOrdId"] = "XE1-1"
    book = [row]
    broker = ExitBroker(book)
    engine = _engine(broker, clock)
    engine.add(_leg(status="CLOSED", exit_order_id=None, exit_client_tag="XE1-1",
                    exit_pending=False))
    caplog.set_level(logging.WARNING)

    engine.reconcile(book, {}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["late_exit_risk"] is True
    assert broker.cancel_calls == ["X9"]          # exactly once across both paths
    assert broker.place_calls == 0
    assert "X9" in (leg.get("cancel_requested_ids") or {})
    messages = [r.message for r in caplog.records]
    assert any("LATE EXIT RISK" in m for m in messages)
    assert not any("ORPHAN TAGGED ORDER" in m for m in messages)

    # A second reconcile while the book still echoes the stale order must not
    # cancel it again (bounded alert only).
    engine.reconcile(book, {}, positions_ok=True)
    assert broker.cancel_calls == ["X9"]

def test_ambiguous_exit_log_redacts_credentials(caplog):
    class LeakyBroker(ExitBroker):
        def place_market_exit(self, exchange, qty, symbol, **kwargs):
            raise RuntimeError(
                "trade_token=super-secret sessionToken=st-raw totp=123456")

    clock = FakeClock()
    broker = LeakyBroker([_book_order("SL1", st="cancelled")])
    engine = _engine(broker, clock)
    engine.add(_leg(status="ACTIVE"))
    caplog.set_level(logging.ERROR)

    assert engine._place_exit(engine.orders["E1"]) == "ambiguous"
    assert "super-secret" not in caplog.text
    assert "st-raw" not in caplog.text
    assert "123456" not in caplog.text
