"""Plan 06 WP3: SL guarantee / naked-position guard.

Covers the incident shape: accepted entries whose fills never arrive on the
feed are repaired from the order book, missing/dead SLs are re-placed at most
once, and a naked leg is explicit (ERRORED + naked_position) instead of silently
unprotected. Also covers the reverse gap: an SL that reappears later heals the
leg and resets the missing streak.
"""
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings
from app.synthetic_engine import SyntheticEngine


def _leg(eid, token="tok", status="PENDING_ENTRY", sl=None, **over):
    d = {"entry_order_id": eid, "sl_order_id": sl,
         "trading_symbol": "X", "status": status, "strategy": "market_exit",
         "target_price": "120.00", "stop_loss_price": "90.00",
         "trail_distance": 5, "high_water_mark": 0.0, "low_water_mark": 0.0,
         "quantity": "20", "exchange_segment": "nse_fo",
         "instrument_token": token, "side": "B", "product": "MIS",
         "target_points": "20", "stop_points": "10"}
    d.update(over)
    return d


def _book_order(oid, st="complete", prc="100.00", qty=20, fldQty=None):
    return {"nOrdNo": oid, "ordSt": st, "trdSym": "X", "trnsTp": "B",
            "qty": qty, "prc": prc,
            "fldQty": qty if fldQty is None else fldQty, "rejRsn": "--"}


class SlBroker:
    def __init__(self, fail_place=False):
        self.placed = []
        self.cancels = []
        self.fail_place = fail_place

    def place_stop_loss(self, *a, **kw):
        if self.fail_place:
            return {"stCode": 500, "msg": "SL rejected"}
        oid = f"SL{len(self.placed) + 1}"
        self.placed.append(oid)
        return {"stCode": 200, "nOrdNo": oid}

    def average_fill_price(self, _oid):
        return None

    def cancel(self, oid):
        self.cancels.append(oid)
        return {"stCode": 200}


def test_dead_feed_six_fills_get_six_sls_exactly_once():
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    for i in range(6):
        engine.add(_leg(f"E{i}", token=f"tok{i}"))

    book = [_book_order(f"E{i}") for i in range(6)]
    summary = engine.reconcile(book, {})

    legs = engine.all()
    assert all(o["status"] == "ACTIVE" and o["sl_order_id"] for o in legs)
    assert len({o["sl_order_id"] for o in legs}) == 6
    assert len(broker.placed) == 6
    assert summary["checked"] == 6
    assert all(o["last_reconcile_ms"] for o in legs)
    assert all(o["sl_guarantee_due_ms"] for o in legs)

    # A second reconcile with the SLs visible must not place duplicates.
    book2 = book + [_book_order(oid, st="open") for oid in broker.placed]
    engine.reconcile(book2, {})
    assert len(broker.placed) == 6


def test_missing_sl_replaced_once_then_second_failure_errors(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "sl_visibility_grace_sec", 0.0)
    monkeypatch.setattr(settings, "sl_replace_max_attempts", 1)
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="ACTIVE", sl="SL0", fill_price="100.00",
                    sl_quantity=20, sl_last_seen_ms=int(time.time() * 1000)))

    # First missing reconcile -> re-place once from stored fill price. A fresh
    # successful positions snapshot must confirm the position (H3).
    pos = {"X": {"qty": 20}}
    summary = engine.reconcile([], pos)
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["sl_order_id"] == "SL1"
    assert len(broker.placed) == 1
    assert summary["repaired"] == ["SL0"]

    # Missing again -> attempts exhausted -> ERRORED + naked alert, no 2nd SL.
    leg["sl_last_seen_ms"] = int(time.time() * 1000) - 60_000
    summary = engine.reconcile([], pos)
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["naked_position"] is True
    assert leg["blocked_reason"] == "SL_MISSING"
    assert len(broker.placed) == 1
    assert summary["naked"] == ["SL1"]


def test_replacement_failure_errors_without_second_place(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "sl_visibility_grace_sec", 0.0)
    broker = SlBroker(fail_place=True)
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="ACTIVE", sl="SL0", fill_price="100.00",
                    sl_quantity=20, sl_last_seen_ms=int(time.time() * 1000)))

    engine.reconcile([], {"X": {"qty": 20}})
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["naked_position"] is True
    assert len(broker.placed) == 0


def test_sl_reappearing_heals_leg_and_resets_streak(monkeypatch):
    monkeypatch.setattr(settings, "sl_replace_max_attempts", 0)
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="ERRORED", sl="SL0", fill_price="100.00",
                    sl_quantity=20, naked_position=True,
                    naked_prev_status="ACTIVE", blocked_reason="SL_MISSING",
                    sl_missing_streak=3, error="SL order missing"))

    engine.reconcile([_book_order("SL0", st="open")], {})
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["naked_position"] is False
    assert leg["blocked_reason"] is None
    assert leg["sl_missing_streak"] == 0
    assert "error" not in leg


def test_unfilled_entry_past_guarantee_alerts(monkeypatch):
    monkeypatch.setattr(settings, "sl_guarantee_behavior", "alert")
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1"))
    engine.orders["E1"]["sl_guarantee_due_ms"] = int(time.time() * 1000) - 1

    engine.reconcile([_book_order("E1", st="open", fldQty=0)], {})
    leg = engine.orders["E1"]
    assert leg["status"] == "PENDING_ENTRY"
    assert leg["sl_guarantee_alerted"] is True
    assert leg["blocked_reason"] == "ENTRY_NOT_FILLED_SL_GUARANTEE"
    assert broker.cancels == []


def test_unfilled_entry_past_guarantee_auto_cancel_when_configured(monkeypatch):
    monkeypatch.setattr(settings, "sl_guarantee_behavior", "auto_cancel")
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1"))
    engine.orders["E1"]["sl_guarantee_due_ms"] = int(time.time() * 1000) - 1

    engine.reconcile([_book_order("E1", st="open", fldQty=0)], {})
    assert broker.cancels == ["E1"]
    assert engine.orders["E1"]["entry_cancel_requested_ms"]


# ---------- independent review follow-ups (WP3) ----------
def test_repair_skips_when_positions_snapshot_unavailable(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "sl_visibility_grace_sec", 0.0)
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="ACTIVE", sl="SL0", fill_price="100.00",
                    sl_quantity=20, sl_last_seen_ms=int(time.time() * 1000)))

    engine.reconcile([], None)
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["blocked_reason"] == "POSITIONS_UNAVAILABLE"
    assert broker.placed == []


def test_flat_position_closes_without_reopening_sl(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "sl_visibility_grace_sec", 0.0)
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="ACTIVE", sl="SL0", fill_price="100.00",
                    sl_quantity=20, sl_last_seen_ms=int(time.time() * 1000)))

    engine.reconcile([], {"X": {"qty": 0}})
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["blocked_reason"] == "POSITION_FLAT_UNCONFIRMED"
    engine.reconcile([], {"X": {"qty": 0}})
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_reason"] == "EXTERNAL_FLAT"
    assert leg["naked_position"] is False
    assert broker.placed == []


def test_position_mismatch_errors_without_replacing_sl(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "sl_visibility_grace_sec", 0.0)
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="ACTIVE", sl="SL0", fill_price="100.00",
                    sl_quantity=20, sl_last_seen_ms=int(time.time() * 1000)))

    engine.reconcile([], {"X": {"qty": -20}})
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "POSITION_MISMATCH"
    assert broker.placed == []


def test_trailing_sl_repair_preserves_status_stop_and_hwm(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "sl_visibility_grace_sec", 0.0)
    broker = SlBroker()
    broker.stops = []

    def place(*_a, **kw):
        broker.stops.append(kw.get("trigger_price") or (_a[3] if len(_a) > 3 else None))
        oid = f"SL{len(broker.placed) + 1}"
        broker.placed.append(oid)
        return {"stCode": 200, "nOrdNo": oid}

    broker.place_stop_loss = place
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="TRAILING", sl="SL0", fill_price="100.00",
                    sl_quantity=20, stop_loss_price="110.00",
                    high_water_mark=115.0, low_water_mark=90.0,
                    sl_last_seen_ms=int(time.time() * 1000)))

    engine.reconcile([], {"X": {"qty": 20}})
    leg = engine.orders["E1"]
    assert leg["status"] == "TRAILING"
    assert leg["stop_loss_price"] == "110.00"
    assert leg["high_water_mark"] == 115.0
    assert broker.stops and broker.stops[0] == "110.00"


def test_partial_fill_places_sl_and_cancels_only_remainder(monkeypatch):
    monkeypatch.setattr(settings, "sl_guarantee_behavior", "auto_cancel")
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1"))
    engine.orders["E1"]["sl_guarantee_due_ms"] = int(time.time() * 1000) - 1

    engine.reconcile([_book_order("E1", st="open", qty=20, fldQty=10)], {})
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["sl_quantity"] == 10
    assert broker.placed == ["SL1"]
    assert broker.cancels == ["E1"]
    assert leg["entry_cancel_sent_ms"]


def test_failed_auto_cancel_retries_until_sent(monkeypatch):
    monkeypatch.setattr(settings, "sl_guarantee_behavior", "auto_cancel")
    monkeypatch.setattr(settings, "exit_confirm_poll_sec", 0.001)

    class FailOnceBroker(SlBroker):
        def __init__(self):
            super().__init__()
            self.fail = True

        def cancel(self, oid):
            self.cancels.append(oid)
            if self.fail:
                self.fail = False
                raise RuntimeError("HTTP/2 stream reset")
            return {"stCode": 200}

    broker = FailOnceBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1"))
    engine.orders["E1"]["sl_guarantee_due_ms"] = int(time.time() * 1000) - 1
    book = [_book_order("E1", st="open", fldQty=0)]

    engine.reconcile(book, {})
    leg = engine.orders["E1"]
    assert leg["entry_cancel_pending"] is True
    assert leg.get("entry_cancel_sent_ms") is None

    leg["entry_cancel_next_ms"] = 0
    engine.reconcile(book, {})
    leg = engine.orders["E1"]
    assert len(broker.cancels) == 2
    assert leg["entry_cancel_sent_ms"]
    assert leg["entry_cancel_pending"] is False


def test_config_rejects_zero_confirmations_and_timers():
    from pydantic import ValidationError

    from app.config import Settings

    with pytest.raises(ValidationError):
        Settings(sl_missing_confirmations=0)
    with pytest.raises(ValidationError):
        Settings(exit_confirm_timeout_sec=0)
    with pytest.raises(ValidationError):
        Settings(sl_guarantee_behavior="cancel-everything")


def test_fno_position_lots_are_normalized_to_shares(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "flat_position_confirmations", 2)
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="ACTIVE", sl="SL0", fill_price="100.00",
                    quantity="20", sl_quantity=20))
    book = [_book_order("SL0", st="open")]

    # Broker reports F&O qty in LOTS (1 lot x 20) while the engine tracks shares.
    summary = engine.reconcile(
        book, {"X": {"qty": 1, "lot_sz": 20, "qty_shares": 20}},
        positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["blocked_reason"] is None
    assert summary["errored"] == []


def test_flat_streak_resets_on_failed_positions_and_cancels_sl_once(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "flat_position_confirmations", 2)
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="ACTIVE", sl="SL0", fill_price="100.00",
                    sl_quantity=20))
    book = [_book_order("SL0", st="open")]
    flat = {"X": {"qty": 0}}

    summary = engine.reconcile(book, flat, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["blocked_reason"] == "POSITION_FLAT_UNCONFIRMED"
    assert leg["position_flat_streak"] == 1
    assert summary["driven"] == []
    assert broker.cancels == []

    # A failed/absent positions fetch must not advance or preserve the streak.
    engine.reconcile(book, None)
    assert engine.orders["E1"]["position_flat_streak"] == 0

    engine.reconcile(book, flat, positions_ok=True)
    assert engine.orders["E1"]["position_flat_streak"] == 1
    assert broker.cancels == []
    engine.reconcile(book, flat, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_reason"] == "EXTERNAL_FLAT"
    assert broker.cancels == ["SL0"]


def test_entry_cancel_retries_after_expired_guarantee_deadline(monkeypatch):
    monkeypatch.setattr(settings, "sl_guarantee_sec", 12.0)
    monkeypatch.setattr(settings, "entry_cancel_retry_sec", 60.0)
    monkeypatch.setattr(settings, "entry_cancel_max_attempts", 3)
    monkeypatch.setattr(settings, "exit_confirm_poll_sec", 1.0)
    monkeypatch.setattr(settings, "sl_guarantee_behavior", "auto_cancel")

    class FailCancel(SlBroker):
        def cancel(self, oid):
            self.cancels.append(oid)
            return {"stCode": 500, "msg": "temporary"}

    broker = FailCancel()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="PENDING_ENTRY"))
    engine.orders["E1"]["sl_guarantee_due_ms"] = int(time.time() * 1000) - 30000

    # Production reconcile can be 30s after a 12s guarantee: it must still
    # attempt the cancel instead of treating the deadline as already failed.
    engine.reconcile([], {}, positions_ok=True)
    leg = engine.orders["E1"]
    assert len(broker.cancels) == 1
    assert leg["entry_cancel_attempts"] == 1

    for _ in range(3):
        leg["entry_cancel_next_ms"] = 0
        engine.reconcile([], {}, positions_ok=True)
    leg = engine.orders["E1"]
    assert len(broker.cancels) == 3
    assert leg["entry_cancel_attempts"] == 3
    assert leg["blocked_reason"] == "ENTRY_CANCEL_FAILED"
    assert leg["status"] == "PENDING_ENTRY"


def test_sl_tag_is_deterministic_and_adopted_after_timeout(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    tags = []
    book = []

    class TimeoutSl(SlBroker):
        def place_stop_loss(self, *_a, **kw):
            tags.append(kw.get("tag"))
            if len(tags) == 1:
                oid = "SL1"
                self.placed.append(oid)
                row = _book_order(oid, st="open")
                row["guiOrdId"] = kw.get("tag")
                book.append(row)
                raise TimeoutError("response lost after accept")
            return super().place_stop_loss(*_a, **kw)

    broker = TimeoutSl()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="PENDING_ENTRY"))

    engine.reconcile([_book_order("E1", st="complete")], {}, positions_ok=True)
    leg = engine.orders["E1"]
    assert tags == ["SE1-1"]
    assert leg["blocked_reason"] == "SL_PLACEMENT_AMBIGUOUS"
    assert leg["status"] == "ERRORED"
    assert leg.get("sl_order_id") is None

    engine.reconcile([_book_order("E1", st="complete")] + book, {},
                     positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["sl_order_id"] == "SL1"
    assert len(tags) == 1                       # adopted, never duplicated


def test_rejected_sl_retries_with_new_tag_after_confirmations(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 2)
    tags = []

    class RejectOnce(SlBroker):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def place_stop_loss(self, *_a, **kw):
            tags.append(kw.get("tag"))
            self.calls += 1
            if self.calls == 1:
                self.fail_place = True
                return {"stCode": 500, "msg": "rejected"}
            self.fail_place = False
            return super().place_stop_loss(*_a, **kw)

    broker = RejectOnce()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="PENDING_ENTRY"))
    entry_book = [_book_order("E1", st="complete")]

    positions = {"X": {"qty": 1, "lot_sz": 20, "qty_shares": 20}}
    engine.reconcile(entry_book, positions, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["blocked_reason"] == "SL_PLACEMENT_REJECTED"
    assert tags == ["SE1-1"]

    engine.reconcile(entry_book, positions, positions_ok=True)
    assert len(tags) == 1                       # first book miss: wait
    engine.reconcile(entry_book, positions, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert tags == ["SE1-1", "SE1-2"]
    assert leg["sl_client_tag"] == "SE1-2"


def test_active_partial_entry_cancel_clears_pending_after_success(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    broker = SlBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="ACTIVE", sl="SL0", fill_price="100.00",
                    sl_quantity=20, entry_cancel_pending=True,
                    entry_cancel_attempts=0))
    book = [_book_order("SL0", st="open")]
    positions = {"X": {"qty": 1, "lot_sz": 20, "qty_shares": 20}}

    engine.reconcile(book, positions, positions_ok=True)
    leg = engine.orders["E1"]
    assert broker.cancels == ["E1"]
    assert leg["entry_cancel_sent_ms"]
    assert leg["entry_cancel_pending"] is False


def test_replacement_sl_timeout_waits_for_tag_adoption(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "sl_visibility_grace_sec", 0.0)
    monkeypatch.setattr(settings, "sl_replace_max_attempts", 1)
    tags = []
    book = []

    class ReplacementTimeout(SlBroker):
        def place_stop_loss(self, *_a, **kw):
            tags.append(kw.get("tag"))
            oid = "SL2"
            self.placed.append(oid)
            row = _book_order(oid, st="open")
            row["guiOrdId"] = kw.get("tag")
            book.append(row)
            raise TimeoutError("response lost after replacement accept")

    broker = ReplacementTimeout()
    engine = SyntheticEngine(broker)
    engine.add(_leg("E1", status="ACTIVE", sl="SL1", fill_price="100.00",
                    sl_quantity=20, sl_last_seen_ms=int(time.time() * 1000)))
    positions = {"X": {"qty": 1, "lot_sz": 20, "qty_shares": 20}}

    engine.reconcile([], positions, positions_ok=True)
    leg = engine.orders["E1"]
    assert tags == ["SE1-1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "SL_PLACEMENT_AMBIGUOUS"
    assert leg["naked_position"] is False
    assert leg.get("sl_order_id") is None

    # The replacement landed after the timeout: adopt it by tag, never naked.
    engine.reconcile(book, positions, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["sl_order_id"] == "SL2"
    assert leg["naked_position"] is False
    assert tags == ["SE1-1"]                    # no duplicate replacement
