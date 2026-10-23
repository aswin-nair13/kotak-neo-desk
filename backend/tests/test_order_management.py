"""Plan 06 WP6: in-app order management (orderbook, cancel, modify SL).

The order book must be structured and must never report a failed read as an
empty success. Cancel/cancel-all/modify-SL run through the command worker,
update the tracker, are double-click safe, and schedule a reconcile.
"""
import json
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings
from app.routes import orders as routes
from app.synthetic_engine import SyntheticEngine


def _leg(eid="E1", status="ACTIVE", sl="SL1", **over):
    d = {"entry_order_id": eid, "sl_order_id": sl,
         "trading_symbol": "X", "status": status, "strategy": "market_exit",
         "target_price": "120.00", "stop_loss_price": "90.00",
         "trail_distance": 5, "high_water_mark": 0.0, "low_water_mark": 0.0,
         "quantity": "20", "exchange_segment": "nse_fo",
         "instrument_token": "tok", "side": "B", "product": "MIS",
         "target_points": "20", "stop_points": "10", "fill_price": "100.00",
         "sl_placed": True, "sl_quantity": 20,
         "sl_last_seen_ms": int(time.time() * 1000)}
    d.update(over)
    return d


class MgmtBroker:
    def __init__(self):
        self.book = {"data": []}
        self.cancels = []
        self.cancel_error_ids = set()
        self.modifies = []
        self.book_error = None
        self.modify_error = None

    def order_book(self, *_a, **_kw):
        if self.book_error:
            raise self.book_error
        return self.book

    def cancel(self, oid):
        self.cancels.append(oid)
        if oid in self.cancel_error_ids:
            raise RuntimeError(f"cancel rejected for {oid}")
        return {"stCode": 200, "nOrdNo": oid}

    def modify_sl(self, oid, qty, trigger, transaction_type=None):
        self.modifies.append((oid, qty, trigger, transaction_type))
        if self.modify_error:
            raise self.modify_error
        return {"stCode": 200, "nOrdNo": oid}


class Bridge:
    def __init__(self):
        self.reconciles = []

    def request_reconcile(self, source="orders-api"):
        self.reconciles.append(source)


@pytest.fixture
def wired():
    broker = MgmtBroker()
    engine = SyntheticEngine(broker)
    bridge = Bridge()
    routes.kotak = broker
    routes.engine = engine
    routes.bridge = bridge
    routes.worker = None
    routes.read_worker = None
    with routes._manual_exit_lock:
        routes._manual_exit_records.clear()
    yield broker, engine, bridge
    routes.kotak = None
    routes.engine = None
    routes.bridge = None
    routes.worker = None
    routes.read_worker = None
    with routes._manual_exit_lock:
        routes._manual_exit_records.clear()


def test_orderbook_is_structured_with_trigger_and_age(wired):
    broker, _engine, _bridge = wired
    now = int(time.time() * 1000)
    broker.book = {"data": [{
        "nOrdNo": "O1", "ordSt": "open", "trdSym": "X", "trnsTp": "S",
        "qty": "20", "avgPrc": "101.25", "trgPrc": "95.50",
        "fldQty": "5", "ordDtTm": str(now - 5000)}]}

    out = routes.orderbook()
    assert out["ok"] is True
    assert out["count"] == 1
    row = out["orders"][0]
    assert row["nOrdNo"] == "O1"
    assert row["trigger_price"] == "95.50"
    assert row["filled_qty"] == "5"
    assert row["status"] == "open"
    assert row["age_ms"] >= 4000
    assert out["stale"] is False
    assert out["fetched_at_ms"]


def test_orderbook_empty_success_is_not_an_error(wired):
    broker, _engine, _bridge = wired
    broker.book = {"data": []}
    out = routes.orderbook()
    assert out["ok"] is True
    assert out["count"] == 0
    assert out["orders"] == []


def test_orderbook_failure_is_structured_and_redacted(wired):
    broker, _engine, _bridge = wired
    broker.book_error = RuntimeError("login failed token=supersecret sid=abc")
    res = routes.orderbook()
    assert res.status_code == 502
    body = json.loads(res.body)
    assert body["ok"] is False
    assert body["orders"] == []
    assert "supersecret" not in body["error"]
    assert "abc" not in body["error"]


def test_cancel_is_idempotent_and_closes_entry_from_reconcile(wired):
    broker, engine, bridge = wired
    engine.add(_leg(status="PENDING_ENTRY", sl=None))

    out = routes.cancel("E1")
    assert out["ok"] is True
    assert out["what"] == "entry"
    assert broker.cancels == ["E1"]
    assert engine.orders["E1"]["cancel_requested_ids"]["E1"]
    assert bridge.reconciles == ["cancel"]

    again = routes.cancel("E1")
    assert again["ok"] is True
    assert again["already"] is True
    assert broker.cancels == ["E1"]        # double-click sent no second cancel

    engine.reconcile([{"nOrdNo": "E1", "ordSt": "cancelled",
                       "trdSym": "X", "trnsTp": "B", "qty": 20}], {},
                     positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_reason"] == "ENTRY_CANCELLED"


def test_cancel_sl_marks_naked_manual_cancel_and_does_not_replace(wired):
    broker, engine, _bridge = wired
    engine.add(_leg())

    out = routes.cancel("SL1")
    assert out["ok"] is True
    assert out["what"] == "sl"
    assert engine.orders["E1"]["manual_sl_cancel"] is True

    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "cancelled"})
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["naked_position"] is True
    assert leg["blocked_reason"] == "SL_CANCELLED_MANUAL"
    # No silent re-place of the cancelled stop.
    engine.reconcile([], {"X": {"qty": 20}}, positions_ok=True)
    assert broker.cancels == ["SL1"]
    assert engine.orders["E1"]["status"] == "ERRORED"


def test_cancel_all_tracks_exact_ids_and_is_idempotent(wired):
    broker, engine, _bridge = wired
    engine.add(_leg(eid="E1", status="PENDING_ENTRY", sl=None))
    engine.add(_leg(eid="E2", sl="SL2"))

    out = routes.cancel_all("X")
    assert out["ok"] is True
    assert out["requested_count"] == 2
    assert out["cancelled_count"] == 2
    assert sorted(broker.cancels) == ["E1", "SL2"]

    again = routes.cancel_all("X")
    assert again["ok"] is True
    assert again["already_count"] == 2
    assert sorted(broker.cancels) == ["E1", "SL2"]


def test_modify_sl_uses_tracker_qty_sets_pending_and_feed_settles(wired):
    broker, engine, bridge = wired
    engine.add(_leg())

    out = routes.modify_sl("SL1", 95.0)
    assert out["ok"] is True
    assert out["quantity"] == "20"
    assert out["sl_modify_pending"] is True
    assert broker.modifies == [("SL1", "20", "95.00", "S")]
    assert bridge.reconciles == ["modify-sl"]

    leg = engine.orders["E1"]
    assert leg["sl_modify_pending"] is True
    assert leg["sl_modify_target"] == "95.00"

    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "open",
                            "trigger_price": "95.00", "qty": 20})
    leg = engine.orders["E1"]
    assert leg["sl_modify_pending"] is False
    assert leg["stop_loss_price"] == "95.00"


def test_modify_sl_failure_clears_pending_and_returns_structured_error(wired):
    broker, engine, _bridge = wired
    engine.add(_leg())
    broker.modify_error = RuntimeError("modify rejected token=secret123")

    res = routes.modify_sl("SL1", 95.0)
    assert res.status_code == 502
    body = json.loads(res.body)
    assert body["ok"] is False
    assert "secret123" not in body["error"]
    leg = engine.orders["E1"]
    assert leg["sl_modify_pending"] is False
    assert leg["stop_loss_price"] == "90.00"   # tracker not silently reverted


def test_modify_sl_rejects_a_tracked_entry_id(wired):
    broker, engine, _bridge = wired
    engine.add(_leg())
    res = routes.modify_sl("E1", 95.0)
    assert res.status_code == 400
    assert broker.modifies == []
    assert engine.orders["E1"]["sl_modify_pending"] is False


def test_modify_sl_requires_positive_trigger(wired):
    _broker, engine, _bridge = wired
    engine.add(_leg())
    res = routes.modify_sl("SL1", 0)
    assert res.status_code == 400
    assert json.loads(res.body)["ok"] is False


def test_orderbook_not_ok_stat_is_a_failure_not_empty_success(wired):
    broker, _engine, _bridge = wired
    broker.book = {"stat": "Not_Ok", "data": None}
    res = routes.orderbook()
    assert res.status_code == 502
    body = json.loads(res.body)
    assert body["ok"] is False
    assert body["orders"] == []
    assert body["count"] == 0


def test_modify_sl_normalizes_quantity_to_positive_int_string(wired):
    broker, _engine, _bridge = wired
    out = routes.modify_sl("UNTRACKED", 95.0, "20.0")
    assert out["ok"] is True
    assert out["quantity"] == "20"
    assert broker.modifies == [("UNTRACKED", "20", "95.00", "S")]

    broker.modifies.clear()
    res = routes.modify_sl("UNTRACKED", 95.0, "20.5")
    assert res.status_code == 400
    assert broker.modifies == []


def test_modify_sl_rejects_loosening_a_trailing_stop(wired):
    broker, engine, _bridge = wired
    engine.add(_leg(status="TRAILING", side="B", stop_loss_price="90.00"))

    res = routes.modify_sl("SL1", 88.0)
    assert res.status_code == 400
    assert broker.modifies == []
    assert "TRAILING" in json.loads(res.body)["error"]

    out = routes.modify_sl("SL1", 95.0)
    assert out["ok"] is True
    assert broker.modifies == [("SL1", "20", "95.00", "S")]


def test_modify_sl_rejects_loosening_a_trailing_stop_on_short_side(wired):
    broker, engine, _bridge = wired
    engine.add(_leg(status="TRAILING", side="S", stop_loss_price="110.00"))

    res = routes.modify_sl("SL1", 112.0)        # further above = looser short stop
    assert res.status_code == 400
    assert broker.modifies == []
    assert "TRAILING" in json.loads(res.body)["error"]

    out = routes.modify_sl("SL1", 108.0)        # tighter
    assert out["ok"] is True
    assert broker.modifies == [("SL1", "20", "108.00", "B")]


def test_broker_ok_requires_stat_and_stcode_to_agree():
    assert routes._broker_ok({"stCode": 200, "stat": "Ok"}) is True
    assert routes._broker_ok({"stCode": "200"}) is True
    assert routes._broker_ok({"stCode": 200, "stat": "Not_Ok"}) is False
    assert routes._broker_ok({"stCode": 500, "stat": "Ok"}) is False


def test_place_tracked_rejects_stat_not_ok_even_with_stcode_200(wired):
    broker, _engine, _bridge = wired
    from fastapi import HTTPException

    class BadPlace:
        def place_order(self, **_kw):
            return {"stCode": 200, "stat": "Not_Ok", "nOrdNo": "E1"}

    routes.kotak = BadPlace()
    try:
        with pytest.raises(HTTPException) as err:
            routes._place_tracked("nse_fo", "X", "20", 100, 10, 5,
                                  "market_exit", None, "MIS")
        assert err.value.status_code == 502
    finally:
        routes.kotak = broker


def test_exit_position_rejects_stat_not_ok_even_with_stcode_200(wired):
    broker, engine, _bridge = wired
    from fastapi import HTTPException

    from app.schemas import ExitBody

    class BadExit:
        def positions(self, strict=False):
            return {"ABC": {"qty": 1, "qty_shares": 20,
                            "exchange": "nse_fo"}}

        def cancel(self, _oid):
            return {"stCode": 200}

        def place_market_exit(self, *_a, **_kw):
            return {"stCode": 200, "stat": "Not_Ok", "nOrdNo": "X1"}

    routes.kotak = BadExit()
    try:
        with pytest.raises(HTTPException) as err:
            routes._exit_position_impl(ExitBody(symbol="ABC"))
        assert err.value.status_code == 502
    finally:
        routes.kotak = broker
        routes.engine = engine


def test_exit_position_positions_timeout_is_504_not_404(wired):
    _broker, _engine, _bridge = wired
    from fastapi import HTTPException

    from app.schemas import ExitBody

    class TimeoutWorker:
        def submit_wait(self, fn, *args, timeout=None, **kwargs):
            raise TimeoutError("positions stalled")

    routes.read_worker = TimeoutWorker()
    try:
        with pytest.raises(HTTPException) as err:
            routes.exit_position(ExitBody(symbol="ABC"))
        assert err.value.status_code == 504
        assert "No open position" not in str(err.value.detail)
    finally:
        routes.read_worker = None


def test_exit_position_positions_failure_is_502_not_404(wired):
    _broker, _engine, _bridge = wired
    from fastapi import HTTPException

    from app.schemas import ExitBody

    class BrokenPositions:
        def positions(self, strict=False):
            raise RuntimeError("positions failed token=supersecret")

    routes.read_worker = None
    routes.kotak = BrokenPositions()
    try:
        with pytest.raises(HTTPException) as err:
            routes.exit_position(ExitBody(symbol="ABC"))
        assert err.value.status_code == 502
        assert "No open position" not in str(err.value.detail)
        assert "supersecret" not in str(err.value.detail)
    finally:
        routes.read_worker = None


class ManualExitBroker:
    def __init__(self, timeout_first=False):
        self.book = []
        self.places = []
        self.timeout_first = timeout_first
        self.report_error = None

    def positions(self, strict=False):
        return {"ABC": {"qty": 1, "qty_shares": 20, "exchange": "nse_fo",
                        "product": "MIS"}}

    def cancel(self, oid):
        return {"stCode": 200}

    def place_market_exit(self, exchange, qty, symbol, **kwargs):
        self.places.append({"exchange": exchange, "qty": qty,
                            "symbol": symbol, **kwargs})
        oid = f"X{len(self.places)}"
        self.book.append({"nOrdNo": oid, "ordSt": "open", "trdSym": symbol,
                          "trnsTp": kwargs.get("transaction_type", "S"),
                          "qty": qty, "prc": "0", "fldQty": "0",
                          "GuiOrdId": kwargs.get("tag")})
        if self.timeout_first and len(self.places) == 1:
            raise TimeoutError("manual exit response lost after accept")
        return {"stCode": 200, "nOrdNo": oid}

    def order_report(self):
        if self.report_error is not None:
            raise self.report_error
        return {"stat": "Ok", "stCode": 200, "data": list(self.book)}


def test_manual_exit_timeout_landed_is_adopted_and_double_click_is_idempotent(
        wired):
    _broker, engine, _bridge = wired
    from app.schemas import ExitBody

    broker = ManualExitBroker(timeout_first=True)
    routes.kotak = broker
    try:
        out = routes.exit_position(
            ExitBody(symbol="ABC", client_order_tag="click-1"))
        assert out["adopted"] is True
        assert out["exit_order"]["nOrdNo"] == "X1"
        assert len(broker.places) == 1
        tag = out["manual_exit_tag"]
        assert broker.places[0]["tag"] == tag
        assert engine.manual_exit_tag_record(tag)["state"] == "complete"

        again = routes.exit_position(
            ExitBody(symbol="ABC", client_order_tag="click-2"))
        assert again["exit_order"]["nOrdNo"] == "X1"
        assert len(broker.places) == 1
    finally:
        routes.kotak = None


def test_manual_exit_double_click_cannot_double_exit(wired):
    _broker, _engine, _bridge = wired
    from app.schemas import ExitBody

    broker = ManualExitBroker()
    routes.kotak = broker
    try:
        first = routes.exit_position(ExitBody(symbol="ABC"))
        second = routes.exit_position(ExitBody(symbol="ABC"))
        assert first["exit_order"]["nOrdNo"] == "X1"
        assert second["exit_order"]["nOrdNo"] == "X1"
        assert len(broker.places) == 1
    finally:
        routes.kotak = None


def test_manual_exit_ambiguous_is_not_replaced_on_retry(wired, monkeypatch):
    _broker, _engine, _bridge = wired
    from fastapi import HTTPException

    from app.schemas import ExitBody

    monkeypatch.setattr(settings, "order_place_confirm_sec", 0)
    broker = ManualExitBroker(timeout_first=True)
    broker.order_report = lambda: {"stat": "Ok", "stCode": 200, "data": []}
    routes.kotak = broker
    try:
        with pytest.raises(HTTPException) as first:
            routes.exit_position(ExitBody(symbol="ABC",
                                          client_order_tag="click-1"))
        assert first.value.status_code == 502
        assert len(broker.places) == 1

        with pytest.raises(HTTPException) as again:
            routes.exit_position(ExitBody(symbol="ABC",
                                          client_order_tag="click-2"))
        assert again.value.status_code == 502
        assert "no duplicate" in str(again.value.detail)
        assert len(broker.places) == 1
    finally:
        routes.kotak = None


def test_cancel_all_partial_failure_returns_top_level_error(wired):
    broker, engine, _bridge = wired
    engine.add(_leg(eid="E1", status="PENDING_ENTRY", sl=None))
    engine.add(_leg(eid="E2", sl="SL2"))
    broker.cancel_error_ids = {"SL2"}

    res = routes.cancel_all("X")
    assert res.status_code == 502
    body = json.loads(res.body)
    assert body["ok"] is False
    assert body["failed_count"] == 1
    assert "SL2" in body["error"]
    failed = [r for r in body["results"] if not (r.get("ok") or r.get("already"))]
    assert failed and failed[0]["order_id"] == "SL2"


def test_cancel_refreshes_entry_cancel_sent_ms_for_active_entry(wired):
    _broker, engine, _bridge = wired
    engine.add(_leg(status="ACTIVE"))
    out = routes.cancel("E1")
    assert out["ok"] is True
    assert out["what"] == "entry"
    leg = engine.orders["E1"]
    assert leg["entry_cancel_sent_ms"]
    assert leg["status"] == "ACTIVE"


def test_orderbook_read_timeout_is_mapped_to_504(wired):
    _broker, _engine, _bridge = wired
    captured = {}

    class TimeoutWorker:
        def submit_wait(self, fn, *args, timeout=None, **kwargs):
            captured["timeout"] = timeout
            raise TimeoutError("broker read stalled")

    routes.read_worker = TimeoutWorker()
    try:
        res = routes.orderbook()
        assert res.status_code == 504
        body = json.loads(res.body)
        assert body["ok"] is False
        assert captured["timeout"] == settings.orderbook_timeout_sec
    finally:
        routes.read_worker = None


def test_positions_read_uses_dedicated_read_worker_and_times_out(wired):
    from app.broker_worker import CommandWorker
    from app.routes import market as market_routes

    seen = {}

    class PositionsBroker:
        def positions(self, strict=False):
            seen["thread"] = threading.current_thread().name
            return {"X": {"qty": 20}}

    market_routes.kotak = PositionsBroker()
    read_worker = CommandWorker("test-positions-reader", call_timeout=2.0)
    market_routes.read_worker = read_worker
    read_worker.start()
    try:
        out = market_routes.positions()
        assert out == {"positions": {"X": {"qty": 20}}}
        assert seen["thread"] == "test-positions-reader"
        assert read_worker.status()["completed"] == 1
    finally:
        read_worker.stop()

    class TimeoutWorker:
        def submit_wait(self, fn, *args, timeout=None, **kwargs):
            raise TimeoutError("positions stalled")

    market_routes.read_worker = TimeoutWorker()
    try:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as err:
            market_routes.positions()
        assert err.value.status_code == 504
    finally:
        market_routes.read_worker = None
        market_routes.kotak = None


def test_slow_orderbook_read_cannot_starve_mutation_worker(wired):
    """A blocked UI read must not delay a queued cancel/exit on the writer."""
    from app.broker_worker import CommandWorker

    broker, _engine, _bridge = wired
    read_started = threading.Event()
    release_read = threading.Event()

    def slow_orderbook(*_a, **_kw):
        read_started.set()
        release_read.wait(10.0)
        return {"data": []}

    broker.order_book = slow_orderbook
    read_worker = CommandWorker("test-reader", call_timeout=0.2)
    mutation_worker = CommandWorker("test-writer", call_timeout=0.2)
    read_worker.start()
    mutation_worker.start()
    routes.read_worker = read_worker
    routes.worker = mutation_worker
    try:
        res = routes.orderbook()
        assert res.status_code == 504           # caller timed out
        assert read_started.wait(2.0)           # the read is still in flight

        # The mutation worker is separate, so a cancel completes immediately
        # even while the read thread is wedged.
        out = routes._run_on_worker(broker.cancel, "SL1", label="cancel")
        assert out == {"stCode": 200, "nOrdNo": "SL1"}
        assert broker.cancels == ["SL1"]
    finally:
        release_read.set()
        read_worker.stop()
        mutation_worker.stop()
        routes.read_worker = None
        routes.worker = None
