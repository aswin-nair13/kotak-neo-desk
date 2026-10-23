"""Plan 06 WP8: deterministic entry tags, adoption, and bounded retry.

A timed-out placement is never blindly re-sent: the pre-registered client tag is
looked up in the order report (over a bounded window) and adopted; only an
authoritative miss may retry, and only within `ORDER_PLACE_MAX_ATTEMPTS`. Every
attempt tag is retained: a superseded attempt that lands late is adopted with a
DUPLICATE_ENTRY alert, never ignored.
"""
import logging
import sys
import time
from pathlib import Path

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.broker_worker import CommandWorker
from app.config import Settings, settings
from app.routes import orders as orders_routes
from app.schemas import SingleOrderBody
from app.synthetic_engine import SyntheticEngine


def _row(oid, tag, symbol="NIFTY25N1825000CE", status="open", qty="75",
         side="B", prc="100.00", filled="0"):
    return {"nOrdNo": oid, "ordSt": status, "trdSym": symbol, "trnsTp": side,
            "qty": qty, "prc": prc, "fldQty": filled, "GuiOrdId": tag}


class TagBroker:
    """Fake broker: configurable accept/timeout behaviour + order report."""

    master_ok = True

    def __init__(self, accept_then_timeout=False, timeout_without_order=False,
                 fill_on_place=False):
        self.book = []
        self.place_calls = []
        self.accept_then_timeout = accept_then_timeout
        self.timeout_without_order = timeout_without_order
        self.fill_on_place = fill_on_place
        self.report_error = None
        self.engine = None
        self.sl_calls = 0

    def token_for(self, *_a, **_kw):
        return "tok"

    def place_order(self, **kw):
        self.place_calls.append(kw)
        tag = kw["tag"]
        symbol = kw["trading_symbol"]
        qty = kw["quantity"]
        side = kw["transaction_type"]
        n = len(self.place_calls)
        if self.timeout_without_order and n == 1:
            raise TimeoutError("response lost before accept")
        oid = f"E{n}"
        self.book.append(_row(oid, tag, symbol=symbol, qty=qty, side=side))
        if self.fill_on_place:
            fill = {"nOrdNo": oid, "ordSt": "complete", "trdSym": symbol,
                    "trnsTp": side, "qty": qty, "prc": "101.00",
                    "fldQty": qty, "GuiOrdId": tag}
            self.engine.on_order_update(fill)
        if self.accept_then_timeout and n == 1:
            raise TimeoutError("timeout after the broker accepted")
        return {"stCode": 200, "nOrdNo": oid}

    def order_report(self):
        if self.report_error is not None:
            raise self.report_error
        return {"stat": "Ok", "stCode": 200, "data": list(self.book)}

    def place_stop_loss(self, *_a, **_kw):
        self.sl_calls += 1
        return {"stCode": 200, "nOrdNo": "SL1"}

    def average_fill_price(self, _oid):
        return 101.0


def _wire(monkeypatch, broker):
    engine = SyntheticEngine(broker)
    broker.engine = engine
    orders_routes._idem_cache.clear()
    monkeypatch.setattr(orders_routes, "kotak", broker)
    monkeypatch.setattr(orders_routes, "engine", engine)
    monkeypatch.setattr(orders_routes, "hub", None)
    monkeypatch.setattr(orders_routes, "bridge", None)
    monkeypatch.setattr(orders_routes, "worker", None)
    return engine


def _body(tag="click-1"):
    return SingleOrderBody(instrument="NIFTY", expiry="25N18", option_type="CE",
                           strike="25000", lots=1, entry_mode="L",
                           limit_price=100, target_points=20, stop_points=10,
                           client_order_tag=tag)


def _payload(eid=None):
    return {
        "entry_order_id": eid, "sl_order_id": None,
        "trading_symbol": "NIFTY25N1825000CE", "status": "PENDING_ENTRY",
        "strategy": "market_exit", "target_price": "120.00",
        "stop_loss_price": "90.00", "trail_distance": 5,
        "high_water_mark": 0.0, "low_water_mark": 0.0, "quantity": "75",
        "exchange_segment": "nse_fo", "instrument_token": "tok", "side": "B",
        "product": "MIS", "target_points": "20", "stop_points": "10",
    }


@pytest.fixture(autouse=True)
def _no_confirm_delay(monkeypatch):
    monkeypatch.setattr(settings, "order_place_confirm_sec", 0)


def test_accept_then_timeout_adopts_tag_exactly_once(monkeypatch):
    broker = TagBroker(accept_then_timeout=True)
    engine = _wire(monkeypatch, broker)

    out = orders_routes.place_single(_body(), ref_ltp=100.0)
    assert len(broker.place_calls) == 1
    assert len(broker.book) == 1
    assert out["entry_order_id"] == "E1"
    assert broker.place_calls[0]["tag"] == out["client_tag"]
    leg = engine.orders["E1"]
    assert leg["entry_client_tag"] == out["client_tag"]
    assert leg["status"] == "PENDING_ENTRY"
    assert engine.entry_intent_health()["counts"]["adopted"] == 1


def test_timeout_with_authoritative_miss_retries_exactly_once(monkeypatch):
    broker = TagBroker(timeout_without_order=True)
    engine = _wire(monkeypatch, broker)

    out = orders_routes.place_single(_body(), ref_ltp=100.0)
    assert len(broker.place_calls) == 2
    tags = [c["tag"] for c in broker.place_calls]
    assert tags[0] != tags[1]
    assert out["entry_order_id"] == "E2"
    assert out["client_tag"] == tags[1]
    assert list(engine.orders) == ["E2"]
    assert engine.pending_entry_tags() == []
    # The first attempt is retained as superseded, not forgotten.
    health = engine.entry_intent_health()
    assert [r["tag"] for r in health["superseded"]] == [tags[0]]


def test_retry_budget_is_bounded(monkeypatch):
    monkeypatch.setattr(settings, "order_place_max_attempts", 2)

    class AlwaysTimeout(TagBroker):
        def place_order(self, **kw):
            self.place_calls.append(kw)
            raise TimeoutError("never accepted")

    broker = AlwaysTimeout()
    engine = _wire(monkeypatch, broker)

    with pytest.raises(HTTPException) as err:
        orders_routes.place_single(_body(), ref_ltp=100.0)
    assert err.value.status_code == 502
    assert len(broker.place_calls) == 2
    assert engine.orders == {}
    assert engine.pending_entry_tags() == []
    assert engine.entry_intent_health()["counts"]["superseded"] == 2


def test_route_wait_covers_slow_report_and_retry(monkeypatch):
    """A slow order_report must not make the route return 504 mid-flow.

    Each ambiguous attempt pays place + order-report, and attempt 2 does the
    same. The old wait budget omitted the report read and would 504 here while
    the worker still owned attempt 2; a caller retry could then duplicate it.
    """
    monkeypatch.setattr(settings, "broker_call_timeout_sec", 1.0)
    monkeypatch.setattr(settings, "order_place_confirm_sec", 0.0)
    monkeypatch.setattr(settings, "order_place_max_attempts", 2)

    class SlowReportBroker(TagBroker):
        def __init__(self):
            super().__init__()
            self.last_place_at = 0.0

        def place_order(self, **kw):
            self.place_calls.append(kw)
            time.sleep(0.25)
            self.last_place_at = time.monotonic()
            raise TimeoutError("slow response lost before accept")

        def order_report(self):
            time.sleep(3.5)
            return {"stat": "Ok", "stCode": 200, "data": []}

    broker = SlowReportBroker()
    engine = _wire(monkeypatch, broker)
    worker = CommandWorker("place-wait-test", max_queue=20, stall_ms=1000,
                           stop_timeout=1.0, call_timeout=0.1)
    orders_routes.worker = worker
    worker.start()
    try:
        with pytest.raises(HTTPException) as err:
            orders_routes.place_single(_body(), ref_ltp=100.0)
        assert err.value.status_code == 502
        assert len(broker.place_calls) == 2
        assert time.monotonic() >= broker.last_place_at
        assert engine.orders == {}
        time.sleep(0.2)
        assert len(broker.place_calls) == 2
    finally:
        worker.stop(1.0)
        orders_routes.worker = None


def test_max_attempts_one_is_honored(monkeypatch):
    monkeypatch.setattr(settings, "order_place_max_attempts", 1)

    class AlwaysTimeout(TagBroker):
        def place_order(self, **kw):
            self.place_calls.append(kw)
            raise TimeoutError("never accepted")

    broker = AlwaysTimeout()
    engine = _wire(monkeypatch, broker)

    with pytest.raises(HTTPException) as err:
        orders_routes.place_single(_body(), ref_ltp=100.0)
    assert err.value.status_code == 502
    assert len(broker.place_calls) == 1
    assert engine.orders == {}
    assert engine.pending_entry_tags() == []
    assert engine.entry_intent_health()["counts"]["superseded"] == 1


def test_book_unavailable_keeps_intent_pending_until_reconcile(monkeypatch):
    broker = TagBroker(timeout_without_order=True)
    broker.report_error = RuntimeError("order report down")
    engine = _wire(monkeypatch, broker)

    with pytest.raises(HTTPException) as err:
        orders_routes.place_single(_body(), ref_ltp=100.0)
    assert err.value.status_code == 502
    assert len(broker.place_calls) == 1
    pending = engine.entry_intent_health()["pending"]
    assert len(pending) == 1
    tag = pending[0]["tag"]

    broker.report_error = None
    book = [_row("E9", tag)]
    summary = engine.reconcile(book, {})
    assert "E9" in engine.orders
    assert engine.orders["E9"]["entry_client_tag"] == tag
    assert engine.pending_entry_tags() == []
    assert summary["orphaned"] == []


def test_superseded_attempt_landing_late_is_adopted_and_alerted(
        monkeypatch, caplog):
    broker = TagBroker(timeout_without_order=True)
    engine = _wire(monkeypatch, broker)

    out = orders_routes.place_single(_body(), ref_ltp=100.0)
    first_tag = broker.place_calls[0]["tag"]
    assert out["entry_order_id"] == "E2"
    assert [r["tag"] for r in engine.entry_intent_health()["superseded"]] == [
        first_tag]

    caplog.set_level(logging.CRITICAL)
    late = _row("E1", first_tag, status="complete", filled="75", prc="101.00")
    summary = engine.reconcile([late], {})
    assert "E1" in engine.orders
    leg = engine.orders["E1"]
    assert leg["duplicate_entry"] is True
    assert leg["duplicate_entry_tag"] == first_tag
    assert leg["status"] == "ACTIVE"
    assert leg["sl_order_id"] == "SL1"
    assert broker.sl_calls == 1
    assert summary["orphaned"] == []
    assert any("DUPLICATE_ENTRY" in r.message for r in caplog.records)


def test_tag_match_ignores_symbol_qty_side(monkeypatch):
    class WrongFields(TagBroker):
        def place_order(self, **kw):
            self.place_calls.append(kw)
            self.book.append(_row("E1", kw["tag"], symbol="OTHER", qty="1",
                                  side="S"))
            raise TimeoutError("timeout after accept with different fields")

    broker = WrongFields()
    engine = _wire(monkeypatch, broker)

    out = orders_routes.place_single(_body(), ref_ltp=100.0)
    assert out["entry_order_id"] == "E1"
    assert len(broker.place_calls) == 1
    assert "E1" in engine.orders


def test_unique_untagged_row_falls_back_to_symbol_qty_side(monkeypatch):
    class Untagged(TagBroker):
        def place_order(self, **kw):
            self.place_calls.append(kw)
            self.book.append(_row("E1", "", symbol=kw["trading_symbol"],
                                  qty=kw["quantity"],
                                  side=kw["transaction_type"]))
            raise TimeoutError("timeout after accept, tag missing")

    broker = Untagged()
    engine = _wire(monkeypatch, broker)

    out = orders_routes.place_single(_body(), ref_ltp=100.0)
    assert out["entry_order_id"] == "E1"
    assert len(broker.place_calls) == 1
    assert "E1" in engine.orders


def test_multiple_tag_matches_are_not_adopted_and_do_not_retry(
        monkeypatch, caplog):
    class TwoMatches(TagBroker):
        def place_order(self, **kw):
            self.place_calls.append(kw)
            tag = kw["tag"]
            self.book.extend([
                _row("E1", tag), _row("E2", tag),
            ])
            raise TimeoutError("timed out with two matching rows")

    broker = TwoMatches()
    engine = _wire(monkeypatch, broker)
    caplog.set_level(logging.CRITICAL)

    with pytest.raises(HTTPException) as err:
        orders_routes.place_single(_body(), ref_ltp=100.0)
    assert err.value.status_code == 502
    assert len(broker.place_calls) == 1
    assert engine.orders == {}
    assert len(engine.pending_entry_tags()) == 1
    assert any("ENTRY AMBIGUOUS" in r.message for r in caplog.records)


def test_discovered_pending_entry_tag_is_adopted_by_reconcile(monkeypatch):
    broker = TagBroker()
    engine = _wire(monkeypatch, broker)
    tag = "Ediscover-1"
    engine.register_entry_intent(tag, _payload())

    summary = engine.reconcile([_row("E7", tag)], {})
    assert "E7" in engine.orders
    leg = engine.orders["E7"]
    assert leg["entry_client_tag"] == tag
    assert engine.pending_entry_tags() == []
    assert summary["orphaned"] == []


def test_duplicate_tag_never_double_tracks_or_silently_drops(monkeypatch, caplog):
    broker = TagBroker()
    engine = _wire(monkeypatch, broker)
    tag = "Edup-1"
    engine.register_entry_intent(tag, _payload())
    engine.commit_entry(tag, "E1")
    assert list(engine.orders) == ["E1"]

    # A second broker order carrying the same entry tag must not become another
    # tracker; it is alerted as an orphan so the duplicate is visible.
    caplog.set_level(logging.CRITICAL)
    engine.on_order_update(_row("E9", tag))
    assert list(engine.orders) == ["E1"]
    assert "E9" in engine.orphaned_orders
    assert any("ORPHAN TAGGED ORDER" in r.message for r in caplog.records)


def test_registration_collision_checks_existing_tracker_tag(monkeypatch):
    broker = TagBroker()
    engine = _wire(monkeypatch, broker)
    engine.add({"entry_order_id": "OLD", "sl_order_id": "SL0",
                "trading_symbol": "NIFTY25N1825000CE", "status": "ACTIVE",
                "strategy": "market_exit", "target_price": "120.00",
                "stop_loss_price": "90.00", "trail_distance": 5,
                "high_water_mark": 0.0, "low_water_mark": 0.0,
                "quantity": "75", "exchange_segment": "nse_fo",
                "instrument_token": "tok", "side": "B", "product": "MIS",
                "target_points": "20", "stop_points": "10",
                "entry_client_tag": "KEcollision-1"})
    tags = iter(["KEcollision-1", "KEfresh-2"])
    monkeypatch.setattr(SyntheticEngine, "make_tag",
                        staticmethod(lambda *_a, **_kw: next(tags)))

    out = orders_routes.place_single(_body(), ref_ltp=100.0)
    assert out["client_tag"] == "KEfresh-2"
    assert len(broker.place_calls) == 1
    assert set(engine.orders) == {"OLD", "E1"}


def test_entry_fill_before_registration_uses_pre_registered_intent(monkeypatch):
    broker = TagBroker(fill_on_place=True)
    engine = _wire(monkeypatch, broker)

    out = orders_routes.place_single(_body(), ref_ltp=100.0)
    assert len(broker.place_calls) == 1
    assert out["entry_order_id"] == "E1"
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["sl_order_id"] == "SL1"
    assert leg["entry_client_tag"] == out["client_tag"]
    assert broker.sl_calls == 1


def test_tags_preserve_unique_tail_with_prefix_and_fit_20(monkeypatch):
    monkeypatch.setattr(settings, "order_tag_prefix", "K")
    a = SyntheticEngine.make_tag("E", "aaaaaaaaaa", 1)
    b = SyntheticEngine.make_tag("E", "baaaaaaaaab", 1)
    assert a != b
    assert a.startswith("KE")
    assert a.endswith("-1")
    assert b.startswith("KE")
    assert len(a) <= 20 and len(b) <= 20

    # Long refs with an identical tail must still differ (hash is included).
    c = SyntheticEngine.make_tag("S", "1" + "A" * 30, 2)
    d = SyntheticEngine.make_tag("S", "2" + "A" * 30, 2)
    assert c != d
    assert len(c) <= 20 and len(d) <= 20
    assert c.startswith("KS") and c.endswith("-2")


def test_manual_exit_tag_is_known_and_not_reported_as_orphan(monkeypatch):
    broker = TagBroker()
    engine = _wire(monkeypatch, broker)
    tag = SyntheticEngine.make_tag("X", "manualref", 1)
    assert engine.register_manual_exit_tag(tag, "NIFTY25N1825000CE", "click-1")
    assert not engine.register_manual_exit_tag(tag, "OTHER", "click-2")

    engine.on_order_update(_row("M1", tag, status="open", side="S"))
    assert "M1" not in engine.orphaned_orders
    rec = engine.manual_exit_tag_record(tag)
    assert rec["order_id"] == "M1"
    assert rec["state"] == "pending"


def test_sl_and_exit_tags_never_collide_with_prefix_and_long_refs(monkeypatch):
    monkeypatch.setattr(settings, "order_tag_prefix", "PREFIX")
    ref = "A" * 32
    sl = SyntheticEngine.make_tag("S", ref, 999)
    ex = SyntheticEngine.make_tag("X", ref, 999)
    assert sl != ex
    assert sl.startswith("PREFIXS") and ex.startswith("PREFIXX")
    assert sl.endswith("-999") and ex.endswith("-999")
    assert len(sl) <= 20 and len(ex) <= 20
    assert SyntheticEngine.is_our_tag_format(sl)
    assert SyntheticEngine.is_our_tag_format(ex)


def test_tag_prefix_validator_rejects_long_or_invalid(monkeypatch):
    with pytest.raises(ValidationError):
        Settings(order_tag_prefix="X" * 20)
    with pytest.raises(ValidationError):
        Settings(order_tag_prefix="bad prefix!")
