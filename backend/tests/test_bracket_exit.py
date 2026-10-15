"""Regression tests for the 15-Oct SENSEX short incident:
  - MKT exits must send price "0" ("" was rejected by OMS, killing every
    synthetic target-exit and Exit-button flatten).
  - A duplicate 'cancelled' feed event for the SL must NOT fire a second
    market exit (position flip) nor mark the leg ERRORED.
  - Exit-order updates must resolve the tracker: complete -> CLOSED,
    rejected -> ERRORED with the reject reason.
  - Entry _leg pricing must be symmetric: MKT marketable both sides,
    L-1 passive both sides with configurable offset, L explicit price.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app.routes.orders as orders_routes  # noqa: E402
from app.schemas import SingleOrderBody, StraddleBody  # noqa: E402
from app.synthetic_engine import SyntheticEngine  # noqa: E402


def _leg_tracker(**over):
    d = {"entry_order_id": "E1", "sl_order_id": None,
         "trading_symbol": "NIFTY25N1825000CE", "status": "PENDING_ENTRY",
         "strategy": "market_exit", "target_price": "120.00", "stop_loss_price": "90.00",
         "trail_distance": 5, "high_water_mark": 0.0, "low_water_mark": 0.0,
         "quantity": "50", "exchange_segment": "nse_fo", "instrument_token": "tok",
         "side": "B", "product": "MIS", "target_points": "20", "stop_points": "10"}
    d.update(over)
    return d


@pytest.fixture()
def pending():
    broker = MagicMock()
    broker.place_stop_loss.return_value = {"stCode": 200, "nOrdNo": "SL9"}
    broker.average_fill_price.return_value = None
    eng = SyntheticEngine(broker=broker)
    eng.add(_leg_tracker())
    return eng, broker


@pytest.fixture()
def tracked():
    broker = MagicMock()
    broker.place_market_exit.return_value = {"stCode": 200, "nOrdNo": "X1"}
    eng = SyntheticEngine(broker=broker)
    eng.add({"entry_order_id": "E1", "sl_order_id": "SL1", "trading_symbol": "SENSEX26O1574200PE",
             "status": "CLOSING", "strategy": "market_exit", "target_price": "139.50",
             "stop_loss_price": "179.50", "trail_distance": 5, "high_water_mark": 0.0,
             "low_water_mark": 0.0, "quantity": "20", "exchange_segment": "bse_fo",
             "instrument_token": "tok", "side": "S", "product": "MIS",
             "target_points": "20", "stop_points": "20"})
    return eng, broker


def test_mkt_exit_sends_price_zero():
    from app.kotak_client import KotakService
    from datetime import date
    s = KotakService(consumer_key="dummy")
    fake = MagicMock()
    fake.place_order.return_value = {"stCode": 200, "nOrdNo": "X9"}
    s.client = fake
    s.login_date = date.today()
    s.place_market_exit("bse_fo", "20", "SENSEX26O1574200PE", transaction_type="B")
    assert fake.place_order.call_args.kwargs["price"] == "0"
    assert fake.place_order.call_args.kwargs["order_type"] == "MKT"


def test_duplicate_sl_cancel_places_single_exit(tracked):
    eng, broker = tracked
    eng.on_order_update({"nOrdNo": "SL1", "ordSt": "cancelled"})
    assert broker.place_market_exit.call_count == 1
    assert eng.orders["E1"]["exit_order_id"] == "X1"
    assert eng.orders["E1"]["status"] == "CLOSING"
    # duplicate confirm/snapshot for the same SL — must be ignored
    eng.on_order_update({"nOrdNo": "SL1", "ordSt": "cancelled"})
    assert broker.place_market_exit.call_count == 1
    assert eng.orders["E1"]["status"] == "CLOSING"
    assert "error" not in eng.orders["E1"]


def test_exit_fill_closes_and_reject_errors(tracked):
    eng, broker = tracked
    eng.on_order_update({"nOrdNo": "SL1", "ordSt": "cancelled"})
    eng.on_order_update({"nOrdNo": "X1", "ordSt": "complete", "prc": "139.30"})
    assert eng.orders["E1"]["status"] == "CLOSED"

    eng.orders["E1"].update(status="CLOSING", exit_order_id="X2")
    broker.place_market_exit.return_value = {"stCode": 200, "nOrdNo": "X2"}
    eng.on_order_update({"nOrdNo": "X2", "ordSt": "rejected", "rejRsn": "EXCHANGE: FROZEN"})
    assert eng.orders["E1"]["status"] == "ERRORED"
    assert "FROZEN" in eng.orders["E1"]["error"]


def test_leg_symmetry():
    buy_mkt = orders_routes._leg("SENSEX", "26O15", "PE", "74200", 1, "MKT", 110.0, 0, "B")[3]
    sell_mkt = orders_routes._leg("SENSEX", "26O15", "PE", "74200", 1, "MKT", 110.0, 0, "S")[3]
    assert buy_mkt == 115.0 and sell_mkt == 105.0  # both marketable
    buy_l1 = orders_routes._leg("SENSEX", "26O15", "PE", "74200", 1, "L-1", 110.0, 0, "B", 2.0)[3]
    sell_l1 = orders_routes._leg("SENSEX", "26O15", "PE", "74200", 1, "L-1", 110.0, 0, "S", 2.0)[3]
    assert buy_l1 == 108.0 and sell_l1 == 112.0  # both passive
    assert orders_routes._leg("SENSEX", "26O15", "PE", "74200", 1, "L",
                              110.0, 0, "S", 1.0, 109.5)[3] == 109.5


# ---------- M6: derived entry price floor ----------
def test_leg_price_floored_at_one_tick():
    # SELL MKT on a cheap premium computes ref-5 <= 0 -> clamped to 0.05
    assert orders_routes._leg("SENSEX", "26O15", "PE", "74200", 1, "MKT", 3.0, 0, "S")[3] == 0.05
    # normal premiums are unchanged
    assert orders_routes._leg("SENSEX", "26O15", "PE", "74200", 1, "MKT", 110.0, 0, "S")[3] == 105.0
    assert orders_routes._leg("SENSEX", "26O15", "PE", "74200", 1, "MKT", 110.0, 0, "B")[3] == 115.0


# ---------- H3: strict fill gate ----------
def test_entry_fill_places_sl_once_from_actual_fill(pending):
    eng, broker = pending
    eng.on_order_update({"nOrdNo": "E1", "ordSt": "complete", "prc": "100.00", "fldQty": 50})
    leg = eng.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert broker.place_stop_loss.call_count == 1
    assert leg["target_price"] == "120.00"   # recomputed from fill (side B)
    assert leg["stop_loss_price"] == "90.00"
    assert leg["fill_price"] == "100.00"
    assert leg["sl_quantity"] == 50
    # duplicate complete frame -> SL placed exactly once
    eng.on_order_update({"nOrdNo": "E1", "ordSt": "complete", "prc": "100.00", "fldQty": 50})
    assert broker.place_stop_loss.call_count == 1
    assert broker.modify_sl.call_count == 0


def test_partial_fill_sizes_sl_to_filled_qty(pending):
    eng, broker = pending
    eng.on_order_update({"nOrdNo": "E1", "ordSt": "complete", "prc": "100.00", "fldQty": 20})
    leg = eng.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert broker.place_stop_loss.call_args.args[1] == "20"  # quantity = filled, not 50
    assert leg["quantity"] == "20" and leg["sl_quantity"] == 20


def test_partial_then_remainder_resizes_sl(pending):
    eng, broker = pending
    eng.on_order_update({"nOrdNo": "E1", "ordSt": "complete", "prc": "100.00", "fldQty": 20})
    eng.on_order_update({"nOrdNo": "E1", "ordSt": "complete", "prc": "100.00", "fldQty": 50})
    assert broker.place_stop_loss.call_count == 1  # never a second SL
    broker.modify_sl.assert_called_once()
    assert eng.orders["E1"]["sl_quantity"] == 50
    assert eng.orders["E1"]["quantity"] == "50"


def test_complete_without_price_or_history_errors_no_sl(pending):
    eng, broker = pending
    broker.average_fill_price.return_value = None
    eng.on_order_update({"nOrdNo": "E1", "ordSt": "complete", "fldQty": 50})
    leg = eng.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert "fill price" in leg["error"]
    assert broker.place_stop_loss.call_count == 0


def test_fill_price_fetched_from_history_when_feed_lacks_it(pending):
    eng, broker = pending
    broker.average_fill_price.return_value = 101.5
    eng.on_order_update({"nOrdNo": "E1", "ordSt": "complete", "fldQty": 50})
    assert broker.average_fill_price.called
    assert eng.orders["E1"]["status"] == "ACTIVE"
    assert eng.orders["E1"]["fill_price"] == "101.50"


def test_entry_reject_errors_without_sl(pending):
    eng, broker = pending
    eng.on_order_update({"nOrdNo": "E1", "ordSt": "rejected", "rejRsn": "EXCHANGE: RMS"})
    assert eng.orders["E1"]["status"] == "ERRORED"
    assert "RMS" in eng.orders["E1"]["error"]
    assert broker.place_stop_loss.call_count == 0


def test_entry_cancel_closes_without_sl(pending):
    eng, broker = pending
    eng.on_order_update({"nOrdNo": "E1", "ordSt": "cancelled"})
    assert eng.orders["E1"]["status"] == "CLOSED"
    assert eng.orders["E1"]["exit_reason"] == "ENTRY_CANCELLED"
    assert broker.place_stop_loss.call_count == 0


# ---------- M5: server-side idempotency ----------
def _wire(monkeypatch):
    broker = MagicMock()
    broker.place_order.return_value = {"stCode": 200, "nOrdNo": "E-DUP"}
    broker.token_for.return_value = "tok"
    eng = SyntheticEngine(broker=broker)
    monkeypatch.setattr(orders_routes, "kotak", broker)
    monkeypatch.setattr(orders_routes, "engine", eng)
    monkeypatch.setattr(orders_routes, "hub", None)
    monkeypatch.setattr(orders_routes, "bridge", None)
    return broker


def test_single_order_idempotent_replay(monkeypatch):
    broker = _wire(monkeypatch)
    body = SingleOrderBody(instrument="NIFTY", expiry="25N18", option_type="CE",
                           strike="25000", lots=1, entry_mode="MKT", side="B",
                           client_order_tag="dup-single-1")
    r1 = orders_routes.place_single(body, ref_ltp=100.0)
    r2 = orders_routes.place_single(body, ref_ltp=100.0)
    assert broker.place_order.call_count == 1  # double request -> one order
    assert r1 == r2 and r1["entry_order_id"] == "E-DUP"


def test_distinct_tags_place_distinct_orders(monkeypatch):
    broker = _wire(monkeypatch)
    b1 = SingleOrderBody(instrument="NIFTY", expiry="25N18", option_type="CE", strike="25000",
                         lots=1, entry_mode="MKT", side="B", client_order_tag="dup-a")
    b2 = SingleOrderBody(instrument="NIFTY", expiry="25N18", option_type="CE", strike="25000",
                         lots=1, entry_mode="MKT", side="B", client_order_tag="dup-b")
    orders_routes.place_single(b1, ref_ltp=100.0)
    orders_routes.place_single(b2, ref_ltp=100.0)
    assert broker.place_order.call_count == 2


def test_straddle_idempotent_replay(monkeypatch):
    broker = _wire(monkeypatch)
    body = StraddleBody(instrument="NIFTY", expiry="25N18", ce_strike="25000",
                        pe_strike="25100", lots=1, client_order_tag="dup-straddle-1")
    r1 = orders_routes.place_straddle(body, ce_ltp=100.0, pe_ltp=90.0)
    r2 = orders_routes.place_straddle(body, ce_ltp=100.0, pe_ltp=90.0)
    assert broker.place_order.call_count == 2  # one per leg, once
    assert r1 == r2 and len(r1["legs"]) == 2
