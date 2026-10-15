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
from app.synthetic_engine import SyntheticEngine  # noqa: E402


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
