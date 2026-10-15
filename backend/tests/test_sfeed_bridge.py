"""SFeed bridge tests — pure mappers + lifecycle, fully offline."""
import asyncio
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.sfeed_bridge import (SFeedBridge, map_book_entry,  # noqa: E402
                              map_feed_message, map_order_data)
from app.synthetic_engine import SyntheticEngine  # noqa: E402


class SFeedScrip:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class SFeedIndex:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class OrderUpdate:
    def __init__(self, data):
        self.data = data


class OrderData:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def test_map_scrip_tick():
    t = map_feed_message(SFeedScrip(instrument_token="12345",
                                    last_traded_price=99.5,
                                    trading_symbol="NIFTY26O1525450CE"))
    assert t == {"tk": "12345", "ltp": "99.5", "ts": "NIFTY26O1525450CE"}


def test_map_index_tick():
    t = map_feed_message(SFeedIndex(name="Nifty 50", last_traded_price=25400.1))
    assert t == {"tk": "Nifty 50", "ltp": "25400.1", "ts": "Nifty 50"}


def test_map_unknown_none():
    assert map_feed_message(object()) is None
    assert map_feed_message(SFeedScrip(instrument_token=None,
                                       last_traded_price=None)) is None


def test_map_order_data():
    od = map_order_data(OrderData(order_id="25100800001", order_status="complete",
                                  trading_symbol="SENSEX26O1574300CE",
                                  transaction_type="B", quantity=20,
                                  average_price="13.25", filled_quantity=20))
    assert od["nOrdNo"] == "25100800001"
    assert od["ordSt"] == "complete"
    assert od["trdSym"] == "SENSEX26O1574300CE"
    assert od["trnsTp"] == "B"


def _bridge():
    hub = MagicMock()
    hub.main_loop = None
    engine = MagicMock()
    engine.all.return_value = []
    kotak = MagicMock()
    kotak.client = MagicMock()
    return SFeedBridge(hub=hub, engine=engine, kotak=kotak, retry_sec=0.05)


def test_bridge_start_stop_offline():
    b = _bridge()
    b.kotak.client.create_websocket.side_effect = RuntimeError("no net")
    b.start()
    time.sleep(0.3)  # let supervisor spin + retry at least once
    b.ensure_option("nse_fo", "99999")  # records wanted, no live ws
    assert ("nse_fo", "99999") in b._wanted
    b.stop()
    assert not (b._thread and b._thread.is_alive())


def test_handle_orderupdate_feeds_engine():
    b = _bridge()
    pushed = []
    b._push = pushed.append
    b._handle_orderfeed_message(OrderUpdate(OrderData(
        order_id="7", order_status="complete", trading_symbol="X",
        transaction_type="S", quantity=1, average_price="5")))
    b.engine.on_order_update.assert_called_once()
    assert pushed and pushed[0]["kind"] == "order"


# ---------- Plan 03: decoupled supervisors ----------
def test_order_feed_survives_market_drop():
    """A market session that raises must not tear down the order supervisor."""
    b = _bridge()
    events = []

    async def market():
        events.append("market-start")
        await asyncio.sleep(0)
        raise RuntimeError("market dropped")

    async def orders():
        events.append("orders-start")
        for _ in range(30):
            await asyncio.sleep(0.01)
        events.append("orders-done")
        b._stop.set()

    b._market_session = market
    b._order_session = orders
    asyncio.run(b._supervise())
    assert "orders-done" in events                 # order supervisor ran to completion
    assert events.count("market-start") > 1        # market supervisor independently retried


def test_stop_cancels_both_supervisors():
    b = _bridge()
    started = []

    async def market():
        started.append("market")
        await asyncio.sleep(30)

    async def orders():
        started.append("orders")
        await asyncio.sleep(30)

    b._market_session = market
    b._order_session = orders
    b.start()
    time.sleep(0.3)
    assert set(started) == {"market", "orders"}
    b.stop()
    assert not (b._thread and b._thread.is_alive())


# ---------- Plan 03: reconcile ----------
def test_map_book_entry_wire_abbrev():
    od = map_book_entry({"nOrdNo": "9", "ordSt": "complete", "trdSym": "X",
                         "trnsTp": "S", "qty": 20, "avgPrc": "12.5", "fldQty": 20})
    assert od["nOrdNo"] == "9" and od["trdSym"] == "X"
    assert od["prc"] == "12.5" and od["fldQty"] == 20 and od["trnsTp"] == "S"
    assert map_book_entry({}) is None
    assert map_book_entry({"foo": "bar"}) is None


def _pending_leg(eid: str = "E1", **kw) -> dict:
    d = {"entry_order_id": eid, "sl_order_id": None, "trading_symbol": "X",
         "status": "PENDING_ENTRY", "strategy": "market_exit", "target_price": "30",
         "stop_loss_price": "5", "trail_distance": 5, "high_water_mark": 0.0,
         "low_water_mark": 0.0, "quantity": "20", "exchange_segment": "nse_fo",
         "instrument_token": "tok", "side": "B", "product": "MIS",
         "target_points": "20", "stop_points": "20"}
    d.update(kw)
    return d


def test_reconcile_drives_missed_fill_once():
    broker = MagicMock()
    broker.place_stop_loss.return_value = {"stCode": 200, "nOrdNo": "SL1"}
    eng = SyntheticEngine(broker=broker)
    eng.add(_pending_leg())
    book = [{"nOrdNo": "E1", "ordSt": "complete", "trdSym": "X", "trnsTp": "B",
             "qty": 20, "prc": "13.25", "fldQty": 20, "rejRsn": "--"}]
    s = eng.reconcile(book, {})
    assert s["driven"] == ["E1"]
    assert broker.place_stop_loss.call_count == 1
    assert eng.orders["E1"]["status"] == "ACTIVE"
    assert eng.orders["E1"]["sl_order_id"] == "SL1"

    # SL now working in the book -> reconcile must not re-place it
    book2 = book + [{"nOrdNo": "SL1", "ordSt": "open", "trdSym": "X", "trnsTp": "S"}]
    eng.reconcile(book2, {})
    assert broker.place_stop_loss.call_count == 1

    # a duplicate complete frame on top of a placed SL is a no-op too
    eng.on_order_update({"nOrdNo": "E1", "ordSt": "complete", "prc": "13.25"})
    assert broker.place_stop_loss.call_count == 1


def test_reconcile_terminal_entry_states():
    broker = MagicMock()
    eng = SyntheticEngine(broker=broker)
    eng.add(_pending_leg("E2"))
    eng.reconcile([{"nOrdNo": "E2", "ordSt": "rejected", "rejRsn": "RMS",
                    "trdSym": "X"}], {})
    assert eng.orders["E2"]["status"] == "ERRORED"
    assert "RMS" in eng.orders["E2"]["error"]

    eng.add(_pending_leg("E3"))
    eng.reconcile([{"nOrdNo": "E3", "ordSt": "cancelled", "trdSym": "X"}], {})
    assert eng.orders["E3"]["status"] == "CLOSED"
    assert broker.place_stop_loss.call_count == 0  # never placed on a dead entry


def test_reconcile_marks_naked_when_sl_missing_or_dead():
    broker = MagicMock()
    eng = SyntheticEngine(broker=broker)
    eng.add(_pending_leg("E1", status="ACTIVE", sl_order_id="SL9"))
    eng.reconcile([], {})  # tracked SL absent from the whole book
    assert eng.orders["E1"]["status"] == "ERRORED"
    assert "missing" in eng.orders["E1"]["error"]

    eng.add(_pending_leg("E2", status="ACTIVE", sl_order_id="SL8"))
    eng.reconcile([{"nOrdNo": "SL8", "ordSt": "cancelled", "trdSym": "X"}], {})
    assert eng.orders["E2"]["status"] == "ERRORED"
    assert "unprotected" in eng.orders["E2"]["error"]
