"""SFeed bridge tests — pure mappers + lifecycle, fully offline."""
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.sfeed_bridge import (SFeedBridge, map_feed_message,  # noqa: E402
                              map_order_data)


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
