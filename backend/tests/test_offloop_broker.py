"""WP1: slow broker I/O must never run on the feed loop.

The bridge loop only enqueues ticks/order updates onto the single-writer
command worker. A fake broker that blocks for 2 s (and a would-be 65 s call)
must therefore leave the feed handler responsive; queued events are still
applied, in order, once the broker call returns.
"""
import asyncio
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.broker_worker import CommandWorker
from app.sfeed_bridge import SFeedBridge
from app.synthetic_engine import SyntheticEngine


class OrderUpdate:
    def __init__(self, data):
        self.data = data


class OrderData:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _leg(eid="E1", token="tok", status="ACTIVE", target="120.00"):
    return {"entry_order_id": eid, "sl_order_id": f"SL-{eid}",
            "trading_symbol": "X", "status": status, "strategy": "market_exit",
            "target_price": target, "stop_loss_price": "90.00",
            "trail_distance": 5, "high_water_mark": 0.0, "low_water_mark": 0.0,
            "quantity": "20", "exchange_segment": "nse_fo", "instrument_token": token,
            "side": "B", "product": "MIS", "target_points": "20", "stop_points": "10"}


def _bridge(engine, broker, worker=None):
    hub = MagicMock()
    hub.main_loop = None
    kotak = MagicMock()
    kotak.client = MagicMock()
    return SFeedBridge(hub=hub, engine=engine, kotak=kotak,
                       worker=worker or CommandWorker("t-worker", max_queue=100,
                                                      stall_ms=100, stop_timeout=0.2),
                       retry_sec=0.02, feed_stale_sec=10, feed_watchdog_sec=0.05,
                       reconcile_sec=10)


def _wait(pred, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.01)
    return False


def test_slow_broker_2s_does_not_block_order_handling():
    started = threading.Event()
    broker = MagicMock()

    def cancel(_oid):
        started.set()
        time.sleep(2.0)
        return {"stCode": 200}

    broker.cancel.side_effect = cancel
    engine = SyntheticEngine(broker)
    engine.add(_leg())
    b = _bridge(engine, broker)
    b.worker.start()
    try:
        b.worker.submit(b._apply_tick, {"tk": "tok", "ltp": "130"}, label="on_tick")
        assert started.wait(1.0), "slow broker call never started"

        t0 = time.monotonic()
        b._handle_orderfeed_message(OrderUpdate(OrderData(
            order_id="UNKNOWN", order_status="complete", trading_symbol="X",
            transaction_type="B", quantity=1, average_price="5")))
        elapsed = time.monotonic() - t0
        assert elapsed < 0.2, f"feed handler blocked for {elapsed:.2f}s"

        # the order update is queued behind the broker call, then applied
        assert _wait(lambda: b._last_order_ok_ms is not None, 4.0)
        assert engine.orders["E1"]["status"] == "CLOSING"
        assert broker.cancel.call_count == 1
    finally:
        b.worker.stop()


class SFeedScrip:
    def __init__(self, token, ltp):
        self.instrument_token = token
        self.last_traded_price = ltp
        self.trading_symbol = "X"


class FakeMarketWs:
    def __init__(self, messages):
        self.messages = list(messages)
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False

    async def subscribe_scrips(self, _tokens):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.messages:
            await asyncio.sleep(0.01)
            return self.messages.pop(0)
        await asyncio.Event().wait()
        raise StopAsyncIteration  # pragma: no cover


class FakeOrderFeed:
    def __init__(self, messages, gate=None):
        self.messages = list(messages)
        self.gate = gate

    @property
    def is_connected(self):
        return True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.gate is not None:
            while not self.gate.is_set():
                await asyncio.sleep(0.01)
        if self.messages:
            return self.messages.pop(0)
        await asyncio.Event().wait()
        raise StopAsyncIteration  # pragma: no cover


class FakeFeedClient:
    def __init__(self, market_msgs, order_msgs, order_gate=None):
        self.market = FakeMarketWs(market_msgs)
        self.orders = FakeOrderFeed(order_msgs, gate=order_gate)

    def create_websocket(self, max_reconnect_attempts=0):
        return self.market

    def create_order_feed(self, max_reconnect_attempts=0):
        return self.orders


class SessionKotak:
    def __init__(self, client):
        self.client = client

    def order_book(self):
        return {"data": []}

    def positions(self, strict=False):
        return {}


def test_slow_broker_65s_keeps_real_market_and_order_feeds_alive():
    """Exercises `_market_session` + `_handle_orderfeed_message` end to end.

    The configured 65 s broker call is simulated with a released event (no
    sleeper). While it is blocked, a later market tick must still reach the hub
    and an order update must still be read and queued.
    """
    started = threading.Event()
    release = threading.Event()
    order_ready = threading.Event()
    broker = MagicMock()

    def cancel(_oid):
        started.set()
        release.wait(70.0)
        return {"stCode": 200}

    broker.cancel.side_effect = cancel
    engine = SyntheticEngine(broker)
    engine.add(_leg())
    hub = MagicMock()
    hub.main_loop = None
    hub.latest = {}
    client = FakeFeedClient(
        [SFeedScrip("tok", "130"), SFeedScrip("tok2", "55")],
        [OrderUpdate(OrderData(
            order_id="UNKNOWN", order_status="complete", trading_symbol="X",
            transaction_type="B", quantity=1, average_price="5"))],
        order_gate=order_ready)
    b = SFeedBridge(hub=hub, engine=engine, kotak=SessionKotak(client),
                    worker=CommandWorker("session-worker", max_queue=100,
                                         stall_ms=100, stop_timeout=0.2),
                    retry_sec=0.02, feed_stale_sec=10,
                    feed_watchdog_sec=0.02, reconcile_sec=10)
    b.start()
    try:
        assert started.wait(2.0), "slow broker call never started"
        assert _wait(lambda: hub.latest.get("tok2", {}).get("ltp") == "55", 2.0), \
            "market feed loop was blocked by the broker call"
        b._last_order_applied_ms = None
        t0 = time.monotonic()
        legs = engine.all()
        assert time.monotonic() - t0 < 0.2 and legs, \
            "engine lock was held across the slow broker call"
        order_ready.set()
        assert _wait(lambda: b.worker.status()["queue_depth"] >= 1, 2.0), \
            "order feed reader did not queue an update while blocked"
        assert b._last_order_applied_ms is None, \
            "order update was applied while the worker was still blocked"
    finally:
        order_ready.set()
        release.set()
        assert _wait(lambda: b._last_order_applied_ms is not None, 2.0), \
            "queued order update was lost after the broker call returned"
        b.stop()
    assert engine.orders["E1"]["status"] == "CLOSING"


def test_submit_wait_times_out_instead_of_hanging():
    worker = CommandWorker("timeout-worker", max_queue=10, call_timeout=0.05)
    worker.start()
    try:
        with pytest.raises(TimeoutError):
            worker.submit_wait(time.sleep, 0.5, label="slow")
    finally:
        worker.stop(timeout=0.01)


def test_place_route_registers_leg_before_queued_fill_is_applied():
    """Route place_order + engine.add must be one worker command.

    The fake broker queues a fill the instant the entry is placed. On the old
    design (add on the request thread) that fill could run first and be dropped;
    here engine.add is recorded on the worker thread before the fill applies.
    """
    from app import schemas
    from app.routes import orders as orders_routes

    worker = CommandWorker("route-worker", max_queue=50, stall_ms=1000,
                           stop_timeout=0.2)
    placed_on: list[threading.Thread] = []

    class RouteBroker:
        master_ok = True

        def place_order(self, **_kw):
            fill = {"nOrdNo": "E1", "ordSt": "complete", "trdSym": "X",
                    "trnsTp": "B", "qty": 75, "prc": "101.00", "fldQty": 75,
                    "rejRsn": "--"}
            threading.Thread(
                target=lambda: worker.submit(
                    engine.on_order_update, fill, critical=True, label="fill"),
                daemon=True).start()
            return {"stCode": 200, "nOrdNo": "E1"}

        def token_for(self, *_a, **_kw):
            return "tok"

        def average_fill_price(self, _oid):
            return 101.0

        def place_stop_loss(self, *_a, **_kw):
            return {"stCode": 200, "nOrdNo": "SL1"}

    broker = RouteBroker()
    engine = SyntheticEngine(broker)
    orig_add = engine.add

    def record_add(d):
        placed_on.append(threading.current_thread())
        orig_add(d)

    engine.add = record_add
    orders_routes.kotak = broker
    orders_routes.engine = engine
    orders_routes.worker = worker
    orders_routes.hub = None
    orders_routes.bridge = None
    worker.start()
    try:
        body = schemas.SingleOrderBody(
            instrument="NIFTY", expiry="25N18", option_type="CE",
            strike="25450", lots=1, entry_mode="L", limit_price=100,
            target_points=20, stop_points=10)
        result = orders_routes.place_single(body, ref_ltp=100)
        assert result["entry_order_id"] == "E1"
        assert _wait(lambda: engine.orders["E1"]["status"] == "ACTIVE", 2.0), \
            "fill that arrived immediately after place_order was dropped"
        assert engine.orders["E1"]["sl_order_id"] == "SL1"
        assert placed_on and placed_on[0] is worker._thread, \
            "engine.add did not run inside the place_order worker command"
    finally:
        worker.stop()
        orders_routes.kotak = None
        orders_routes.engine = None
        orders_routes.worker = None


def test_concurrent_ticks_orders_reconcile_no_duplicate_sl():
    broker = MagicMock()
    broker.average_fill_price.return_value = None
    broker.cancel.return_value = {"stCode": 200}
    lock = threading.Lock()
    placed: list[str] = []

    def place_sl(*_a, **_kw):
        with lock:
            oid = f"SL{len(placed) + 1}"
            placed.append(oid)
        return {"stCode": 200, "nOrdNo": oid}

    broker.place_stop_loss.side_effect = place_sl

    engine = SyntheticEngine(broker)
    n = 20
    for i in range(n):
        leg = _leg(f"E{i}", token=f"tok{i}", status="PENDING_ENTRY", target="1000")
        leg["sl_order_id"] = None
        engine.add(leg)

    worker = CommandWorker("stress-worker", max_queue=5000, stall_ms=1000,
                           stop_timeout=1)
    b = _bridge(engine, broker, worker=worker)
    worker.start()

    def stress_reconcile():
        with lock:
            sl_ids = list(placed)
        book = [{"nOrdNo": f"E{i}", "ordSt": "complete", "trdSym": "X",
                 "trnsTp": "B", "qty": 20, "prc": "100.00", "fldQty": 20,
                 "rejRsn": "--"} for i in range(n)]
        book += [{"nOrdNo": oid, "ordSt": "open", "trdSym": "X", "trnsTp": "S"}
                 for oid in sl_ids]
        engine.reconcile(book, {})

    try:
        for i in range(n):
            worker.submit(b._handle_orderfeed_message, OrderUpdate(OrderData(
                order_id=f"E{i}", order_status="complete", trading_symbol="X",
                transaction_type="B", quantity=20, average_price="100.00",
                filled_quantity=20)), critical=True, label="order")
        for _ in range(10):
            for i in range(n):
                worker.submit(b._apply_tick, {"tk": f"tok{i}", "ltp": "50"},
                              label="tick")
            worker.submit(stress_reconcile, critical=True, label="reconcile")

        assert _wait(lambda: worker.status()["queue_depth"] == 0, 8.0)
        assert _wait(lambda: all(o["status"] == "ACTIVE" for o in engine.all()), 8.0)
        legs = engine.all()
        assert len({o["sl_order_id"] for o in legs}) == n  # one SL per leg
        assert broker.place_stop_loss.call_count == n       # never duplicated
        assert all(o["status"] in ("ACTIVE", "CLOSING", "CLOSED") for o in legs)
    finally:
        worker.stop()
