"""WP2: order-feed watchdog + periodic reconcile + clean shutdown.

Fake feed/order-client style: a session yields messages then hangs forever
(the SDK iterator never raising) — the watchdog must force a new session.
Reconcile runs on every connect and on a timer, always through the worker so
two reconciles can never overlap.
"""
import asyncio
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

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


class HangingFeed:
    def __init__(self, msgs, connected=True):
        self.msgs = list(msgs)
        self.closed = False
        self._connected = connected

    @property
    def is_connected(self):
        return self._connected

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.msgs:
            return self.msgs.pop(0)
        await asyncio.Event().wait()  # never returns, never raises
        raise StopAsyncIteration  # pragma: no cover


class LiveFeed:
    """Yields a message forever (feed alive, no watchdog churn)."""

    def __init__(self, msg_factory):
        self.msg_factory = msg_factory
        self.closed = False

    @property
    def is_connected(self):
        return True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0.02)
        return self.msg_factory()


class FakeOrderClient:
    def __init__(self, msg_factory, live=False, connected=True):
        self.msg_factory = msg_factory
        self.live = live
        self.connected = connected
        self.sessions = 0
        self.feeds: list = []

    def create_order_feed(self, max_reconnect_attempts=0):
        self.sessions += 1
        if self.live:
            feed = LiveFeed(self.msg_factory)
        else:
            msgs = self.msg_factory()
            feed = HangingFeed(
                msgs if isinstance(msgs, (list, tuple)) else [msgs],
                connected=self.connected)
        self.feeds.append(feed)
        return feed


class FakeKotak:
    def __init__(self, client):
        self.client = client
        self.book_calls = 0
        self.position_calls = 0

    def order_book(self):
        self.book_calls += 1
        return {"data": []}

    def positions(self, strict=False):
        self.position_calls += 1
        return {}


class SlowEngine(SyntheticEngine):
    """Reconcile that records overlap and takes long enough to collide if it
    were ever run concurrently."""

    def __init__(self, broker, delay=0.12):
        super().__init__(broker)
        self.delay = delay
        self.calls = 0
        self.active = 0
        self.max_active = 0
        self._stats = threading.Lock()

    def reconcile(self, book, positions=None):
        with self._stats:
            self.calls += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(self.delay)
            return {"checked": 0, "driven": [], "repaired": [], "errored": [],
                    "naked": [], "positions": 0}
        finally:
            with self._stats:
                self.active -= 1


def _wait(pred, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.01)
    return False


def _bridge(engine, kotak, *, stale=10.0, watchdog=0.02, reconcile=10.0,
            retry=0.02, idle=300.0):
    hub = MagicMock()
    hub.main_loop = None
    b = SFeedBridge(
        hub=hub, engine=engine, kotak=kotak,
        worker=CommandWorker("watchdog-worker", max_queue=200, stall_ms=1000,
                             stop_timeout=0.5),
        retry_sec=retry, feed_stale_sec=stale,
        feed_watchdog_sec=watchdog, feed_idle_reconnect_sec=idle,
        reconcile_sec=reconcile)

    async def market():
        while not b._stop.is_set():
            await asyncio.sleep(0.02)

    b._market_session = market
    return b


def _order_msg():
    return OrderUpdate(OrderData(
        order_id="UNKNOWN", order_status="open", trading_symbol="X",
        transaction_type="B", quantity=1, average_price="5"))


def test_disconnected_order_feed_triggers_watchdog_reconnect():
    # A quiet-but-connected feed is healthy; a closed SDK socket is not.
    client = FakeOrderClient(lambda: [_order_msg()], connected=False)
    kotak = FakeKotak(client)
    engine = SyntheticEngine(MagicMock())
    b = _bridge(engine, kotak, stale=0.15, watchdog=0.02, reconcile=10,
                retry=0.02)
    b.start()
    try:
        assert _wait(lambda: client.sessions >= 2, 3.0), \
            "watchdog did not force a reconnect on a disconnected feed"
        assert b.health()["watchdog_reconnects"] >= 1
        assert client.feeds and client.feeds[0].closed
        assert _wait(lambda: kotak.book_calls >= 2, 3.0), \
            "reconcile did not run on each reconnect"
    finally:
        b.stop()


def test_quiet_healthy_feed_is_not_recycled():
    # One order update then silence, while the SDK still reports connected.
    client = FakeOrderClient(lambda: [_order_msg()], connected=True)
    kotak = FakeKotak(client)
    b = _bridge(SyntheticEngine(MagicMock()), kotak, stale=0.05,
                watchdog=0.02, idle=0.05, reconcile=10, retry=0.02)
    b.start()
    try:
        assert _wait(lambda: b._last_order_ok_ms is not None, 2.0)
        time.sleep(0.35)  # > both stale and idle thresholds
        assert client.sessions == 1, "quiet healthy feed was reconnected"
        assert b.health()["watchdog_reconnects"] == 0
        assert b._order_connected
    finally:
        b.stop()


def test_idle_fallback_reconnects_only_without_connection_signal():
    client = FakeOrderClient(lambda: [_order_msg()], connected=None)
    kotak = FakeKotak(client)
    b = _bridge(SyntheticEngine(MagicMock()), kotak, stale=0.05,
                watchdog=0.02, idle=0.15, reconcile=10, retry=0.02)
    b.start()
    try:
        assert _wait(lambda: client.sessions >= 2, 3.0), \
            "idle fallback did not reconnect a feed with no liveness signal"
    finally:
        b.stop()


def _empty_msgs():
    return []


@pytest.mark.parametrize("live", [False, True], ids=["hung", "alive"])
def test_periodic_reconcile_runs_and_never_overlaps(live):
    # hung: no messages at all (stale threshold is 10 s, so it stays open);
    # alive: one message every 20 ms.
    client = FakeOrderClient(_order_msg, live=live)
    kotak = FakeKotak(client)
    engine = SlowEngine(MagicMock(), delay=0.12)
    b = _bridge(engine, kotak, stale=10.0, watchdog=0.02, reconcile=0.05,
                retry=0.02)
    b.start()
    try:
        assert _wait(lambda: engine.calls >= 3, 3.0)
        assert b._order_connected, "feed sessions should still be open"
        assert engine.max_active == 1, "reconcile overlapped itself"
        h = b.health()
        assert h["last_reconcile_ms"] is not None
        assert h["last_reconcile_age_ms"] is not None
        assert "checked" in h["last_reconcile"]
    finally:
        b.stop()


class FailingBookKotak(FakeKotak):
    def order_book(self):
        self.book_calls += 1
        raise TimeoutError("order book timed out")


def test_failed_book_fetch_never_errors_live_legs():
    client = FakeOrderClient(_empty_msgs)
    kotak = FailingBookKotak(client)
    engine = SyntheticEngine(MagicMock())
    engine.add({"entry_order_id": "E1", "sl_order_id": "SL1",
                "trading_symbol": "X", "status": "ACTIVE",
                "strategy": "market_exit", "target_price": "120",
                "stop_loss_price": "90", "quantity": "20",
                "exchange_segment": "nse_fo", "instrument_token": "tok",
                "side": "B", "product": "MIS", "sl_last_seen_ms": _now_ms()})
    b = _bridge(engine, kotak, reconcile=10)
    b._run_reconcile("test")
    assert engine.orders["E1"]["status"] == "ACTIVE"
    h = b.health()
    assert h["last_reconcile_ms"] is None
    assert h["last_reconcile_ok_ms"] is None
    assert h["last_reconcile_error"] == "order_book_unavailable"
    assert h["last_reconcile_error_ms"] is not None


def _now_ms():
    return int(time.time() * 1000)


def test_empty_book_is_authoritative():
    b = _bridge(SyntheticEngine(MagicMock()), FakeKotak(None), reconcile=10)
    book, positions, ok = b._fetch_book_positions()
    assert ok is True and book == [] and positions == {}


def test_stop_cleans_up_loop_worker_and_scheduler():
    client = FakeOrderClient(_empty_msgs)
    kotak = FakeKotak(client)
    b = _bridge(SyntheticEngine(MagicMock()), kotak, stale=10.0,
                watchdog=0.02, reconcile=0.05)
    b.start()
    assert _wait(lambda: b.worker.is_running() and b._thread and b._thread.is_alive(), 2.0)
    t0 = time.monotonic()
    b.stop()
    elapsed = time.monotonic() - t0
    assert elapsed < 3.0
    assert b._thread is None
    assert b._scheduler_thread is None
    assert not b.worker.is_running()
    assert b.worker._thread is None or not b.worker._thread.is_alive()
    assert b._started is False


def test_health_endpoint_exposes_freshness_fields(monkeypatch):
    from app import main

    now = int(time.time() * 1000)
    monkeypatch.setattr(main.bridge, "_last_order_msg_ms", now - 1234)
    monkeypatch.setattr(main.bridge, "_last_tick_ms", now - 500)
    monkeypatch.setattr(main.bridge, "_last_reconcile_ms", now - 2000)
    monkeypatch.setattr(main.bridge, "_last_reconcile_summary", {"checked": 3})

    with TestClient(main.app) as c:
        r = c.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["order_age_ms"] >= 1000
    assert body["tick_age_ms"] >= 400
    assert body["last_reconcile_ms"] == now - 2000
    assert "feed" in body and body["feed"]["last_order_msg_ms"] == now - 1234
    assert "worker" in body and "queue_depth" in body["worker"]
    assert "worker_last_error" in body
