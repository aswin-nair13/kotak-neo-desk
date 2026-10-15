"""SFeed bridge: async Kotak websockets -> sync FastAPI app.

The legacy callback WS (client.subscribe/on_message) was REMOVED in SDK v2.2+
(installed v3 raises NotImplementedError). v3 uses async SFeedWebSocket
(market ticks) + OrderFeedWebSocket (orders/positions). This module runs both
on a dedicated daemon thread and forwards events to Hub (browser WS) and the
synthetic engine, using TransTrader-shaped dicts throughout.
"""

import asyncio
import logging
import threading
import time

log = logging.getLogger("sfeed")


# ---------- pure mappers (dispatch on type name so tests need no SDK) ----------
def map_feed_message(msg) -> dict | None:
    """SFeedScrip/SFeedIndex -> {'tk','ltp','ts'} tick. None if not mappable."""
    name = type(msg).__name__
    if name == "SFeedIndex":
        ltp = getattr(msg, "last_traded_price", None)
        nm = getattr(msg, "name", None)
        if nm is None or ltp is None:
            return None
        return {"tk": str(nm), "ltp": str(ltp), "ts": str(nm)}
    if name == "SFeedScrip":
        tok = getattr(msg, "instrument_token", None)
        ltp = getattr(msg, "last_traded_price", None)
        if tok is None or ltp is None:
            return None
        return {"tk": str(tok), "ltp": str(ltp),
                "ts": str(getattr(msg, "trading_symbol", "") or "")}
    return None


def map_order_data(d) -> dict:
    """OrderUpdate.data -> TransTrader-style order dict for engine + UI.
    t_feed_ms stamps server receipt of the exchange update (click-to-fill math)."""
    tx = getattr(d, "transaction_type", "?")
    return {
        "nOrdNo": str(getattr(d, "order_id", "")),
        "ordSt": str(getattr(d, "order_status", "unknown")),
        "trdSym": str(getattr(d, "trading_symbol", None) or getattr(d, "symbol", "")),
        "trnsTp": str(tx),
        "qty": getattr(d, "quantity", 0),
        "brdLtQty": str(getattr(d, "quantity", "0")),
        "prc": str(getattr(d, "average_price", "0")),
        "fldQty": getattr(d, "filled_quantity", 0),
        "rejRsn": str(getattr(d, "reject_reason", None) or getattr(d, "rejRsn", None) or "--"),
        "t_feed_ms": int(time.time() * 1000),
    }


def map_book_entry(d: dict) -> dict | None:
    """Raw order-book row (Kotak `order_report()` JSON) -> same shape as
    `map_order_data`, so `SyntheticEngine.reconcile` can re-drive it uniformly.
    Tolerates the wire abbreviations (nOrdNo/ordSt/avgPrc/...) and the
    long-form names. Returns None when there is no order id to key on."""
    if not isinstance(d, dict):
        return None

    def g(*keys, default=""):
        for k in keys:
            v = d.get(k)
            if v not in (None, ""):
                return v
        return default

    oid = g("nOrdNo", "order_id", "orderId", "order_no")
    if not oid:
        return None
    qty = g("qty", "quantity", default=0)
    return {
        "nOrdNo": str(oid),
        "ordSt": str(g("ordSt", "order_status", "status", default="unknown")),
        "trdSym": str(g("trdSym", "trading_symbol", "sym", "symbol")),
        "trnsTp": str(g("trnsTp", "transaction_type", "transactionType")),
        "qty": qty,
        "brdLtQty": str(qty),
        "prc": str(g("avgPrc", "average_price", "avgPrc", "prc", "price", default="0")),
        "fldQty": g("fldQty", "filled_quantity", default=0),
        "rejRsn": str(g("rejRsn", "reject_reason", default="--")),
        "t_feed_ms": int(time.time() * 1000),
    }


class SFeedBridge:
    def __init__(self, hub, engine, kotak, retry_sec: float = 5.0):
        self.hub = hub
        self.engine = engine
        self.kotak = kotak
        self.retry_sec = retry_sec
        self._lock = threading.Lock()
        self._wanted: set[tuple[str, str]] = set()  # (exchange, token) option legs
        self._subscribed: set[tuple[str, str]] = set()  # wanted legs already on the live ws
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws = None
        self._reconcile_lock: asyncio.Lock | None = None  # created on the bridge loop

    # ---------- lifecycle ----------
    def start(self):
        """(Re)start the feed thread. Call after every Kotak TOTP login."""
        self.stop()
        with self._lock:
            for o in self.engine.all():
                if o.get("status") in ("ACTIVE", "TRAILING", "PENDING_ENTRY") \
                        and o.get("instrument_token"):
                    self._wanted.add((o["exchange_segment"], str(o["instrument_token"])))
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="sfeed", daemon=True)
        self._thread.start()
        log.info("SFeed bridge started (wanted options: %d)", len(self._wanted))

    def stop(self):
        self._stop.set()
        with self._lock:
            self._subscribed.clear()  # next session resubscribes the full set
        th, self._thread = self._thread, None
        if th and th.is_alive():
            th.join(timeout=10)

    def ensure_option(self, exchange: str, token: str | None):
        """Subscribe one more option leg (called when a tracked order is placed,
        or when the UI watchlist changes). Already-streaming tokens are skipped —
        without this every watchlist edit re-sent the whole set per leg."""
        if not token:
            return
        key = (exchange, str(token))
        with self._lock:
            self._wanted.add(key)
            if key in self._subscribed:
                return
            loop, ws = self._loop, self._ws
        if loop and ws and not self._stop.is_set():
            fut = asyncio.run_coroutine_threadsafe(self._subscribe_wanted(ws), loop)
            fut.add_done_callback(lambda f: f.exception() and log.error("sub err: %s", f.exception()))

    # ---------- thread body ----------
    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._supervise())
        except Exception as e:
            log.error("bridge loop died: %s", e)
        finally:
            self._loop = None

    async def _supervise(self):
        """Own both feed supervisors on one loop (Plan 03).

        Market and order feeds have independent sessions + reconnect loops, so
        a market-feed drop can no longer tear down the order feed (and
        vice-versa). Each supervisor only exits when `self._stop` is set.
        """
        self._reconcile_lock = asyncio.Lock()
        tasks = {
            asyncio.create_task(self._supervise_market(), name="sfeed-market"),
            asyncio.create_task(self._supervise_orders(), name="sfeed-orders"),
        }
        try:
            while not self._stop.is_set():
                done, _ = await asyncio.wait(tasks, timeout=0.5,
                                             return_when=asyncio.FIRST_COMPLETED)
                for t in done:  # supervisors are loops; a return is unexpected
                    exc = t.exception()
                    if exc is not None and not self._stop.is_set():
                        log.error("feed supervisor %s died: %s", t.get_name(), exc)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _supervise_market(self):
        while not self._stop.is_set():
            try:
                await self._market_session()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error("market session ended: %s", e)
            if not self._stop.is_set():
                await asyncio.sleep(self.retry_sec)

    async def _supervise_orders(self):
        while not self._stop.is_set():
            try:
                await self._order_session()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error("order session ended: %s", e)
            if not self._stop.is_set():
                await asyncio.sleep(self.retry_sec)

    async def _market_session(self):
        from neo_api_client.websocket.feed import WsToken
        client = self.kotak.client
        if client is None:
            raise RuntimeError("no Kotak client (login first)")
        base = [WsToken("nse_cm", "Nifty 50"), WsToken("bse_cm", "SENSEX")]
        # SDK auto-reconnect is disabled (max_reconnect_attempts=0): the
        # supervisor owns reconnect, so an SDK reconnect window can't pre-empt
        # `async for` (which used to end the whole session, order feed included).
        async with client.create_websocket(max_reconnect_attempts=0) as ws:
            self._ws = ws
            with self._lock:
                self._subscribed.clear()  # a fresh socket forgot every sub
            await self._subscribe_wanted(ws, extra=base)
            try:
                async for msg in ws:
                    tick = map_feed_message(msg)
                    if tick is None:
                        continue
                    try:
                        self.hub.latest[tick["tk"]] = tick
                        self.engine.on_tick(tick)
                    except Exception as e:
                        log.error("tick handle err: %s", e)
                    self._push({"kind": "tick", "tick": tick})
            finally:
                self._ws = None

    async def _order_session(self):
        client = self.kotak.client
        if client is None:
            raise RuntimeError("no Kotak client (login first)")
        async with client.create_order_feed(max_reconnect_attempts=0) as feed:
            # Reconcile against the broker on every (re)connect: a fill/reject
            # that landed during a feed gap is re-driven, so the protective SL
            # is never silently missed.
            await self._reconcile()
            async for msg in feed:
                self._handle_orderfeed_message(msg)

    async def _reconcile(self):
        """Re-derive missed order state from the broker after a (re)connect.

        The broker fetch is blocking, so it runs in an executor (off the tick
        path, backlog M2); the engine mutation is applied back on this loop,
        serialized with tick/order handling. Guarded by a lock so a fast
        reconnect can't overlap a still-running reconcile (backlog M3).
        """
        if self._stop.is_set():
            return
        loop = asyncio.get_running_loop()
        if self._reconcile_lock is None:
            self._reconcile_lock = asyncio.Lock()
        async with self._reconcile_lock:
            try:
                book, positions = await loop.run_in_executor(None, self._fetch_book_positions)
            except Exception as e:
                log.error("reconcile fetch failed: %s", e)
                return
            try:
                summary = self.engine.reconcile(book, positions)
            except Exception as e:
                log.error("reconcile apply failed: %s", e)
                return
        log.info("reconcile: checked=%s driven=%s errored=%s",
                 summary.get("checked"), summary.get("driven"), summary.get("errored"))
        self._push({"kind": "reconcile", "summary": summary})

    def _fetch_book_positions(self):
        book_rows = []
        try:
            raw = self.kotak.order_book()
            if isinstance(raw, dict):
                data = raw.get("data")
                book_rows = data if isinstance(data, list) else []
            elif isinstance(raw, list):
                book_rows = raw
        except Exception as e:
            log.error("reconcile order_book failed: %s", e)
        book = [m for m in (map_book_entry(r) for r in book_rows) if m]
        try:
            positions = self.kotak.positions() or {}
        except Exception as e:
            log.error("reconcile positions failed: %s", e)
            positions = {}
        return book, positions

    async def _subscribe_wanted(self, ws, extra=None):
        from neo_api_client.websocket.feed import WsToken
        with self._lock:
            wanted = sorted(self._wanted)
            toks = [WsToken(ex, tk) for ex, tk in wanted] + list(extra or [])
        if not toks:
            return
        try:
            await ws.subscribe_scrips(toks)
            with self._lock:
                self._subscribed.update(wanted)
            log.info("subscribed %d scrips", len(toks))
        except Exception as e:
            log.error("subscribe failed: %s", e)

    def _handle_orderfeed_message(self, msg):
        name = type(msg).__name__
        data = getattr(msg, "data", None)
        if name == "OrderUpdate" and data is not None:
            od = map_order_data(data)
            if od.get("nOrdNo"):
                try:
                    self.engine.on_order_update(od)
                except Exception as e:
                    log.error("engine order err: %s", e)
                self._push({"kind": "order", "order": od})
        elif name == "PositionUpdate" and data is not None:
            d = data.__dict__ if hasattr(data, "__dict__") else {}
            self._push({"kind": "position", "position": {k: str(v) for k, v in d.items()}})

    def _push(self, msg: dict):
        loop = self.hub.main_loop
        if loop is None:
            return
        try:
            asyncio.run_coroutine_threadsafe(self.hub.broadcast(msg), loop)
        except Exception as e:
            log.error("push err: %s", e)
