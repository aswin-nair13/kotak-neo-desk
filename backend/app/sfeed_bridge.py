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
        while not self._stop.is_set():
            try:
                await self._session()
            except Exception as e:
                log.error("feed session ended: %s", e)
            if not self._stop.is_set():
                await asyncio.sleep(self.retry_sec)

    async def _session(self):
        from neo_api_client.websocket.feed import WsToken
        client = self.kotak.client
        if client is None:
            raise RuntimeError("no Kotak client (login first)")
        base = [WsToken("nse_cm", "Nifty 50"), WsToken("bse_cm", "SENSEX")]
        async with client.create_websocket() as ws:
            self._ws = ws
            await self._subscribe_wanted(ws, extra=base)
            order_task = asyncio.create_task(self._consume_orders(client))
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
                order_task.cancel()

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

    async def _consume_orders(self, client):
        try:
            async with client.create_order_feed() as feed:
                async for msg in feed:
                    self._handle_orderfeed_message(msg)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("order feed ended: %s", e)

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
