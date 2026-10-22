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
from datetime import datetime, timedelta, timezone

from .broker_worker import CommandWorker
from .config import settings

log = logging.getLogger("sfeed")


def _now_ms() -> int:
    return int(time.time() * 1000)


# Broker wall-clock timestamps are IST (Asia/Kolkata, no DST). Naive strings
# must be interpreted in IST regardless of the host timezone, otherwise on a
# UTC host every row looks hours stale and versioning breaks.
_IST = timezone(timedelta(hours=5, minutes=30))

_NUMERIC_TIME_FMTS = (
    "%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S",
    "%Y%m%d%H%M%S",
)

# Exchange/broker last-update fields, in preference order. `updRecvTm` is the
# SDK's nanosecond receive time; `hsUpTm` is the exchange update wall clock.
_UPDATE_KEYS = (
    "updRecvTm", "updRecvTime", "updrecvtm", "hsUpTm", "hsUpTime", "hsUptm",
    "updTm", "ordUpdTm", "orderUpdateTime", "lastUpdateTime",
)
_CREATE_KEYS = ("ordDtTm", "ord_dt_tm", "order_time", "ordTm", "ordDateTime",
                "orderTime", "createdAt")


def _numeric_epoch_ms(f: float) -> int:
    """Epoch value -> ms, accepting seconds / ms / us / ns magnitudes."""
    a = abs(f)
    if a >= 1e17:      # nanoseconds
        return int(f / 1e6)
    if a >= 1e14:      # microseconds
        return int(f / 1e3)
    if a >= 1e11:      # milliseconds
        return int(f)
    return int(f * 1000)


def parse_broker_time_ms(value) -> int | None:
    """Broker timestamp (epoch number or IST wall clock) -> epoch ms.

    Numeric values may be ns/ms/s. Text values with an explicit offset (or a
    trailing Z) keep it; naive text is assumed IST. None when unparseable.
    """
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _numeric_epoch_ms(float(value))
    s = str(value).strip()
    try:
        return _numeric_epoch_ms(float(s))
    except (TypeError, ValueError):
        pass
    iso = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_IST)
        return int(dt.timestamp() * 1000)
    except ValueError:
        pass
    text = s.replace("T", " ")[:19]
    for fmt in _NUMERIC_TIME_FMTS:
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=_IST)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return None


def _src_get(src, key):
    if isinstance(src, dict):
        return src.get(key)
    return getattr(src, key, None)


def _last_update_ms(src) -> int | None:
    for k in _UPDATE_KEYS:
        ms = parse_broker_time_ms(_src_get(src, k))
        if ms is not None:
            return ms
    return None


def _created_ms(src) -> int | None:
    for k in _CREATE_KEYS:
        ms = parse_broker_time_ms(_src_get(src, k))
        if ms is not None:
            return ms
    return None


def _version_fields(src) -> dict:
    """Row versioning: prefer the update time, else the creation time.

    `update_ts_source` records which one was used so the engine never treats a
    creation timestamp as a modification version.
    """
    upd = _last_update_ms(src)
    if upd is not None:
        return {"update_ts_ms": upd, "update_ts_source": "update"}
    created = _created_ms(src)
    if created is not None:
        return {"update_ts_ms": created, "update_ts_source": "created"}
    return {"update_ts_ms": None, "update_ts_source": None}


class OrderFeedStale(RuntimeError):
    """The SDK reported a dead/disconnected order feed — force a new session."""


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
    t_feed_ms stamps server receipt of the exchange update (click-to-fill math).
    trigger_price carries an SL's current trigger so a manual broker-app modify
    can be synced into the tracker (Plan 06 WP5)."""
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
        "trigger_price": str(getattr(d, "trigger_price", None)
                             or getattr(d, "trgPrc", None)
                             or getattr(d, "triggerPrice", None) or ""),
        "rejRsn": str(getattr(d, "reject_reason", None) or getattr(d, "rejRsn", None) or "--"),
        "guiOrdId": str(getattr(d, "gui_order_id", None)
                        or getattr(d, "GuiOrdId", None)
                        or getattr(d, "tag", None) or ""),
        "t_feed_ms": int(time.time() * 1000),
        **_version_fields(d),
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
        "filled_qty": g("fldQty", "filled_quantity", default=0),
        # Trigger is needed to adopt a manual broker-app SL modify (Plan 06 WP5).
        "trigger_price": str(g("trgPrc", "trigger_price", "triggerPrice",
                               default="")),
        "rejRsn": str(g("rejRsn", "reject_reason", default="--")),
        # Client tag echoed by Kotak as GuiOrdId. The engine uses it to adopt a
        # market exit whose placement response timed out (Plan 06 M1) instead of
        # firing a second exit.
        "guiOrdId": str(g("GuiOrdId", "guiOrdId", "tag", default="")),
        # Raw broker creation timestamp (if any) lets the Orders panel show
        # order age. Never use it as a modification version (see below).
        "ord_time": str(g("ordDtTm", "ord_dt_tm", "order_time", "ordTm",
                          "ordDateTime", "orderTime", "createdAt", default="")),
        "t_feed_ms": int(time.time() * 1000),
        # Versioned by the exchange/broker update time (updRecvTm/hsUpTm),
        # falling back to the creation time only when no update time exists.
        **_version_fields(d),
    }


def parse_order_book(raw) -> tuple[list[dict], bool]:
    """Raw order_report() response -> (rows, ok).

    `ok=False` means the response was a failure/unusable shape, never an
    authoritative empty book. Shared by reconcile (via `fetch_book`) and the
    Orders panel so both agree on what "flat" means (Plan 06 WP5/WP7).
    """
    rows: list = []
    ok = False
    if isinstance(raw, dict):
        data = raw.get("data")
        error = any(
            isinstance(v, str) and k.lower() in
            ("error", "error message", "errmsg", "error_msg")
            for k, v in raw.items())
        if isinstance(data, list):
            rows = data
            ok = True
        elif data is None and "data" in raw and not error:
            # SDK success shape for "no orders today": authoritative empty book.
            ok = True
        stat = raw.get("stat")
        if stat is not None and str(stat).strip().lower() not in ("ok", "success"):
            # `stat: Not_Ok` with data:null/[] is a failed fetch, not an empty
            # book (Plan 06 WP6 honesty requirement).
            rows, ok = [], False
        code = raw.get("stCode")
        if code is not None and str(code) != "200":
            rows, ok = [], False
    elif isinstance(raw, list):
        rows = raw
        ok = True
    return rows, ok


class SFeedBridge:
    def __init__(self, hub, engine, kotak, retry_sec: float = 5.0, worker=None,
                 feed_stale_sec: float | None = None,
                 feed_watchdog_sec: float | None = None,
                 feed_idle_reconnect_sec: float | None = None,
                 reconcile_sec: float | None = None):
        self.hub = hub
        self.engine = engine
        self.kotak = kotak
        self.retry_sec = retry_sec
        self.feed_stale_sec = float(
            settings.feed_stale_sec if feed_stale_sec is None else feed_stale_sec)
        self.feed_watchdog_sec = max(0.01, float(
            settings.feed_watchdog_sec if feed_watchdog_sec is None else feed_watchdog_sec))
        self.feed_idle_reconnect_sec = float(
            settings.feed_idle_reconnect_sec
            if feed_idle_reconnect_sec is None else feed_idle_reconnect_sec)
        self.reconcile_sec = max(0.01, float(
            settings.reconcile_sec if reconcile_sec is None else reconcile_sec))
        self.worker = worker or CommandWorker(
            "engine-worker", max_queue=settings.broker_worker_queue,
            stall_ms=int(settings.broker_stall_log_sec * 1000),
            stop_timeout=settings.worker_stop_timeout_sec,
            call_timeout=settings.broker_call_timeout_sec)
        self._lock = threading.Lock()
        self._wanted: set[tuple[str, str]] = set()  # (exchange, token) option legs
        self._subscribed: set[tuple[str, str]] = set()  # wanted legs already on the live ws
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._scheduler_thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws = None
        self._started = False
        # Freshness / reconcile state (read by /api/health from FastAPI threads).
        self._last_order_msg_ms: int | None = None
        self._last_order_ok_ms: int | None = None
        self._last_order_applied_ms: int | None = None
        self._last_order_liveness_ms: int | None = None
        self._last_tick_ms: int | None = None
        self._last_reconcile_attempt_ms: int | None = None
        self._last_reconcile_ms: int | None = None
        self._last_reconcile_ok_ms: int | None = None
        self._last_reconcile_error: str | None = None
        self._last_reconcile_error_ms: int | None = None
        self._last_reconcile_summary: dict = {}
        self._order_connected = False
        self._watchdog_reconnects = 0
        self._quiet_warned = False
        self._reconcile_state_lock = threading.Lock()
        self._reconcile_queued = 0
        self._reconcile_pending_force = False
        self._last_positions_ok = False
        self._last_positions_ms: int | None = None
        self._last_positions_error: str | None = None
        self._last_book_ms: int | None = None
        self._last_book_ok = False
        self._poll_lock = threading.Lock()
        self._poll_timer: threading.Timer | None = None
        self._poll_due_ms: int | None = None
        self._poll_generation = 0

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
        self._started = True
        self._last_order_msg_ms = None
        self._last_order_ok_ms = None
        self._last_order_applied_ms = None
        self._last_order_liveness_ms = None
        self._last_tick_ms = None
        self._last_reconcile_attempt_ms = None
        self._last_reconcile_ms = None
        self._last_reconcile_ok_ms = None
        self._last_reconcile_error = None
        self._last_reconcile_error_ms = None
        self._last_reconcile_summary = {}
        self._last_positions_ok = False
        self._last_positions_ms = None
        self._last_positions_error = None
        self._last_book_ms = None
        self._last_book_ok = False
        self._order_connected = False
        self._watchdog_reconnects = 0
        self._quiet_warned = False
        self.worker.start()
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop, name="reconcile-sched", daemon=True)
        self._scheduler_thread.start()
        self._thread = threading.Thread(target=self._run, name="sfeed", daemon=True)
        self._thread.start()
        log.info("SFeed bridge started (wanted options: %d, stale=%ss, reconcile=%ss)",
                 len(self._wanted), self.feed_stale_sec, self.reconcile_sec)

    def stop(self):
        self._stop.set()
        with self._poll_lock:
            timer, self._poll_timer = self._poll_timer, None
            self._poll_due_ms = None
            self._poll_generation += 1
        if timer:
            timer.cancel()
        with self._lock:
            self._subscribed.clear()  # next session resubscribes the full set
        sched, self._scheduler_thread = self._scheduler_thread, None
        if sched and sched.is_alive() and sched is not threading.current_thread():
            sched.join(timeout=2)
        th, self._thread = self._thread, None
        if th and th.is_alive() and th is not threading.current_thread():
            th.join(timeout=10)
        self.worker.stop()
        self._started = False
        with self._reconcile_state_lock:
            self._reconcile_queued = 0
            self._reconcile_pending_force = False

    # ---------- periodic reconcile ----------
    def _scheduler_loop(self):
        while not self._stop.wait(self.reconcile_sec):
            try:
                self._schedule_reconcile("periodic")
            except Exception as e:  # noqa: BLE001 - scheduler must survive one bad tick
                log.error("periodic reconcile schedule failed: %s", e)

    def _schedule_reconcile(self, source: str, force: bool = False) -> bool:
        """Queue a reconcile without blocking; at most one pending/running.

        A `force` request that arrives while one is already queued sets a flag
        instead of enqueueing a second command, so connect storms cannot pile up.
        """
        if self._stop.is_set():
            return False
        with self._reconcile_state_lock:
            if self._reconcile_queued:
                if force:
                    self._reconcile_pending_force = True
                return False
            self._reconcile_queued = 1
        ok = self.worker.submit(
            self._run_reconcile, source, critical=True,
            label=f"reconcile:{source}", allow_inline=False)
        if not ok:
            with self._reconcile_state_lock:
                self._reconcile_queued = 0
                self._reconcile_pending_force = False
            log.warning("reconcile (%s) not queued: worker unavailable", source)
        return ok

    def request_reconcile(self, source: str = "orders-api") -> bool:
        """Public hook for route-side actions (cancel/modify) to schedule one
        authoritative reconcile on the worker (never a direct broker call)."""
        return self._schedule_reconcile(source, force=True)

    def request_exit_poll(self, delay_sec: float = 0.0) -> bool:
        """Queue one exit follow-up reconcile, optionally after a delay.

        At most one timer is pending; a sooner deadline replaces a later one
        and an immediate request cancels the timer. This keeps the worker free
        while still following up inside a broker-call timeout window.
        """
        if self._stop.is_set():
            return False
        try:
            delay = max(0.0, float(delay_sec))
        except (TypeError, ValueError):
            delay = 0.0
        if delay <= 0:
            with self._poll_lock:
                timer, self._poll_timer = self._poll_timer, None
                self._poll_due_ms = None
                self._poll_generation += 1
            if timer:
                timer.cancel()
            return self._schedule_reconcile("exit-poll", force=True)
        due = _now_ms() + int(delay * 1000)
        with self._poll_lock:
            if self._stop.is_set():
                return False
            if self._poll_timer and self._poll_due_ms is not None \
                    and self._poll_due_ms <= due:
                return False
            old, self._poll_timer = self._poll_timer, None
            if old:
                old.cancel()
            self._poll_generation += 1
            generation = self._poll_generation
            self._poll_due_ms = due
            timer = threading.Timer(delay, self._fire_exit_poll, args=(generation,))
            timer.daemon = True
            self._poll_timer = timer
            timer.start()
        return True

    def _fire_exit_poll(self, generation: int):
        with self._poll_lock:
            if (generation != self._poll_generation
                    or self._poll_timer is None or self._stop.is_set()):
                return
            self._poll_timer = None
            self._poll_due_ms = None
        self._schedule_reconcile("exit-poll", force=True)

    def _run_reconcile(self, source: str):
        """Runs on the worker: broker fetch + engine mutation, serialized."""
        try:
            if self._stop.is_set():
                return
            self._last_reconcile_attempt_ms = _now_ms()
            book, positions, ok = self._fetch_book_positions()
            if not ok:
                # A failed fetch is NOT an empty book. Authoritative reconcile
                # here would mark every live SL as gone and drive duplicates.
                self._last_reconcile_error = "order_book_unavailable"
                self._last_reconcile_error_ms = _now_ms()
                log.warning("reconcile (%s) skipped: order book unavailable", source)
                note = getattr(self.engine, "note_book_unavailable", None)
                if note:
                    try:
                        note()
                    except Exception as e:  # noqa: BLE001
                        log.error("book-unavailable accounting failed: %s", e)
                return
            try:
                try:
                    summary = self.engine.reconcile(
                        book, positions, positions_ok=self._last_positions_ok)
                except TypeError as e:
                    if "positions_ok" not in str(e):
                        raise
                    # Older/custom engine doubles only accept (book, positions).
                    summary = self.engine.reconcile(book, positions)
            except Exception as e:  # noqa: BLE001
                self._last_reconcile_error = f"apply:{type(e).__name__}"
                self._last_reconcile_error_ms = _now_ms()
                log.error("reconcile apply failed: %s", e)
                return
            done = _now_ms()
            self._last_reconcile_ms = done
            self._last_reconcile_ok_ms = done
            self._last_reconcile_error = None
            self._last_reconcile_summary = dict(summary)
            log.info("reconcile (%s): checked=%s driven=%s repaired=%s errored=%s naked=%s",
                     source, summary.get("checked"), summary.get("driven"),
                     summary.get("repaired"), summary.get("errored"),
                     summary.get("naked"))
            self._push({"kind": "reconcile", "summary": summary})
        finally:
            rerun = False
            with self._reconcile_state_lock:
                if self._reconcile_pending_force and not self._stop.is_set():
                    # Keep the queued flag set while resubmitting so another
                    # thread cannot slip a second reconcile into the same slot.
                    self._reconcile_pending_force = False
                    self._reconcile_queued = 1
                    rerun = True
                else:
                    self._reconcile_queued = 0
            if rerun:
                ok = self.worker.submit(
                    self._run_reconcile, "forced", critical=True,
                    label="reconcile:forced", allow_inline=False)
                if not ok:
                    with self._reconcile_state_lock:
                        self._reconcile_queued = 0
                        self._reconcile_pending_force = False
                    log.warning("forced reconcile not queued: worker unavailable")

    # ---------- health ----------
    def health(self) -> dict:
        now = _now_ms()

        def age(ts):
            return None if ts is None else max(0, now - int(ts))

        order_age = age(self._last_order_msg_ms)
        threshold = int(self.feed_stale_sec * 1000)
        quiet = bool(self._started and self._order_connected
                     and (order_age is None or order_age >= threshold))
        stale = bool(self._started and not self._order_connected)
        return {
            "started": self._started,
            "order_connected": self._order_connected,
            "stale": stale,
            "quiet": quiet,
            "order_age_ms": order_age,
            "tick_age_ms": age(self._last_tick_ms),
            "last_order_msg_ms": self._last_order_msg_ms,
            "last_order_ok_ms": self._last_order_ok_ms,
            "last_order_applied_ms": self._last_order_applied_ms,
            "last_order_liveness_ms": self._last_order_liveness_ms,
            "last_tick_ms": self._last_tick_ms,
            "last_reconcile_attempt_ms": self._last_reconcile_attempt_ms,
            "last_reconcile_ms": self._last_reconcile_ms,
            "last_reconcile_ok_ms": self._last_reconcile_ok_ms,
            "last_reconcile_age_ms": age(self._last_reconcile_ms),
            "last_reconcile_error": self._last_reconcile_error,
            "last_reconcile_error_ms": self._last_reconcile_error_ms,
            "last_reconcile": dict(self._last_reconcile_summary),
            "last_positions_ms": self._last_positions_ms,
            "last_positions_age_ms": age(self._last_positions_ms),
            "last_positions_ok": self._last_positions_ok,
            "last_positions_error": self._last_positions_error,
            "last_book_ms": self._last_book_ms,
            "last_book_age_ms": age(self._last_book_ms),
            "last_book_ok": self._last_book_ok,
            "watchdog_reconnects": self._watchdog_reconnects,
            "worker": self.worker.status(),
        }

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
                    self._last_tick_ms = _now_ms()
                    self.hub.latest[tick["tk"]] = tick
                    if not self.worker.submit(self._apply_tick, tick,
                                              label="on_tick",
                                              allow_inline=False):
                        log.debug("tick dropped: worker unavailable")
                    self._push({"kind": "tick", "tick": tick})
            finally:
                self._ws = None

    async def _order_session(self):
        client = self.kotak.client
        if client is None:
            raise RuntimeError("no Kotak client (login first)")
        async with client.create_order_feed(max_reconnect_attempts=0) as feed:
            # Reader only reads. The engine applies each event on the worker via
            # _handle_orderfeed_message, so a broker call can never block this
            # loop (the 2026-10-21 61 s cancel starved it and killed the feed).
            self._order_connected = True
            self._last_order_msg_ms = _now_ms()
            self._last_order_ok_ms = self._last_order_msg_ms
            self._last_order_liveness_ms = self._last_order_msg_ms
            self._quiet_warned = False
            log.info("order feed connected — scheduling reconcile")
            self._schedule_reconcile("connect", force=True)
            reader = asyncio.create_task(self._read_order_feed(feed),
                                         name="sfeed-order-reader")
            try:
                while True:
                    done, _ = await asyncio.wait(
                        {reader}, timeout=self.feed_watchdog_sec,
                        return_when=asyncio.FIRST_COMPLETED)
                    if reader in done:
                        exc = reader.exception()
                        if exc is not None:
                            raise exc
                        return
                    if self._watchdog_tick(feed):
                        raise OrderFeedStale("order feed disconnected")
            finally:
                self._order_connected = False
                self._quiet_warned = False
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)

    def _watchdog_tick(self, feed) -> bool:
        """True only when the feed is genuinely dead (force a reconnect).

        Message silence alone is not death: Kotak can legally stay quiet for
        minutes. Prefer the SDK's `is_connected`; fall back to a conservative
        idle timeout when the SDK exposes no signal.
        """
        now = _now_ms()
        connected = getattr(feed, "is_connected", None)
        if not isinstance(connected, bool):
            connected = None
        last_msg = self._last_order_msg_ms
        if connected is False:
            self._watchdog_reconnects += 1
            log.error("order feed reports disconnected — forcing reconnect")
            return True
        if connected is True:
            self._last_order_liveness_ms = now
            self._last_order_ok_ms = now
            msg_age = now - (last_msg if last_msg is not None else now)
            if msg_age >= self.feed_stale_sec * 1000:
                if not self._quiet_warned:
                    log.warning("order feed quiet for %dms but still connected "
                                "(threshold %dms); not reconnecting",
                                msg_age, int(self.feed_stale_sec * 1000))
                    self._quiet_warned = True
            else:
                self._quiet_warned = False
            return False
        idle = now - (last_msg if last_msg is not None else now)
        if idle >= self.feed_idle_reconnect_sec * 1000:
            self._watchdog_reconnects += 1
            log.error("order feed idle for %dms with no connection signal "
                      "(threshold %dms) — forcing reconnect",
                      idle, int(self.feed_idle_reconnect_sec * 1000))
            return True
        if idle >= self.feed_stale_sec * 1000 and not self._quiet_warned:
            log.warning("order feed quiet for %dms; SDK exposes no liveness "
                        "signal, waiting up to %ss before reconnecting",
                        idle, self.feed_idle_reconnect_sec)
            self._quiet_warned = True
        return False

    async def _read_order_feed(self, feed):
        async for msg in feed:
            self._last_order_msg_ms = _now_ms()
            self._quiet_warned = False
            self._handle_orderfeed_message(msg)

    def fetch_book(self):
        """Return (mapped_book, ok) for reconcile + cancel confirmation.

        `ok=False` means the order book could not be read authoritatively
        (exception/timeout/unexpected shape). The caller must not treat an
        empty list here as "no live orders". Runs on the command worker in
        production, so cancel confirmation never blocks the feed loop.
        """
        book_rows = []
        book_ok = False
        try:
            raw = self.kotak.order_book()
            book_rows, book_ok = parse_order_book(raw)
        except Exception as e:
            log.error("reconcile order_book failed: %s", e)
        self._last_book_ms = _now_ms()
        self._last_book_ok = book_ok
        book = [m for m in (map_book_entry(r) for r in book_rows) if m]
        return book, book_ok

    def _fetch_book_positions(self):
        """Return (book, positions, book_ok).

        `_last_positions_ok` distinguishes a successful empty positions
        snapshot (flat account) from a failed fetch. The engine must not
        auto-replace an SL without a fresh authoritative positions snapshot
        (Plan 06 H3).
        """
        book, book_ok = self.fetch_book()
        try:
            try:
                positions = self.kotak.positions(strict=True) or {}
            except TypeError:
                # Older/custom broker doubles only accept positions().
                positions = self.kotak.positions() or {}
            self._last_positions_ok = True
            self._last_positions_ms = _now_ms()
            self._last_positions_error = None
        except Exception as e:
            log.error("reconcile positions failed: %s", e)
            positions = {}
            self._last_positions_ok = False
            self._last_positions_ms = _now_ms()
            # /api/health is only readable by an authenticated user, but keep the
            # exposed text to the exception type (no SDK/session details).
            self._last_positions_error = type(e).__name__
        return book, positions, book_ok

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

    def _apply_tick(self, tick: dict):
        try:
            self.engine.on_tick(tick)
        except Exception as e:
            log.error("tick handle err: %s", e)

    def _apply_order_update(self, od: dict):
        try:
            self.engine.on_order_update(od)
        except Exception as e:
            log.error("engine order err: %s", e)
        finally:
            now = _now_ms()
            self._last_order_applied_ms = now
            self._last_order_ok_ms = now

    def _handle_orderfeed_message(self, msg):
        name = type(msg).__name__
        data = getattr(msg, "data", None)
        if name == "OrderUpdate" and data is not None:
            od = map_order_data(data)
            if od.get("nOrdNo"):
                if not self.worker.submit(self._apply_order_update, od,
                                          critical=True,
                                          label="on_order_update",
                                          allow_inline=False):
                    log.critical("ORDER UPDATE DROPPED %s: worker unavailable",
                                 od.get("nOrdNo"))
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
