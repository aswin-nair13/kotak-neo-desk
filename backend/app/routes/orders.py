"""Order endpoints: single leg, straddle (CE+PE), cancel/modify, synthetic list, orderbook."""

import copy
import logging
import threading
import time
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from ..auth import require_user
from ..broker_worker import WorkerQueueFull
from ..config import settings
from ..kotak_client import KotakService, sanitize_error_text
from ..schemas import ExitBody, ForceExitBody, SingleOrderBody, StraddleBody
from ..sfeed_bridge import map_book_entry, parse_order_book
from ..synthetic_engine import SyntheticEngine
from ..ws_manager import Hub

log = logging.getLogger("orders")

router = APIRouter()

kotak: KotakService | None = None
engine: SyntheticEngine | None = None
hub: Hub | None = None
bridge = None  # SFeedBridge, wired in main.py
worker = None  # CommandWorker, wired in main.py (None in unit tests -> direct)
# Read-only broker calls (order book / positions UI) must never enter the
# single-writer mutation queue: a slow read would delay a queued cancel/exit.
read_worker = None  # dedicated CommandWorker, wired in main.py


def _run_on_worker(fn, *args, label: str | None = None,
                   timeout: float | None = None, **kwargs):
    """Run a route-side mutation on the single writer and map backlog errors.

    Place (broker call + engine.add) and exit (cancel + mark + market order)
    must be one worker command, otherwise an order update can be processed
    between the broker call and the engine registration. `timeout` covers the
    whole bounded command so a client timeout cannot race an internal retry.
    """
    if worker is None:
        return fn(*args, **kwargs)
    try:
        return worker.submit_wait(
            fn, *args, label=label or getattr(fn, "__name__", "command"),
            timeout=timeout, **kwargs)
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=f"Broker command timed out: {e}")
    except WorkerQueueFull as e:
        raise HTTPException(status_code=503, detail=f"Broker queue full: {e}")


def _run_read(fn, *args, label: str | None = None,
              timeout: float | None = None, **kwargs):
    """Run a read-only broker call on the dedicated read worker."""
    if read_worker is None:
        return fn(*args, **kwargs)
    wait = (max(0.1, float(settings.orderbook_timeout_sec))
            if timeout is None else timeout)
    try:
        return read_worker.submit_wait(
            fn, *args, label=label or getattr(fn, "__name__", "read"),
            timeout=wait, **kwargs)
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=f"Broker read timed out: {e}")
    except WorkerQueueFull as e:
        raise HTTPException(status_code=503, detail=f"Broker read queue full: {e}")


def _broker_call(fn, *args, label: str | None = None, **kwargs):
    """Run a broker call on the single command worker when it is wired."""
    return _run_on_worker(fn, *args, label=label, **kwargs)

# M5 - server-side idempotency. The UI sends a per-click `client_order_tag`;
# a duplicate (double-click / retry) with the same tag within the TTL returns
# the original response instead of placing a second order. The lock makes the
# check-and-place atomic so two concurrent identical requests can't both reach
# the broker.
IDEM_TTL_SEC = 60.0
_idem_lock = threading.Lock()
_idem_cache: dict[str, tuple[float, dict]] = {}


def _idem_replay(tag: str | None):
    """Cached response for a tag placed within the TTL, else None. Caller holds _idem_lock."""
    if not tag:
        return None
    hit = _idem_cache.get(tag)
    if hit is None:
        return None
    ts, resp = hit
    if time.time() - ts >= IDEM_TTL_SEC:
        _idem_cache.pop(tag, None)
        return None
    return resp


def _idem_store(tag: str | None, resp: dict):
    """Record a placed order's response under its tag, pruning expired entries. Caller holds _idem_lock."""
    if not tag:
        return
    now = time.time()
    for k in [k for k, (ts, _) in _idem_cache.items() if now - ts >= IDEM_TTL_SEC]:
        _idem_cache.pop(k, None)
    _idem_cache[tag] = (now, resp)


# Manual Exit-button flatten: there is no tracker to attach the broker order to,
# so an in-memory symbol-level record carries the deterministic broker tag across
# retries/double-clicks. Pending records live longer than completed ones because
# an ambiguous exit may need a later order-report adoption; completed records are
# only cached long enough to swallow a double-click.
MANUAL_EXIT_PENDING_TTL_SEC = 120.0
MANUAL_EXIT_DONE_TTL_SEC = 15.0
_manual_exit_lock = threading.Lock()
_manual_exit_records: dict[str, dict] = {}


def _manual_exit_ttl(rec: dict) -> float:
    return (MANUAL_EXIT_PENDING_TTL_SEC
            if rec.get("state") in ("pending", "ambiguous")
            else MANUAL_EXIT_DONE_TTL_SEC)


def _manual_exit_prune_locked(now: float):
    for symbol, rec in list(_manual_exit_records.items()):
        if now - float(rec.get("updated_at") or now) >= _manual_exit_ttl(rec):
            _manual_exit_records.pop(symbol, None)


def _manual_exit_get(symbol: str) -> dict | None:
    now = time.time()
    with _manual_exit_lock:
        _manual_exit_prune_locked(now)
        rec = _manual_exit_records.get(symbol)
        return copy.deepcopy(rec) if rec is not None else None


def _manual_exit_claim(symbol: str, tag: str, client_tag: str | None):
    now = time.time()
    with _manual_exit_lock:
        _manual_exit_prune_locked(now)
        existing = _manual_exit_records.get(symbol)
        if existing is not None:
            return copy.deepcopy(existing), False
        rec = {"symbol": symbol, "tag": tag, "client_tag": client_tag,
               "state": "pending", "response": None, "base": None,
               "created_at": now, "updated_at": now}
        _manual_exit_records[symbol] = rec
        return copy.deepcopy(rec), True


def _manual_exit_update(symbol: str, tag: str, **fields):
    with _manual_exit_lock:
        rec = _manual_exit_records.get(symbol)
        if rec is None or rec.get("tag") != tag:
            return
        rec.update(fields)
        rec["updated_at"] = time.time()


def _manual_exit_drop(symbol: str, tag: str):
    with _manual_exit_lock:
        rec = _manual_exit_records.get(symbol)
        if rec is not None and rec.get("tag") == tag:
            _manual_exit_records.pop(symbol, None)


def _call_place_with_tag(fn, *args, tag: str, **kwargs):
    """Pass the broker tag, tolerating legacy test doubles without it."""
    try:
        return _broker_call(fn, *args, tag=tag, **kwargs)
    except TypeError as e:
        if "tag" not in str(e):
            raise
        return _broker_call(fn, *args, **kwargs)



def _lot(instrument: str) -> int:
    return settings.sensex_lot if instrument == "SENSEX" else settings.nifty_lot


def _positive_int_qty(raw) -> str | None:
    """Broker v3 order qty must be a positive integer string (`"20"`, not
    `"20.0"`/`"20.5"`). Returns None when invalid."""
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if value <= 0 or not value.is_integer():
        return None
    return str(int(value))


def _require_master():
    """Block order ENTRY when the scrip master is unavailable (Plan 02 C5).

    Every order's instrument_token comes from the master; if it failed to load
    (TLS/HTTP/parse, possibly MITM) the symbol would resolve to no/forged token.
    Risk-reducing actions (cancel / modify-SL / exit-position) are NOT gated."""
    assert kotak is not None
    if not kotak.master_ok:
        raise HTTPException(
            status_code=503,
            detail="Scrip master unavailable - trading disabled. Refresh the Kotak "
                   "session so the scrip-master download succeeds before placing orders.")


def _leg(instrument, expiry, option_type, strike, lots, mode, ref_ltp, entry_offset, side: str = "B",
         offset: float = 1.0, limit_price: float | None = None):
    """Derive the entry limit price. Entry modes, symmetric by side
    (is_long True for BUY, False for SELL/short), with ref = live LTP:
      MKT : marketable limit - BUY ref+5, SELL ref-5 (true MKT is rejected
            for some F&O legs, so we fake it with a through-the-market limit)
      ML  : limit at ref (both sides)
      L-1 : passive limit with configurable offset -
            BUY ref-offset (bid below), SELL ref+offset (offer above)
      L   : explicit limit_price from the UI Limit column (both sides);
            400 if missing/invalid. Legacy entry_offset fallback kept so old
            clients sending L + entry_offset still work (ref - entry_offset).
    """
    if not expiry:
        raise HTTPException(status_code=400, detail="Pick an expiry from GET /api/market/expiries")
    is_long = str(side).upper() != "S"
    exchange = "bse_fo" if instrument == "SENSEX" else "nse_fo"
    symbol = f"{instrument}{expiry}{strike}{option_type}"
    qty = str(_lot(instrument) * lots)
    if mode == "L":
        if limit_price is not None:
            try:
                entry = float(limit_price)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail=f"Limit price invalid for {symbol} (L mode needs a price)")
            if entry <= 0:
                raise HTTPException(status_code=400, detail=f"Limit price required for {symbol} (L mode, price missing)")
        elif float(entry_offset or 0):
            entry = float(ref_ltp) - float(entry_offset)
        else:
            raise HTTPException(status_code=400, detail=f"Limit price required for {symbol} (L mode, price missing)")
    elif mode == "ML":
        entry = float(ref_ltp)
    elif mode == "L-1":
        d = float(offset if offset is not None else 1.0)
        if d < 0:
            raise HTTPException(status_code=400, detail=f"Offset must be >= 0 for {symbol}")
        entry = float(ref_ltp) - d if is_long else float(ref_ltp) + d
    elif is_long:
        entry = float(ref_ltp) + 5
    else:  # SELL MKT: accept below last price so the short fills immediately
        entry = float(ref_ltp) - 5
    # M6: never emit a non-positive price (SELL MKT on a cheap premium computes
    # ref-5 <= 0; the exchange rejects it). Floor every mode at one tick.
    entry = round(max(0.05, float(entry)), 2)
    return exchange, symbol, qty, entry


def _ms() -> int:
    return int(time.time() * 1000)


def _stacked_leg(symbol: str):
    """An existing open/working tracker on this exact contract (WP11).

    An ERRORED tracker with no live/ambiguous order state (for example an entry
    that was rejected before any fill) must not block a fresh entry forever.
    Anything that may still have an exchange order stays blocking.
    """
    if engine is None:
        return None
    try:
        legs = engine.legs_for_symbol(symbol)
    except Exception:  # noqa: BLE001 - never block on a read failure
        return None
    for leg in legs:
        if leg.get("status") != "ERRORED":
            return leg
        if any(leg.get(k) for k in (
                "naked_position", "exit_pending", "exit_ambiguous",
                "sl_ambiguous", "sl_order_id", "exit_order_id")):
            return leg
    return None


def _passive_warning(symbol: str, side: str, entry, ref_ltp) -> str | None:
    """Warn when a passive limit is unusually far from LTP (WP11).

    BUY below LTP and SELL above LTP are passive; a gap beyond
    `MAX_PASSIVE_OFFSET` is legal but unlikely to fill, so it is surfaced on
    the placement response (never rejected).
    """
    try:
        ref = float(ref_ltp)
        px = float(entry)
    except (TypeError, ValueError):
        return None
    gap = (px - ref) if str(side).upper() == "S" else (ref - px)
    limit = float(settings.max_passive_offset)
    if gap <= limit:
        return None
    return (f"{symbol}: limit {px:.2f} is {gap:.2f} away from LTP {ref:.2f} "
            f"(max passive offset {limit:g}); it may not fill")


def _attach_warnings(result, symbol: str, side: str, entry, ref_ltp):
    warning = _passive_warning(symbol, side, entry, ref_ltp)
    if not warning or not isinstance(result, dict):
        return result
    log.warning("PASSIVE OFFSET %s", warning)
    out = dict(result)
    out["warnings"] = list(out.get("warnings") or []) + [warning]
    return out


def _safe_text(value, limit: int = 200) -> str:
    """One-line, credential-redacted broker text for API/log surfaces."""
    return sanitize_error_text(value, limit)


def _place_wait_sec() -> float:
    """HTTP wait covering the whole bounded place/adopt/retry flow.

    Worst case per ambiguous attempt is: one SDK place call (bounded by
    `BROKER_CALL_TIMEOUT_SEC`) + one order-report read (same SDK transport
    timeout) + the full `ORDER_PLACE_CONFIRM_SEC` window. A retry adds a second
    such attempt. The margin absorbs scheduling/queue latency. Waiting only the
    default broker timeout would let the caller see 504 while the worker is still
    placing/checking attempt 2, and a manual retry could then duplicate it. The
    request therefore waits for the bounded worst case.
    """
    call = max(0.0, float(settings.broker_call_timeout_sec))
    confirm = max(0.0, float(settings.order_place_confirm_sec))
    attempts = max(1, int(settings.order_place_max_attempts))
    per_attempt = 2.0 * call + confirm
    return max(call, attempts * per_attempt + 5.0)



def _broker_ok(res) -> bool:
    """Broker success check: both `stat` and `stCode` must agree.

    `stCode=200` plus `stat=Not_Ok` is a failure, not a success.
    """
    if not isinstance(res, dict):
        return True
    stat = res.get("stat")
    if stat is not None and str(stat).strip().lower() not in ("ok", "success"):
        return False
    code = res.get("stCode")
    if code is not None:
        return str(code) == "200"
    return True


def _safe_result(res) -> dict:
    """Small sanitized broker response body (never a raw session-bearing dict)."""
    if not isinstance(res, dict):
        return {"result": _safe_text(res)}
    out = {}
    for k in ("stCode", "stat", "nOrdNo", "errMsg", "message", "Error"):
        if k in res:
            v = res.get(k)
            out[k] = _safe_text(v) if isinstance(v, str) else v
    return out or {"result": _safe_text(res)}


def _request_reconcile(source: str):
    """Schedule one authoritative reconcile after a successful route action."""
    if bridge is None:
        return
    fn = getattr(bridge, "request_reconcile", None)
    try:
        if fn is not None:
            fn(source)
        else:
            bridge.request_exit_poll(0)
    except Exception as e:  # noqa: BLE001 - reconcile scheduling is best effort
        log.error("schedule reconcile (%s) failed: %s", source, _safe_text(e))


_ORDER_TIME_FORMATS = (
    "%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S",
)


def _parse_order_time(value) -> int | None:
    """Best-effort broker order timestamp -> epoch ms (None if unparseable)."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        return int(f if f > 1e11 else f * 1000)
    s = str(value).strip()
    try:
        f = float(s)
        return int(f if f > 1e11 else f * 1000)
    except (TypeError, ValueError):
        pass
    text = s.replace("T", " ").replace("Z", "")[:19]
    for fmt in _ORDER_TIME_FORMATS:
        try:
            return int(datetime.strptime(text, fmt).timestamp() * 1000)  # noqa: DTZ007
        except ValueError:
            continue
    return None


def _book_row(raw) -> dict | None:
    row = map_book_entry(raw)
    if row is None:
        return None
    placed = _parse_order_time(row.get("ord_time"))
    row["age_ms"] = max(0, _ms() - placed) if placed else None
    # Explicit aliases so the panel does not depend on Kotak's wire names.
    row["status"] = row.get("ordSt")
    row["price"] = row.get("prc")
    row["filled_qty"] = row.get("fldQty")
    return row


def _orderbook_impl():
    """Worker-side order-book read: normalized rows + fetch freshness."""
    assert kotak is not None
    fetched = _ms()
    try:
        raw = kotak.order_book()
    except Exception as e:  # noqa: BLE001 - surfaced as a structured failure
        log.error("order book read failed: %s", _safe_text(e))
        return {"ok": False, "orders": [], "count": 0,
                "error": _safe_text(e), "fetched_at_ms": fetched,
                "age_ms": 0, "stale": True, "source": "broker"}
    rows, ok = parse_order_book(raw)
    if not ok:
        log.error("order book returned an unusable response: %s", _safe_text(raw))
        return {"ok": False, "orders": [], "count": 0,
                "error": "broker returned an unusable order-book response",
                "fetched_at_ms": fetched, "age_ms": _ms() - fetched,
                "stale": True, "source": "broker"}
    orders = [r for r in (_book_row(x) for x in rows) if r]
    return {"ok": True, "orders": orders, "count": len(orders),
            "fetched_at_ms": fetched, "age_ms": _ms() - fetched,
            "stale": False, "source": "broker"}


def _entry_ref() -> str:
    return uuid.uuid4().hex[:10]


def _book_rows_for_tag():
    """Fresh broker order report rows for timeout adoption: (rows, authoritative)."""
    assert kotak is not None
    fn = getattr(kotak, "order_report", None)
    if not callable(fn):
        fn = getattr(kotak, "order_book", None)
    if not callable(fn):
        return [], False
    try:
        raw = fn()
    except Exception as e:  # noqa: BLE001 - uncertain placement is handled by caller
        log.error("entry tag lookup failed: %s", _safe_text(e))
        return [], False
    rows, ok = parse_order_book(raw)
    if not ok:
        return [], False
    return [m for m in (map_book_entry(r) for r in rows) if m], True


def _order_tag(row: dict) -> str:
    for key in ("guiOrdId", "GuiOrdId", "tag"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _same_side(row_side, side: str) -> bool:
    got = str(row_side or "").upper()
    if not got:
        return False
    wanted = str(side or "").upper()
    aliases = {"B": {"B", "BUY"}, "S": {"S", "SELL"}}
    return got in aliases.get(wanted, {wanted})


def _fallback_match(row: dict, symbol: str, qty: str, side: str) -> bool:
    sym = str(row.get("trdSym") or "").strip()
    if symbol and sym and sym.upper() != str(symbol).strip().upper():
        return False
    row_qty = str(row.get("qty") or "").strip()
    if qty and row_qty:
        try:
            if float(row_qty) != float(qty):
                return False
        except (TypeError, ValueError):
            return False
    return _same_side(row.get("trnsTp"), side)


def _lookup_entry_once(tag: str, symbol: str, qty: str, side: str):
    """One order-report lookup.

    A tag match is ours and is NOT gated on symbol/qty/side. Symbol/qty/side is
    only a fallback for rows whose broker tag is missing, and only a unique
    match may be adopted. Returns (row|None, authoritative, reason, matches).
    """
    rows, ok = _book_rows_for_tag()
    if not ok:
        return None, False, "book_unavailable", []
    tagged = [r for r in rows if _order_tag(r)]
    exact = [r for r in tagged if _order_tag(r) == str(tag)]
    if len(exact) == 1:
        return exact[0], True, "tag", exact
    if len(exact) > 1:
        return None, True, "ambiguous_tag", exact
    untagged = [r for r in rows if not _order_tag(r)]
    fallback = [r for r in untagged if _fallback_match(r, symbol, qty, side)]
    if len(fallback) == 1:
        return fallback[0], True, "fallback", fallback
    if len(fallback) > 1:
        return None, True, "ambiguous_fallback", fallback
    return None, True, "absent", []


def _find_entry_confirmed(tag: str, symbol: str, qty: str, side: str):
    """Look up a tag over a short bounded window (book lag after accept).

    Runs only on the ambiguous placement path. The window is deliberately
    short/bounded; it does not poll forever and does not run on every order.
    """
    budget = max(0.0, float(settings.order_place_confirm_sec))
    deadline = time.monotonic() + budget
    while True:
        row, ok, reason, matches = _lookup_entry_once(tag, symbol, qty, side)
        if row is not None or not ok or reason.startswith("ambiguous"):
            return row, ok, reason, matches
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return row, ok, reason, matches
        time.sleep(min(0.2, remaining))


def _manual_exit_lookup_once(tag: str):
    """Exact manual-exit tag lookup: (row|None, authoritative, reason, matches)."""
    rows, ok = _book_rows_for_tag()
    if not ok:
        return None, False, "book_unavailable", []
    exact = [r for r in rows if _order_tag(r) == str(tag)]
    if len(exact) == 1:
        return exact[0], True, "tag", exact
    if len(exact) > 1:
        return None, True, "ambiguous_tag", exact
    return None, True, "absent", []


def _find_manual_exit_confirmed(tag: str):
    """Bounded tag lookup for a manual exit whose placement response was lost."""
    budget = max(0.0, float(settings.order_place_confirm_sec))
    deadline = time.monotonic() + budget
    while True:
        row, ok, reason, matches = _manual_exit_lookup_once(tag)
        if row is not None or not ok or reason.startswith("ambiguous"):
            return row, ok, reason, matches
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return row, ok, reason, matches
        time.sleep(min(0.2, remaining))



def _place_tracked(exchange, symbol, qty, entry, body_target, body_stop, strategy, trail, product,
                   side: str = "B", t_recv_ms: int | None = None, tag: str | None = None):
    """Place entry as plain Limit; SL/target/trailing are managed synthetically by the engine.
    (SDK v3 place_order exposes no native BO/CO kwargs.)
    side B = buy/long (SL+exit are SELL), S = sell/short (SL+exit are BUY).
    Planned target/stop are side-aware; the engine recomputes them from the
    actual fill price when the entry completes (prevents 'STOP PRICE IS NOT
    REASONABLE' when the fill differs from ref_ltp+5).

    Plan 06 WP8: a deterministic entry tag is pre-registered as an intent BEFORE
    the broker call. A timeout/uncertain result is resolved by querying the order
    report for that tag over a short bounded window (adopt, never duplicate). An
    exact tag match ignores symbol/qty/side; untagged rows may be adopted only
    via a unique symbol/qty/side fallback; multiple matches are alerted and never
    adopted arbitrarily. Only an authoritative miss may trigger one retry
    (`ORDER_PLACE_MAX_ATTEMPTS`). Every attempt tag is retained: a superseded
    attempt that lands late is still adopted and flagged DUPLICATE_ENTRY. If the
    book is unavailable the intent stays pending and no retry is sent. The whole
    flow runs inside one worker command; the route wait covers its bounded worst
    case so a client timeout cannot race a second attempt.
    Timing is 3x time.time() calls (microseconds) - negligible overhead, used for the
    click-to-fill latency display."""
    assert kotak is not None and engine is not None
    side = "S" if str(side).upper() == "S" else "B"
    # Plan 06 WP11: never stack a second bracket on one contract unless the
    # operator explicitly enabled it. Checked on the worker so a concurrent
    # double request cannot both pass a route-side check.
    if not settings.allow_stacked_entries:
        existing = _stacked_leg(symbol)
        if existing is not None:
            log.warning("STACKED ENTRY BLOCKED %s: existing leg %s status=%s",
                        symbol, existing.get("entry_order_id"),
                        existing.get("status"))
            raise HTTPException(
                status_code=409,
                detail=(f"{symbol} already has an open bracket "
                        f"({existing.get('status')}, entry "
                        f"{existing.get('entry_order_id')}). Stacked entries are "
                        "disabled (ALLOW_STACKED_ENTRIES=false); cancel/exit it "
                        "first or enable stacked entries server-side."))
    t_recv_ms = t_recv_ms if t_recv_ms is not None else _ms()
    kw = dict(exchange_segment=exchange, quantity=qty, trading_symbol=symbol,
              transaction_type=side, product=product, order_type="L", price=entry)
    if side == "S":
        target = max(0.05, float(entry) - float(body_target))
        stop = float(entry) + float(body_stop)
    else:
        target = float(entry) + float(body_target)
        stop = max(0.05, float(entry) - float(body_stop))
    token = (str(kotak.token_for(symbol, exchange))
             if callable(getattr(kotak, "token_for", None)) else "")
    payload = {
        "sl_order_id": None, "trading_symbol": symbol, "status": "PENDING_ENTRY",
        "strategy": strategy, "target_price": str(round(target, 2)),
        "stop_loss_price": str(round(stop, 2)), "trail_distance": trail,
        "high_water_mark": 0.0, "low_water_mark": 0.0, "quantity": qty,
        "exchange_segment": exchange, "instrument_token": token, "side": side,
        "product": product, "target_points": str(body_target),
        "stop_points": str(body_stop), "api_recv_ms": t_recv_ms,
    }
    if tag:
        payload["client_order_tag"] = tag   # UI idempotency key, not the broker tag
    ref = _entry_ref()
    max_attempts = max(1, int(settings.order_place_max_attempts))
    entry_id = None
    chosen_tag = None
    t_b0 = t_b1 = _ms()
    adopted = False
    last_error = None
    for attempt in range(1, max_attempts + 1):
        payload["_entry_ref"] = ref
        payload["_attempt"] = attempt
        chosen_tag = SyntheticEngine.make_tag("E", ref, attempt)
        payload["entry_client_tag"] = chosen_tag
        payload["tag"] = chosen_tag
        registered = False
        for _ in range(4):
            if engine.register_entry_intent(chosen_tag, payload):
                registered = True
                break
            # Tag collision (another attempt/tracker owns this value): never
            # reuse it; re-roll the reference and try again.
            ref = _entry_ref()
            payload["_entry_ref"] = ref
            chosen_tag = SyntheticEngine.make_tag("E", ref, attempt)
            payload["entry_client_tag"] = chosen_tag
            payload["tag"] = chosen_tag
        if not registered:
            raise HTTPException(
                status_code=500,
                detail=f"Could not allocate a unique entry tag for {symbol}")
        kw["tag"] = chosen_tag
        t_b0 = _ms()
        r = None
        try:
            r = kotak.place_order(**kw)
            t_b1 = _ms()
        except Exception as e:  # noqa: BLE001 - timeout/reset => uncertain placement
            t_b1 = _ms()
            last_error = _safe_text(e)
            log.error("ENTRY PLACE ambiguous %s tag=%s: %s", symbol, chosen_tag,
                      last_error)
        if r is not None:
            if not _broker_ok(r):
                engine.abandon_entry_intent(chosen_tag, "broker rejected")
                raise HTTPException(status_code=502,
                                    detail=f"Entry rejected for {symbol}: {_safe_text(r)}")
            oid = (r.get("nOrdNo") if isinstance(r, dict) else None)
            if oid:
                entry_id = str(oid)
                break
            last_error = "broker accepted the entry without an order id"
        # Uncertain result: the order may already be live. The lookup ignores
        # symbol/qty/side for an exact tag match, uses them only for untagged
        # rows, and never adopts arbitrarily when multiple rows match.
        found, book_ok, reason, matches = _find_entry_confirmed(
            chosen_tag, symbol, qty, side)
        if reason in ("ambiguous_tag", "ambiguous_fallback"):
            engine.mark_entry_ambiguous(chosen_tag, reason)
            log.critical("ENTRY AMBIGUOUS %s tag=%s: %d order(s) match (%s) - "
                         "no adoption/retry; MANUAL ACTION REQUIRED",
                         symbol, chosen_tag, len(matches), reason)
            raise HTTPException(
                status_code=502,
                detail=(f"Entry placement ambiguous for {symbol}: "
                        f"{len(matches)} broker order(s) match {chosen_tag}; "
                        "no retry was sent. Check the order book manually."))
        if found is not None:
            found_id = str(found.get("nOrdNo") or "")
            found_st = str(found.get("ordSt") or "").lower()
            if "reject" in found_st:
                engine.abandon_entry_intent(chosen_tag, f"order report: {found_st}")
                raise HTTPException(status_code=502,
                                    detail=f"Entry rejected for {symbol}: {found_st}")
            if found_id:
                entry_id = found_id
                adopted = True
                if reason == "fallback":
                    log.warning("ENTRY AMBIGUOUS adopted by unique "
                                "symbol/qty/side (tag missing) %s -> order %s",
                                chosen_tag, entry_id)
                else:
                    log.warning("ENTRY AMBIGUOUS adopted by tag %s -> order %s "
                                "(%s)", chosen_tag, entry_id, symbol)
                break
        if not book_ok:
            # Cannot prove the order was not accepted: keep the intent pending
            # so a later reconcile/feed frame can adopt it. Never retry blind.
            raise HTTPException(
                status_code=502,
                detail=(f"Entry placement uncertain for {symbol}: "
                        f"{last_error or 'no order id'} (order book unavailable; "
                        "no automatic retry)"))
        # Authoritative miss over the bounded confirmation window. Keep the tag
        # as superseded (never discard): if it lands late it is adopted with a
        # DUPLICATE_ENTRY alert. Only then may the bounded retry fire.
        engine.supersede_entry_intent(
            chosen_tag, "authoritative tag miss",
            superseded_by=(SyntheticEngine.make_tag("E", ref, attempt + 1)
                           if attempt < max_attempts else None))
        if attempt < max_attempts:
            log.warning("ENTRY PLACE RETRY %s tag=%s (attempt %d/%d) after "
                        "authoritative tag miss", symbol, chosen_tag, attempt,
                        max_attempts)
            continue
        break
    if entry_id is None:
        raise HTTPException(
            status_code=502,
            detail=(f"Entry placement failed for {symbol} after {max_attempts} "
                    f"attempt(s): {last_error or 'no broker order id'}"))
    tracker = engine.commit_entry(chosen_tag, entry_id)
    if tracker is None:
        tracker = engine.by_entry_tag(chosen_tag)
    if tracker is None:
        # Only reachable if the feed adopted the tag under a different id; fall
        # back to a direct registration so the position is never unmanaged.
        payload["entry_order_id"] = entry_id
        engine.add(payload)
    log.info("entry order placed symbol=%s order=%s side=%s qty=%s tag=%s%s",
             symbol, entry_id, side, qty, chosen_tag,
             " (adopted by tag)" if adopted else "")
    # stream this leg's ticks so target/trailing can fire
    try:
        if bridge is not None:
            bridge.ensure_option(exchange, token)
    except Exception:
        pass
    return {"entry_order_id": entry_id, "symbol": symbol, "entry": entry,
            "target": target, "stop": stop, "client_tag": chosen_tag,
            "timing": {"server_recv_ms": t_recv_ms, "broker_rtt_ms": t_b1 - t_b0,
                       "server_total_ms": _ms() - t_recv_ms, "placed_at_ms": t_b1}}


@router.post("/single", dependencies=[Depends(require_user)])
def place_single(body: SingleOrderBody, ref_ltp: float = 0):
    t_recv = _ms()
    _require_master()
    ltp = ref_ltp or (hub.latest.get("__manual__", {}).get("ltp", 0) if hub else 0)
    if not ltp:
        # caller must pass ?ref_ltp=<live price from UI ticker>
        raise HTTPException(status_code=400, detail="Pass ?ref_ltp=<current option LTP> (shown in ticket)")
    ex, sym, qty, entry = _leg(body.instrument, body.expiry, body.option_type, body.strike,
                               body.lots, body.entry_mode, ltp, body.entry_offset, body.side,
                               body.offset, body.limit_price)
    tag = (body.client_order_tag or "").strip() or None
    with _idem_lock:
        replay = _idem_replay(tag)
        if replay is not None:
            log.info("idempotent replay: single tag=%s -> %s", tag, replay.get("entry_order_id"))
            return replay
        result = _run_on_worker(
            _place_tracked, ex, sym, qty, entry, body.target_points, body.stop_points,
            body.strategy, body.trail_distance, body.product, body.side,
            t_recv, tag=tag, label=f"place_single:{sym}",
            timeout=_place_wait_sec())
        result = _attach_warnings(result, sym, body.side, entry, ltp)
        _idem_store(tag, result)
        return result


@router.post("/straddle", dependencies=[Depends(require_user)])
def place_straddle(body: StraddleBody, ce_ltp: float = 0, pe_ltp: float = 0):
    t_recv = _ms()
    _require_master()
    if not (ce_ltp and pe_ltp):
        raise HTTPException(status_code=400, detail="Pass ?ce_ltp=&pe_ltp= from tickers")
    tag = (body.client_order_tag or "").strip() or None
    with _idem_lock:
        replay = _idem_replay(tag)
        if replay is not None:
            log.info("idempotent replay: straddle tag=%s", tag)
            return replay
        out = []
        for otype, strike, ltp in (("CE", body.ce_strike, ce_ltp), ("PE", body.pe_strike, pe_ltp)):
            ex, sym, qty, entry = _leg(body.instrument, body.expiry, otype, strike, body.lots,
                                       body.entry_mode, ltp, 0, body.side)
            leg_out = _run_on_worker(
                _place_tracked, ex, sym, qty, entry, body.target_points, body.stop_points,
                body.strategy, body.trail_distance, body.product,
                body.side, t_recv, tag=tag, label=f"place_straddle:{sym}",
                timeout=_place_wait_sec())
            out.append(_attach_warnings(leg_out, sym, body.side, entry, ltp))
        result = {"legs": out}
        _idem_store(tag, result)
        return result


@router.get("/synthetic", dependencies=[Depends(require_user)])
def list_synthetic():
    assert engine is not None
    return {"orders": engine.all()}


@router.get("/orderbook", dependencies=[Depends(require_user)])
def orderbook():
    """Normalized working/pending/rejected rows + fetch freshness (Plan 06 WP6).

    A failed read is never reported as a successful empty book: it returns a
    structured `ok:false` body with a 5xx status.
    """
    assert kotak is not None
    try:
        res = _run_read(_orderbook_impl, label="orderbook")
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={
            "ok": False, "orders": [], "count": 0,
            "error": _safe_text(e.detail), "fetched_at_ms": _ms(),
            "stale": True, "source": "broker"})
    except Exception as e:  # noqa: BLE001
        return JSONResponse(status_code=502, content={
            "ok": False, "orders": [], "count": 0,
            "error": _safe_text(e), "fetched_at_ms": _ms(),
            "stale": True, "source": "broker"})
    if not res.get("ok"):
        return JSONResponse(status_code=502, content=res)
    return res


@router.post("/cancel/{order_id}", dependencies=[Depends(require_user)])
def cancel(order_id: str):
    assert kotak is not None
    try:
        res = _run_on_worker(_cancel_impl, order_id,
                             label=f"cancel:{order_id}")
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={
            "ok": False, "action": "cancel", "order_id": order_id,
            "error": _safe_text(e.detail)})
    if not (res.get("ok") or res.get("already")):
        return JSONResponse(status_code=502, content=res)
    return res


def _cancel_impl(order_id: str, schedule: bool = True) -> dict:
    """Cancel one broker order on the worker, idempotently.

    The tracker records the requested id *before* the feed confirms, so a
    double-click / retry cannot send a second cancel. On success a reconcile is
    scheduled so the tracker converges from the broker book even with a dead feed.
    """
    assert kotak is not None and engine is not None
    oid = str(order_id).strip()
    if not oid:
        return {"ok": False, "action": "cancel", "order_id": oid,
                "error": "order id required"}
    if engine.manual_cancel_requested(oid):
        return {"ok": True, "already": True, "action": "cancel",
                "order_id": oid, "result": {"stCode": 200,
                                            "note": "cancel already requested"},
                "cancelled": 0}
    syn = engine.by_any_id(oid)
    what = "unknown"
    if syn:
        if oid == str(syn.get("entry_order_id")):
            what = "entry"
        elif oid == str(syn.get("sl_order_id")):
            what = "sl"
        elif oid == str(syn.get("exit_order_id")):
            what = "exit"
    try:
        r = kotak.cancel(oid)
        ok = _broker_ok(r)
        err = None if ok else _safe_text(r)
    except Exception as e:  # noqa: BLE001 - reported as a structured failure
        r, ok, err = None, False, _safe_text(e)
    engine.note_manual_cancel(oid, ok, err)
    if ok and schedule:
        _request_reconcile("cancel")
    res = {"ok": bool(ok), "already": False, "action": "cancel",
           "order_id": oid, "what": what, "result": _safe_result(r),
           "cancelled": 1 if ok else 0}
    if not ok:
        res["error"] = err or "cancel failed"
        log.error("MANUAL CANCEL failed order=%s symbol=%s: %s", oid,
                  (syn or {}).get("trading_symbol"), err)
    return res


@router.post("/cancel-all/{symbol}", dependencies=[Depends(require_user)])
def cancel_all(symbol: str):
    """Cancel every tracked working entry/SL for one symbol, idempotently."""
    assert kotak is not None and engine is not None
    try:
        res = _run_on_worker(_cancel_all_impl, symbol,
                             label=f"cancel_all:{symbol}")
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={
            "ok": False, "action": "cancel-all", "symbol": symbol,
            "results": [], "error": _safe_text(e.detail)})
    if not res.get("ok"):
        return JSONResponse(status_code=502, content=res)
    return res


def _cancel_all_impl(symbol: str) -> dict:
    assert engine is not None
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"ok": False, "action": "cancel-all", "symbol": sym,
                "results": [], "error": "symbol required"}
    ids: list[str] = []
    seen: set[str] = set()
    for leg in engine.legs_for_symbol(sym):
        status = leg.get("status")
        entry = str(leg.get("entry_order_id") or "")
        sl = str(leg.get("sl_order_id") or "")
        # Entry is still working while PENDING_ENTRY (or a partial-fill cancel
        # is still being retried); the SL is working for any live bracket.
        if (entry and (status == "PENDING_ENTRY"
                       or leg.get("entry_cancel_pending"))
                and entry not in seen):
            seen.add(entry)
            ids.append(entry)
        if (sl and status in ("ACTIVE", "TRAILING", "CLOSING", "ERRORED")
                and sl not in seen):
            seen.add(sl)
            ids.append(sl)
    results = [_cancel_impl(oid, schedule=False) for oid in ids]
    if ids:
        _request_reconcile("cancel-all")
    cancelled = sum(1 for r in results if r.get("ok") and not r.get("already"))
    already = sum(1 for r in results if r.get("already"))
    failed = [r for r in results
              if not (r.get("ok") or r.get("already"))]
    ok = not failed
    out = {"ok": ok, "action": "cancel-all", "symbol": sym,
           "requested_count": len(ids), "cancelled_count": cancelled,
           "already_count": already, "failed_count": len(failed),
           "results": results}
    if failed:
        details = "; ".join(
            f"{r.get('order_id')}: {r.get('error') or 'cancel failed'}"
            for r in failed)
        out["error"] = (f"{len(failed)} of {len(results)} cancel(s) failed "
                        f"for {sym}: {details}")
    return out


@router.post("/exit-position", dependencies=[Depends(require_user)])
def exit_position(body: ExitBody):
    return _run_on_worker(_exit_position_impl, body,
                          label=f"exit_position:{body.symbol.strip().upper()}")


def _positions_strict():
    """Worker-side positions read: failure must never look like a flat account."""
    assert kotak is not None
    return kotak.positions(strict=True)


def _manual_exit_response(rec: dict, order, adopted: bool, t_b1: int) -> dict:
    base = copy.deepcopy(rec.get("base") or {})
    base.pop("_t_recv_ms", None)
    t_recv = int(rec.get("t_recv_ms") or _ms())
    t_b0 = int(rec.get("t_b0_ms") or t_b1)
    return {
        **base,
        "exit_order": order,
        "adopted": bool(adopted),
        "manual_exit_tag": rec.get("tag"),
        "client_tag": rec.get("client_tag"),
        "timing": {"server_recv_ms": t_recv, "broker_rtt_ms": t_b1 - t_b0,
                   "server_total_ms": _ms() - t_recv, "placed_at_ms": t_b1},
    }


def _manual_exit_existing(symbol: str):
    """Replay/adopt a prior manual exit instead of placing a duplicate."""
    rec = _manual_exit_get(symbol)
    if rec is None:
        return None
    if rec.get("state") == "complete" and rec.get("response"):
        return copy.deepcopy(rec["response"])
    tag = str(rec.get("tag") or "")
    if tag:
        found, _book_ok, _reason, _matches = _find_manual_exit_confirmed(tag)
        if found is not None:
            oid = str(found.get("nOrdNo") or "")
            st = str(found.get("ordSt") or "").lower()
            if "reject" in st:
                engine.abandon_manual_exit_tag(tag)
                _manual_exit_drop(symbol, tag)
                raise HTTPException(
                    status_code=502,
                    detail=f"Exit rejected for {symbol}: {st}")
            if oid:
                resp = _manual_exit_response(rec, found, True, _ms())
                _manual_exit_update(symbol, tag, state="complete", response=resp,
                                    updated_at=time.time())
                engine.complete_manual_exit_tag(tag, oid)
                log.warning("EXIT MANUAL adopted by tag %s -> order %s (%s)",
                            tag, oid, symbol)
                return resp
        if rec.get("state") == "ambiguous":
            raise HTTPException(
                status_code=502,
                detail=(f"Previous exit for {symbol} is still unconfirmed "
                        f"(tag {tag}); no duplicate was sent. Check the order "
                        "book/positions before retrying."))
    raise HTTPException(
        status_code=409,
        detail=(f"A manual exit for {symbol} is already in progress; "
                "no duplicate was sent."))


def _exit_position_impl(body: ExitBody):
    """Flatten one net position at market and stand down its bracket.

    1. sizes the exit from the live broker position (full net qty),
    2. cancels any live entry/SL exchange orders for that symbol,
    3. marks those synthetic trackers CLOSED (target is synthetic - there is
       no exchange target order to cancel),
    4. fires one tagged market order in the opposite direction.

    Positions are read through the strict read worker: timeout/queue/broker
    failure maps to 504/503/502, never a 404 "no position" on a failed read.
    The market exit carries a deterministic tag stored before the broker call.
    A lost response is resolved by that tag; repeated clicks/symbol retries
    replay or adopt the same exit and never place a second one.
    """
    assert kotak is not None and engine is not None
    symbol = body.symbol.strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol required")
    existing = _manual_exit_existing(symbol)
    if existing is not None:
        return existing
    t_recv = _ms()
    try:
        positions = _run_read(_positions_strict,
                              label=f"exit_position:positions:{symbol}")
    except HTTPException:
        raise
    except Exception as e:
        log.error("exit-position positions read failed for %s: %s",
                  symbol, _safe_text(e))
        raise HTTPException(
            status_code=502,
            detail=(f"Positions unavailable for {symbol}: {_safe_text(e)} "
                    "(no exit order was sent)")) from e
    pos = (positions or {}).get(symbol)
    if not pos:
        raise HTTPException(status_code=404, detail=f"No open position in {symbol}")
    try:
        net_lots = float(pos.get("qty", 0))
    except (TypeError, ValueError):
        net_lots = 0
    if not net_lots:
        raise HTTPException(status_code=404, detail=f"No open position in {symbol}")
    shares = int(pos.get("qty_shares") or 0)
    if shares <= 0:
        lot = int(pos.get("lot_sz") or 0) or _lot("NIFTY" if symbol.startswith("NIFTY") else "SENSEX")
        shares = int(round(abs(net_lots) * lot)) or int(round(abs(net_lots)))
    if shares <= 0:
        raise HTTPException(status_code=400, detail=f"Could not size exit for {symbol}")
    # long -> SELL to flatten, short -> BUY
    exit_txn = "S" if net_lots > 0 else "B"
    exchange = pos.get("exchange") or ("bse_fo" if symbol.startswith("SENSEX") else "nse_fo")
    legs = engine.legs_for_symbol(symbol)
    product = body.product or (legs[0].get("product") if legs else None) \
        or pos.get("product") or "MIS"

    cancelled: list[dict] = []
    for leg in legs:
        for key in ("sl_order_id", "entry_order_id"):
            oid = leg.get(key)
            # entry cancel only makes sense while pending; SL while working
            if key == "entry_order_id" and leg.get("status") != "PENDING_ENTRY":
                continue
            if key == "sl_order_id" and leg.get("status") not in ("ACTIVE", "TRAILING", "CLOSING", "ERRORED"):
                continue
            if not oid:
                continue
            try:
                _broker_call(kotak.cancel, str(oid))
                cancelled.append({"order_id": str(oid), "what": key})
            except Exception as e:
                cancelled.append({"order_id": str(oid), "what": key,
                                  "warn": _safe_text(e)})
    engine.mark_manual_exit(symbol)

    client_tag = (body.client_order_tag or "").strip() or None
    tag = None
    for _ in range(4):
        candidate = SyntheticEngine.make_tag("X", _entry_ref(), 1)
        if engine.register_manual_exit_tag(candidate, symbol, client_tag):
            tag = candidate
            break
    if tag is None:
        raise HTTPException(
            status_code=500,
            detail=f"Could not allocate a unique manual-exit tag for {symbol}")
    rec, claimed = _manual_exit_claim(symbol, tag, client_tag)
    if not claimed:
        engine.abandon_manual_exit_tag(tag)
        replay = _manual_exit_get(symbol)
        if replay and replay.get("response"):
            return copy.deepcopy(replay["response"])
        raise HTTPException(
            status_code=409,
            detail=(f"A manual exit for {symbol} is already in progress; "
                    "no duplicate was sent."))

    base = {
        "symbol": symbol, "exit_side": ("SELL" if exit_txn == "S" else "BUY"),
        "quantity": str(shares), "exchange": exchange, "product": product,
        "cancelled": cancelled, "closed_legs": len(legs),
    }
    t_b0 = _ms()
    _manual_exit_update(symbol, tag, base=base, t_recv_ms=t_recv, t_b0_ms=t_b0)
    try:
        r = _call_place_with_tag(
            kotak.place_market_exit, exchange, str(shares), symbol,
            tag=tag, transaction_type=exit_txn, product=product)
        t_b1 = _ms()
    except HTTPException:
        engine.abandon_manual_exit_tag(tag)
        _manual_exit_drop(symbol, tag)
        raise
    except Exception as e:
        t_b1 = _ms()
        last_error = _safe_text(e)
        log.error("exit-position MKT failed for %s tag=%s: %s",
                  symbol, tag, last_error)
        found, book_ok, reason, matches = _find_manual_exit_confirmed(tag)
        if found is not None:
            oid = str(found.get("nOrdNo") or "")
            st = str(found.get("ordSt") or "").lower()
            if "reject" in st:
                engine.abandon_manual_exit_tag(tag)
                _manual_exit_drop(symbol, tag)
                raise HTTPException(
                    status_code=502,
                    detail=f"Exit rejected for {symbol}: {st}") from e
            if oid:
                rec = _manual_exit_get(symbol) or rec
                resp = _manual_exit_response(rec, found, True, t_b1)
                _manual_exit_update(symbol, tag, state="complete", response=resp)
                engine.complete_manual_exit_tag(tag, oid)
                log.warning("EXIT MANUAL adopted by tag %s -> order %s (%s)",
                            tag, oid, symbol)
                return resp
        _manual_exit_update(symbol, tag, state="ambiguous", error=last_error)
        if reason.startswith("ambiguous"):
            raise HTTPException(
                status_code=502,
                detail=(f"Exit placement ambiguous for {symbol}: "
                        f"{len(matches)} broker order(s) match tag {tag}; no "
                        "retry was sent. Check the order book.")) from e
        if not book_ok:
            raise HTTPException(
                status_code=502,
                detail=(f"Exit placement uncertain for {symbol}: {last_error} "
                        "(order book unavailable; no automatic retry)")) from e
        raise HTTPException(
            status_code=502,
            detail=(f"Exit placement uncertain for {symbol}: {last_error} "
                    f"(tag {tag} not in the order book yet; no retry was sent)")) from e
    if not (r and _broker_ok(r)):
        engine.abandon_manual_exit_tag(tag)
        _manual_exit_drop(symbol, tag)
        # Log the full rejection server-side - the UI only shows a summary and
        # the order-feed REJECTED row is the only other trace.
        log.error("exit-position MKT rejected for %s (side=%s qty=%s product=%s): %s",
                  symbol, exit_txn, shares, product, _safe_text(r))
        raise HTTPException(status_code=502,
                            detail=f"Exit rejected for {symbol}: {_safe_text(r)}")
    oid = str(r.get("nOrdNo") or "")
    rec = _manual_exit_get(symbol) or rec
    resp = _manual_exit_response(rec, r, False, t_b1)
    _manual_exit_update(symbol, tag, state="complete", response=resp)
    engine.complete_manual_exit_tag(tag, oid)
    return resp



@router.post("/force-exit")
def force_exit(body: ForceExitBody, user: str = Depends(require_user)):
    """Audited operator escape hatch for a blocked (EXIT_BLOCKED_SL_STILL_WORKING)
    or naked/ERRORED leg. Still cancels + confirms the SL before one market exit;
    it never bypasses the no-exit-while-SL-working invariant.
    """
    assert engine is not None
    syn = engine.by_any_id(body.entry_order_id)
    if syn is None:
        raise HTTPException(status_code=404,
                            detail=f"No synthetic leg for {body.entry_order_id}")
    log.warning("AUDIT force_exit requested user=%s entry=%s symbol=%s reason=%s",
                user, body.entry_order_id, syn.get("trading_symbol"), body.reason)
    result = _run_on_worker(_force_exit_impl, body, user,
                            label=f"force_exit:{body.entry_order_id}")
    return result


def _force_exit_impl(body: ForceExitBody, user: str):
    assert engine is not None
    res = engine.force_exit(body.entry_order_id, user, body.reason)
    if res is None:
        raise HTTPException(status_code=404,
                            detail=f"No synthetic leg for {body.entry_order_id}")
    outcome = res.get("force_exit_result")
    # Pending is not success: the operator must see 409 unless the broker
    # cancel/exit is actually confirmed.
    ok = outcome in ("closed", "exit_placed", "sl_hit")
    payload = {"ok": ok, "result": outcome, "leg": res}
    if not ok:
        log.error("AUDIT force_exit BLOCKED user=%s entry=%s result=%s",
                  user, body.entry_order_id, outcome)
        return JSONResponse(status_code=409, content=payload)
    log.warning("AUDIT force_exit done user=%s entry=%s exit=%s result=%s",
                user, body.entry_order_id, res.get("exit_order_id"), outcome)
    return payload


@router.post("/modify-sl/{order_id}", dependencies=[Depends(require_user)])
def modify_sl(order_id: str, trigger: float, quantity: str | None = None):
    assert kotak is not None and engine is not None
    try:
        res = _run_on_worker(_modify_sl_impl, order_id, trigger, quantity,
                             label=f"modify_sl:{order_id}")
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={
            "ok": False, "action": "modify-sl", "order_id": order_id,
            "trigger": trigger, "error": _safe_text(e.detail)})
    if not res.get("ok"):
        code = 400 if res.get("bad_request") else 502
        return JSONResponse(status_code=code, content=res)
    return res


def _modify_sl_impl(order_id: str, trigger: float, quantity: str | None):
    """Manual SL trigger change: quantity comes from the tracker (v3 requires it).

    `sl_modify_pending` is set before the broker call so an in-flight modify can
    never be fought by an automatic SL re-place. The pending window settles when
    the feed/book echoes the new trigger (or its deadline expires); a failure
    clears it and alerts instead of silently reverting the tracker.
    """
    assert kotak is not None and engine is not None
    oid = str(order_id).strip()
    try:
        trig = float(trigger)
    except (TypeError, ValueError):
        trig = 0.0
    if not oid or trig <= 0:
        return {"ok": False, "action": "modify-sl", "order_id": oid,
                "trigger": trigger, "bad_request": True,
                "error": "order_id and a positive trigger required"}
    syn = engine.by_any_id(oid)
    if syn is not None and oid != str(syn.get("sl_order_id") or ""):
        return {"ok": False, "action": "modify-sl", "order_id": oid,
                "trigger": trig, "bad_request": True,
                "error": "order is not the tracked SL for this leg"}
    # Tracker quantity is authoritative; the explicit param is only a fallback
    # for an order this app does not track.
    raw_qty = ((syn.get("sl_quantity") or syn.get("quantity")) if syn else None) \
        or quantity
    qty = _positive_int_qty(raw_qty)
    if qty is None:
        return {"ok": False, "action": "modify-sl", "order_id": oid,
                "trigger": trig, "bad_request": True,
                "error": "no valid tracked quantity (positive integer required)"}
    if syn is not None and syn.get("status") == "TRAILING":
        try:
            old_stop = float(syn.get("stop_loss_price"))
        except (TypeError, ValueError):
            old_stop = 0.0
        side = str(syn.get("side") or "B").upper()
        loosened = (side == "S" and trig > old_stop) or \
                   (side != "S" and trig < old_stop)
        if old_stop > 0 and loosened:
            return {"ok": False, "action": "modify-sl", "order_id": oid,
                    "trigger": f"{trig:.2f}", "bad_request": True,
                    "error": ("cannot loosen the SL on a TRAILING leg "
                              f"(current {old_stop:.2f}); only tighten")}
    sl_txn = "B" if (syn and syn.get("side") == "S") else "S"
    engine.begin_sl_modify(oid, trig)
    try:
        r = kotak.modify_sl(oid, str(qty), f"{trig:.2f}",
                            transaction_type=sl_txn)
        ok = _broker_ok(r)
        err = None if ok else _safe_text(r)
    except Exception as e:  # noqa: BLE001 - structured failure + alert
        r, ok, err = None, False, _safe_text(e)
    engine.finish_sl_modify(oid, ok, err)
    if ok:
        _request_reconcile("modify-sl")
    leg = engine.by_any_id(oid) or {}
    res = {"ok": bool(ok), "action": "modify-sl", "order_id": oid,
           "trigger": f"{trig:.2f}", "quantity": str(qty),
           "result": _safe_result(r),
           "sl_modify_pending": bool(leg.get("sl_modify_pending"))}
    if not ok:
        res["error"] = err or "modify failed"
        log.error("MANUAL SL MODIFY failed order=%s symbol=%s trigger=%s: %s",
                  oid, (syn or {}).get("trading_symbol"), trig, err)
    return res
