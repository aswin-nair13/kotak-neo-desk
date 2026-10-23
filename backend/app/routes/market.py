import time

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from ..auth import require_user
from ..broker_worker import WorkerQueueFull
from ..config import settings
from ..kotak_client import BrokerDataError, KotakService, sanitize_error_text
from ..schemas import WatchBody

router = APIRouter()
kotak: KotakService | None = None
bridge = None  # SFeedBridge, wired in main.py
read_worker = None  # dedicated CommandWorker for read-only broker calls


def _step(instrument: str) -> int:
    return 100 if instrument == "SENSEX" else 50


def _ms() -> int:
    return int(time.time() * 1000)


def _market_error(message, **extra) -> JSONResponse:
    """Structured 502: never a 200 with an empty/last-good-looking payload."""
    body = {"ok": False, "stale": True, "source": "broker",
            "error": sanitize_error_text(message), "fetched_at_ms": _ms()}
    body.update(extra)
    return JSONResponse(status_code=502, content=body)


def _positions_impl():
    """Worker-side strict positions read (raises BrokerDataError on failure)."""
    assert kotak is not None
    return kotak.positions(strict=True)


def _read_call(fn, *args, label: str, timeout: float | None = None, **kwargs):
    """Run a market-data read on the dedicated worker (or inline in tests).

    Timeout/backlog map to 504/503; broker/data failures are left to the caller
    so it can return a structured 502 instead of an empty-looking 200.
    """
    if read_worker is None:
        return fn(*args, **kwargs)
    wait = (max(0.1, float(settings.orderbook_timeout_sec))
            if timeout is None else timeout)
    try:
        return read_worker.submit_wait(
            fn, *args, label=label, timeout=wait, **kwargs)
    except TimeoutError as e:
        raise HTTPException(status_code=504,
                            detail=f"Broker read timed out: {e}") from e
    except WorkerQueueFull as e:
        raise HTTPException(status_code=503,
                            detail=f"Broker read queue full: {e}") from e


def _batch_quotes_impl(body: WatchBody):
    """Worker-side batch quote read: (ltps, per-symbol errors)."""
    assert kotak is not None
    out: dict[str, float | None] = {}
    errors: dict[str, str] = {}
    for leg in body.legs:
        symbol = f"{leg.instrument}{leg.expiry}{leg.strike}{leg.option_type}"
        exchange = "bse_fo" if leg.instrument == "SENSEX" else "nse_fo"
        try:
            out[symbol] = kotak.quote_ltp(symbol, exchange, strict=True)
        except BrokerDataError as e:
            out[symbol] = None
            errors[symbol] = sanitize_error_text(e)
        except Exception as e:  # noqa: BLE001 - surfaced as a structured failure
            out[symbol] = None
            errors[symbol] = sanitize_error_text(e)
    return out, errors


def _atm_filter(strikes: list[str], spot: float, step: int, width: int = 10) -> tuple[list[str], int | None]:
    """Keep ATM ± width strikes. Returns (filtered, atm). No spot -> all."""
    if not strikes or not spot or spot <= 0:
        return strikes, None
    try:
        atm = int(step * round(float(spot) / step))
    except (TypeError, ValueError):
        return strikes, None
    lo, hi = atm - width * step, atm + width * step
    filt = [s for s in strikes if lo <= int(float(s)) <= hi]
    return (filt or strikes), atm


@router.get("/expiries", dependencies=[Depends(require_user)])
def expiries(instrument: str = "NIFTY"):
    assert kotak is not None
    return {"instrument": instrument, "expiries": kotak.expiries(instrument)}


@router.get("/strikes", dependencies=[Depends(require_user)])
def strikes(instrument: str = "NIFTY", expiry: str = "", spot: float = 0):
    assert kotak is not None
    # Prefer real listed strikes from the scrip master; fall back to TransTrader ±4 estimate.
    step = _step(instrument)
    out = {}
    atm = None
    for otype in ("CE", "PE"):
        listed = kotak.master_strikes(instrument, expiry, otype) if expiry else []
        full = listed or KotakService.strikes(instrument, otype, spot)
        filt, a = _atm_filter(full, spot, step, 10)
        if a is not None:
            atm = a
        out[otype.lower()] = filt
    return {"instrument": instrument, "expiry": expiry, "atm": atm,
            "step": step, **out}


@router.get("/lots", dependencies=[Depends(require_user)])
def lots():
    return {"NIFTY": settings.nifty_lot, "SENSEX": settings.sensex_lot}


@router.get("/quote", dependencies=[Depends(require_user)])
def quote(instrument: str = "NIFTY", expiry: str = "", strike: str = "",
          option_type: str = "CE"):
    """LTP for one option contract, e.g. ?instrument=NIFTY&expiry=25N18&strike=25450&option_type=CE."""
    assert kotak is not None
    if not (expiry and strike):
        raise HTTPException(status_code=400, detail="expiry + strike required")
    symbol = f"{instrument}{expiry}{strike}{option_type}"
    exchange = "bse_fo" if instrument == "SENSEX" else "nse_fo"
    try:
        ltp = _read_call(kotak.quote_ltp, symbol, exchange, strict=True,
                         label=f"quote:{symbol}")
    except HTTPException:
        raise
    except BrokerDataError as e:
        return _market_error(e, symbol=symbol, ltp=None)
    except Exception as e:  # noqa: BLE001 - surfaced as a structured failure
        return _market_error(e, symbol=symbol, ltp=None)
    return {"symbol": symbol, "ltp": ltp, "token": kotak.token_for(symbol, exchange),
            "ok": True, "stale": False, "fetched_at_ms": _ms()}


@router.post("/watch", dependencies=[Depends(require_user)])
def watch(body: WatchBody):
    """Subscribe the server websocket to watchlist legs so their LTPs stream
    over /ws (ticks carry both token `tk` and symbol `ts`). Returns token map."""
    assert kotak is not None
    tokens: dict[str, str | None] = {}
    for leg in body.legs:
        symbol = f"{leg.instrument}{leg.expiry}{leg.strike}{leg.option_type}"
        exchange = "bse_fo" if leg.instrument == "SENSEX" else "nse_fo"
        tok = kotak.token_for(symbol, exchange)
        tokens[symbol] = tok
        try:
            if bridge is not None and tok:
                bridge.ensure_option(exchange, tok)
        except Exception:
            pass
    return {"tokens": tokens}


@router.post("/batch-quotes", dependencies=[Depends(require_user)])
def batch_quotes(body: WatchBody):
    """REST fallback for watchlist LTPs (used until ws ticks arrive).

    Any failed leg makes the whole response a structured 502. The UI keeps its
    previous prices and marks them stale; it never renders a failed quote as 0.
    """
    assert kotak is not None
    try:
        out, errors = _read_call(_batch_quotes_impl, body, label="batch-quotes")
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001 - structured failure, never partial 200
        return _market_error(e, ltps={})
    if errors:
        first = next(iter(errors.values()))
        return _market_error(first, ltps=out, errors=errors)
    return {"ltps": out, "ok": True, "stale": False, "fetched_at_ms": _ms()}


@router.get("/positions", dependencies=[Depends(require_user)])
def positions():
    """Net positions, or a structured 502 when the broker read fails.

    A failed/timed-out call is never returned as `200 {positions:{}}` (which
    would look like a flat account). The UI keeps its last good snapshot and
    shows an unavailable/stale banner with its timestamp.
    """
    assert kotak is not None
    try:
        data = _read_call(_positions_impl, label="positions")
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001 - structured failure, never {}
        return _market_error(e, positions={})
    return {"positions": data}


@router.post("/masters/refresh", dependencies=[Depends(require_user)])
def refresh_masters():
    """Re-download the scrip masters (Plan 02 C5). Clears the 'unavailable'
    state on success so the UI can re-enable order entry after a transient
    TLS/network failure at login."""
    assert kotak is not None
    if not kotak.logged_in:
        raise HTTPException(status_code=400, detail="Kotak session not live — log in first")
    return kotak.refresh_masters()
