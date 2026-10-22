from fastapi import APIRouter, Depends, HTTPException

from ..auth import require_user
from ..broker_worker import WorkerQueueFull
from ..config import settings
from ..kotak_client import KotakService
from ..schemas import WatchBody

router = APIRouter()
kotak: KotakService | None = None
bridge = None  # SFeedBridge, wired in main.py
read_worker = None  # dedicated CommandWorker for read-only broker calls


def _step(instrument: str) -> int:
    return 100 if instrument == "SENSEX" else 50


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
    ltp = kotak.quote_ltp(symbol, exchange)
    if ltp is None:
        raise HTTPException(status_code=502, detail=f"No quote for {symbol}")
    return {"symbol": symbol, "ltp": ltp, "token": kotak.token_for(symbol, exchange)}


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
    """REST fallback for watchlist LTPs (used until ws ticks arrive)."""
    assert kotak is not None
    out: dict[str, float | None] = {}
    for leg in body.legs:
        symbol = f"{leg.instrument}{leg.expiry}{leg.strike}{leg.option_type}"
        exchange = "bse_fo" if leg.instrument == "SENSEX" else "nse_fo"
        try:
            out[symbol] = kotak.quote_ltp(symbol, exchange)
        except Exception:
            out[symbol] = None
    return {"ltps": out}


@router.get("/positions", dependencies=[Depends(require_user)])
def positions():
    assert kotak is not None
    if read_worker is None:
        return {"positions": kotak.positions()}
    try:
        data = read_worker.submit_wait(
            kotak.positions, label="positions",
            timeout=max(0.1, float(settings.orderbook_timeout_sec)))
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=f"Broker read timed out: {e}")
    except WorkerQueueFull as e:
        raise HTTPException(status_code=503, detail=f"Broker read queue full: {e}")
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
