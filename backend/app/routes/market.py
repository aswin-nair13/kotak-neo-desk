from fastapi import APIRouter, Depends, HTTPException

from ..auth import require_user
from ..config import settings
from ..kotak_client import KotakService

router = APIRouter()
kotak: KotakService | None = None


@router.get("/expiries", dependencies=[Depends(require_user)])
def expiries(instrument: str = "NIFTY"):
    assert kotak is not None
    return {"instrument": instrument, "expiries": kotak.expiries(instrument)}


@router.get("/strikes", dependencies=[Depends(require_user)])
def strikes(instrument: str = "NIFTY", expiry: str = "", spot: float = 0):
    assert kotak is not None
    # Prefer real listed strikes from the scrip master; fall back to TransTrader ±4 estimate.
    out = {}
    for otype in ("CE", "PE"):
        listed = kotak.master_strikes(instrument, expiry, otype) if expiry else []
        out[otype.lower()] = listed or KotakService.strikes(instrument, otype, spot)
    return {"instrument": instrument, "expiry": expiry, **out}


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
    return {"symbol": symbol, "ltp": ltp}


@router.get("/positions", dependencies=[Depends(require_user)])
def positions():
    assert kotak is not None
    return {"positions": kotak.positions()}
