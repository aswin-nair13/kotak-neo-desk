"""Order endpoints: single leg, straddle (CE+PE), cancel/modify, synthetic list, orderbook."""

from fastapi import APIRouter, Depends, HTTPException

from ..auth import require_user
from ..config import settings
from ..kotak_client import KotakService
from ..schemas import SingleOrderBody, StraddleBody
from ..synthetic_engine import SyntheticEngine
from ..ws_manager import Hub

router = APIRouter()

kotak: KotakService | None = None
engine: SyntheticEngine | None = None
hub: Hub | None = None


def _lot(instrument: str) -> int:
    return settings.sensex_lot if instrument == "SENSEX" else settings.nifty_lot


def _leg(instrument, expiry, option_type, strike, lots, mode, ref_ltp, entry_offset):
    if not expiry:
        raise HTTPException(status_code=400, detail="Pick an expiry from GET /api/market/expiries")
    exchange = "bse_fo" if instrument == "SENSEX" else "nse_fo"
    symbol = f"{instrument}{expiry}{strike}{option_type}"
    qty = str(_lot(instrument) * lots)
    if mode == "L":
        entry = float(ref_ltp) - float(entry_offset)
    elif mode == "ML":
        entry = float(ref_ltp)
    elif mode == "L-1":
        entry = float(ref_ltp) - 1
    else:  # MKT emulated as limit +5 (TransTrader behaviour — MKT rejected for some F&O legs)
        entry = float(ref_ltp) + 5
    return exchange, symbol, qty, entry


def _place_tracked(exchange, symbol, qty, entry, body_target, body_stop, strategy, trail, product):
    """Place entry as plain Limit; SL/target/trailing are managed synthetically by the engine.
    (SDK v3 place_order exposes no native BO/CO kwargs.)"""
    assert kotak is not None and engine is not None
    kw = dict(exchange_segment=exchange, quantity=qty, trading_symbol=symbol,
              transaction_type="B", product=product, order_type="L", price=entry)
    r = kotak.place_order(**kw)
    if not (r and r.get("stCode") == 200):
        raise HTTPException(status_code=502, detail=f"Entry rejected for {symbol}: {r}")
    entry_id = r.get("nOrdNo")
    target = float(entry) + float(body_target)
    stop = float(entry) - float(body_stop)
    engine.add({"entry_order_id": entry_id, "sl_order_id": None, "trading_symbol": symbol,
                "status": "PENDING_ENTRY", "strategy": strategy, "target_price": str(target),
                "stop_loss_price": str(stop), "trail_distance": trail, "high_water_mark": 0.0,
                "quantity": qty, "exchange_segment": exchange,
                "instrument_token": str(kotak.token_for(symbol, exchange))})
    return {"entry_order_id": entry_id, "symbol": symbol, "entry": entry,
            "target": target, "stop": stop}


@router.post("/single", dependencies=[Depends(require_user)])
def place_single(body: SingleOrderBody, ref_ltp: float = 0):
    ltp = ref_ltp or (hub.latest.get("__manual__", {}).get("ltp", 0) if hub else 0)
    if not ltp:
        # caller must pass ?ref_ltp=<live price from UI ticker>
        raise HTTPException(status_code=400, detail="Pass ?ref_ltp=<current option LTP> (shown in ticket)")
    ex, sym, qty, entry = _leg(body.instrument, body.expiry, body.option_type, body.strike,
                               body.lots, body.entry_mode, ltp, body.entry_offset)
    return _place_tracked(ex, sym, qty, entry, body.target_points, body.stop_points,
                          body.strategy, body.trail_distance, body.product)


@router.post("/straddle", dependencies=[Depends(require_user)])
def place_straddle(body: StraddleBody, ce_ltp: float = 0, pe_ltp: float = 0):
    if not (ce_ltp and pe_ltp):
        raise HTTPException(status_code=400, detail="Pass ?ce_ltp=&pe_ltp= from tickers")
    out = []
    for otype, strike, ltp in (("CE", body.ce_strike, ce_ltp), ("PE", body.pe_strike, pe_ltp)):
        ex, sym, qty, entry = _leg(body.instrument, body.expiry, otype, strike, body.lots, body.entry_mode, ltp, 0)
        out.append(_place_tracked(ex, sym, qty, entry, body.target_points, body.stop_points,
                                  body.strategy, body.trail_distance, body.product))
    return {"legs": out}


@router.get("/synthetic", dependencies=[Depends(require_user)])
def list_synthetic():
    assert engine is not None
    return {"orders": engine.all()}


@router.get("/orderbook", dependencies=[Depends(require_user)])
def orderbook():
    assert kotak is not None
    try:
        return kotak.order_book()  # v3: order_report(); order_history() needs one order_id
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/cancel/{order_id}", dependencies=[Depends(require_user)])
def cancel(order_id: str):
    assert kotak is not None
    try:
        return kotak.cancel(order_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/modify-sl/{order_id}", dependencies=[Depends(require_user)])
def modify_sl(order_id: str, trigger: float, quantity: str | None = None):
    """v3 modify_order requires quantity — taken from the synthetic tracker if omitted."""
    assert kotak is not None and engine is not None
    qty = quantity
    if not qty:
        syn = engine.by_any_id(order_id)
        qty = syn["quantity"] if syn else None
    if not qty:
        raise HTTPException(status_code=400, detail="Pass ?quantity= (leg not tracked synthetically)")
    try:
        return kotak.modify_sl(order_id, qty, str(trigger))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
