"""Order endpoints: single leg, straddle (CE+PE), cancel/modify, synthetic list, orderbook."""

import time

from fastapi import APIRouter, Depends, HTTPException

from ..auth import require_user
from ..config import settings
from ..kotak_client import KotakService
from ..schemas import ExitBody, SingleOrderBody, StraddleBody
from ..synthetic_engine import SyntheticEngine
from ..ws_manager import Hub

router = APIRouter()

kotak: KotakService | None = None
engine: SyntheticEngine | None = None
hub: Hub | None = None
bridge = None  # SFeedBridge, wired in main.py


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


def _ms() -> int:
    return int(time.time() * 1000)


def _place_tracked(exchange, symbol, qty, entry, body_target, body_stop, strategy, trail, product,
                   side: str = "B", t_recv_ms: int | None = None):
    """Place entry as plain Limit; SL/target/trailing are managed synthetically by the engine.
    (SDK v3 place_order exposes no native BO/CO kwargs.)
    side B = buy/long (SL+exit are SELL), S = sell/short (SL+exit are BUY).
    Planned target/stop are side-aware; the engine recomputes them from the
    actual fill price when the entry completes (prevents 'STOP PRICE IS NOT
    REASONABLE' when the fill differs from ref_ltp+5).
    Timing is 3x time.time() calls (~µs) — negligible overhead, used for the
    click-to-fill latency display."""
    assert kotak is not None and engine is not None
    side = "S" if str(side).upper() == "S" else "B"
    t_recv_ms = t_recv_ms if t_recv_ms is not None else _ms()
    kw = dict(exchange_segment=exchange, quantity=qty, trading_symbol=symbol,
              transaction_type=side, product=product, order_type="L", price=entry)
    t_b0 = _ms()
    r = kotak.place_order(**kw)
    t_b1 = _ms()
    if not (r and r.get("stCode") == 200):
        raise HTTPException(status_code=502, detail=f"Entry rejected for {symbol}: {r}")
    entry_id = r.get("nOrdNo")
    if side == "S":
        target = max(0.05, float(entry) - float(body_target))
        stop = float(entry) + float(body_stop)
    else:
        target = float(entry) + float(body_target)
        stop = max(0.05, float(entry) - float(body_stop))
    token = str(kotak.token_for(symbol, exchange))
    engine.add({"entry_order_id": entry_id, "sl_order_id": None, "trading_symbol": symbol,
                "status": "PENDING_ENTRY", "strategy": strategy, "target_price": str(round(target, 2)),
                "stop_loss_price": str(round(stop, 2)), "trail_distance": trail, "high_water_mark": 0.0,
                "low_water_mark": 0.0, "quantity": qty, "exchange_segment": exchange,
                "instrument_token": token, "side": side, "product": product,
                "target_points": str(body_target), "stop_points": str(body_stop),
                "api_recv_ms": t_recv_ms, "placed_at_ms": t_b1, "broker_rtt_ms": t_b1 - t_b0})
    # stream this leg's ticks so target/trailing can fire
    try:
        if bridge is not None:
            bridge.ensure_option(exchange, token)
    except Exception:
        pass
    return {"entry_order_id": entry_id, "symbol": symbol, "entry": entry,
            "target": target, "stop": stop,
            "timing": {"server_recv_ms": t_recv_ms, "broker_rtt_ms": t_b1 - t_b0,
                       "server_total_ms": _ms() - t_recv_ms, "placed_at_ms": t_b1}}


@router.post("/single", dependencies=[Depends(require_user)])
def place_single(body: SingleOrderBody, ref_ltp: float = 0):
    t_recv = _ms()
    ltp = ref_ltp or (hub.latest.get("__manual__", {}).get("ltp", 0) if hub else 0)
    if not ltp:
        # caller must pass ?ref_ltp=<live price from UI ticker>
        raise HTTPException(status_code=400, detail="Pass ?ref_ltp=<current option LTP> (shown in ticket)")
    ex, sym, qty, entry = _leg(body.instrument, body.expiry, body.option_type, body.strike,
                               body.lots, body.entry_mode, ltp, body.entry_offset)
    return _place_tracked(ex, sym, qty, entry, body.target_points, body.stop_points,
                          body.strategy, body.trail_distance, body.product, body.side, t_recv)


@router.post("/straddle", dependencies=[Depends(require_user)])
def place_straddle(body: StraddleBody, ce_ltp: float = 0, pe_ltp: float = 0):
    t_recv = _ms()
    if not (ce_ltp and pe_ltp):
        raise HTTPException(status_code=400, detail="Pass ?ce_ltp=&pe_ltp= from tickers")
    out = []
    for otype, strike, ltp in (("CE", body.ce_strike, ce_ltp), ("PE", body.pe_strike, pe_ltp)):
        ex, sym, qty, entry = _leg(body.instrument, body.expiry, otype, strike, body.lots, body.entry_mode, ltp, 0)
        out.append(_place_tracked(ex, sym, qty, entry, body.target_points, body.stop_points,
                                  body.strategy, body.trail_distance, body.product, body.side, t_recv))
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


@router.post("/exit-position", dependencies=[Depends(require_user)])
def exit_position(body: ExitBody):
    """Flatten one net position at market and stand down its bracket.

    1. sizes the exit from the live broker position (full net qty),
    2. cancels any live entry/SL exchange orders for that symbol,
    3. marks those synthetic trackers CLOSED (target is synthetic — there is
       no exchange target order to cancel),
    4. fires one market order in the opposite direction.
    """
    assert kotak is not None and engine is not None
    symbol = body.symbol.strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol required")
    t_recv = _ms()
    pos = kotak.positions().get(symbol)
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
                kotak.cancel(str(oid))
                cancelled.append({"order_id": str(oid), "what": key})
            except Exception as e:
                cancelled.append({"order_id": str(oid), "what": key, "warn": str(e)[:200]})
    engine.mark_manual_exit(symbol)

    t_b0 = _ms()
    try:
        r = kotak.place_market_exit(exchange, str(shares), symbol,
                                    transaction_type=exit_txn, product=product)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Exit order failed for {symbol}: {e}")
    t_b1 = _ms()
    if not (r and r.get("stCode") == 200):
        raise HTTPException(status_code=502, detail=f"Exit rejected for {symbol}: {r}")
    return {"symbol": symbol, "exit_side": ("SELL" if exit_txn == "S" else "BUY"),
            "quantity": str(shares), "exchange": exchange, "product": product,
            "cancelled": cancelled, "closed_legs": len(legs), "exit_order": r,
            "timing": {"server_recv_ms": t_recv, "broker_rtt_ms": t_b1 - t_b0,
                       "server_total_ms": _ms() - t_recv, "placed_at_ms": t_b1}}


@router.post("/modify-sl/{order_id}", dependencies=[Depends(require_user)])
def modify_sl(order_id: str, trigger: float, quantity: str | None = None):
    """v3 modify_order requires quantity — taken from the synthetic tracker if omitted.
    Limit side follows the leg: long legs hold a SELL SL, short legs a BUY SL."""
    assert kotak is not None and engine is not None
    syn = engine.by_any_id(order_id)
    qty = quantity or (syn["quantity"] if syn else None)
    if not qty:
        raise HTTPException(status_code=400, detail="Pass ?quantity= (leg not tracked synthetically)")
    sl_txn = "B" if (syn and syn.get("side") == "S") else "S"
    try:
        return kotak.modify_sl(order_id, qty, str(trigger), transaction_type=sl_txn)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
