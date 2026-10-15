"""Synthetic bracket engine — port of TransTrader controller logic.

Lifecycle per leg: PENDING_ENTRY -> ACTIVE -> {CLOSING -> CLOSED | TRAILING -> CLOSED} | ERRORED
  - entry fill  -> auto-place SL sell
  - LTP >= target + strategy=market_exit -> cancel SL, market-exit
  - strategy=trailing_stop -> on target, ratchet SL up by trail_distance on new highs
Feeds: on_order_update(order_details) from Kotak order socket; on_tick(tick) from price socket.
"""

import logging
import time

log = logging.getLogger("synthetic")


class SyntheticEngine:
    def __init__(self, broker):
        self.broker = broker  # KotakService
        self.orders: dict[str, dict] = {}  # entry_order_id -> tracker

    # -- CRUD --
    def add(self, d: dict):
        self.orders[d["entry_order_id"]] = d

    def by_any_id(self, oid: str) -> dict | None:
        if oid in self.orders:
            return self.orders[oid]
        for o in self.orders.values():
            if o.get("sl_order_id") == oid:
                return o
        return None

    def by_token(self, token: str) -> dict | None:
        for o in self.orders.values():
            if o.get("instrument_token") == str(token) and o.get("status") in ("ACTIVE", "TRAILING"):
                return o
        return None

    def all(self) -> list[dict]:
        return list(self.orders.values())

    def legs_for_symbol(self, symbol: str) -> list[dict]:
        return [o for o in self.orders.values()
                if o.get("trading_symbol") == symbol
                and o.get("status") in ("PENDING_ENTRY", "ACTIVE", "TRAILING", "CLOSING", "ERRORED")]

    def mark_manual_exit(self, symbol: str, reason: str = "MANUAL_EXIT") -> list[dict]:
        """Close all open trackers for a symbol (manual flatten from Positions).
        The target is purely synthetic (no exchange order), so only live
        entry/SL exchange orders need cancelling by the caller."""
        out = self.legs_for_symbol(symbol)
        for o in out:
            o["status"] = "CLOSED"
            o["exit_reason"] = reason
        return out

    # -- order feed --
    @staticmethod
    def _fill_price(od: dict) -> float | None:
        for k in ("prc", "average_price", "avgPrc"):
            try:
                v = float(od.get(k))
                if v and v > 0:
                    return v
            except (TypeError, ValueError):
                continue
        return None

    def on_order_update(self, od: dict):
        oid = od.get("nOrdNo")
        status = str(od.get("ordSt", "")).lower()
        syn = self.by_any_id(oid)
        if not syn:
            return
        t_now = int(time.time() * 1000)
        log.info("synthetic %s: %s -> %s (%s)", syn["trading_symbol"], oid, status, syn["status"])

        if oid == syn["entry_order_id"] and status == "complete" and syn["status"] == "PENDING_ENTRY":
            # Recompute target/SL from the ACTUAL fill price, not the planned
            # entry (ref_ltp+5). A planned stop can end up on the wrong side of
            # the market (e.g. fill lower than planned -> SELL SL trigger above
            # LTP), which the exchange rejects as 'STOP PRICE IS NOT REASONABLE'.
            try:
                side = syn.get("side", "B")
                # stored distances (preferred) — fall back to deriving from plan
                try:
                    target_dist = float(syn.get("target_points", 0) or 0)
                except (TypeError, ValueError):
                    target_dist = 0.0
                try:
                    stop_dist = float(syn.get("stop_points", 0) or 0)
                except (TypeError, ValueError):
                    stop_dist = 0.0
                fill = self._fill_price(od)
                if fill and target_dist and stop_dist:
                    if side == "S":  # short: profit when price falls
                        target = max(0.05, fill - target_dist)
                        stop = fill + stop_dist
                    else:
                        target = fill + target_dist
                        stop = max(0.05, fill - stop_dist)
                    syn["target_price"] = f"{target:.2f}"
                    syn["stop_loss_price"] = f"{stop:.2f}"
                    syn["fill_price"] = f"{fill:.2f}"
                sl_txn = "B" if side == "S" else "S"
                r = self.broker.place_stop_loss(syn["exchange_segment"], syn["quantity"],
                                                syn["trading_symbol"], syn["stop_loss_price"],
                                                transaction_type=sl_txn,
                                                product=syn.get("product", "MIS"))
                t_sl = int(time.time() * 1000)
                if r and r.get("stCode") == 200:
                    placed = syn.get("placed_at_ms")
                    try:
                        fill_lat = t_now - int(placed) if placed else None
                    except (TypeError, ValueError):
                        fill_lat = None
                    syn.update(status="ACTIVE", sl_order_id=r.get("nOrdNo"),
                               fill_at_ms=t_now, sl_placed_at_ms=t_sl)
                    if fill_lat is not None and fill_lat >= 0:
                        syn["fill_latency_ms"] = fill_lat
                else:
                    syn["status"] = "ERRORED"
                    syn["error"] = str(r)[:300]
            except Exception as e:
                log.error("SL place failed: %s", e)
                syn["status"] = "ERRORED"
                syn["error"] = str(e)[:300]

        elif oid == syn.get("sl_order_id") and status == "complete" and syn["status"] != "CLOSED":
            syn["status"] = "CLOSED"
            syn["closed_at_ms"] = t_now

        elif oid == syn.get("sl_order_id") and status == "cancelled" and syn["status"] == "CLOSING":
            try:
                side = syn.get("side", "B")
                exit_txn = "B" if side == "S" else "S"
                r = self.broker.place_market_exit(syn["exchange_segment"], syn["quantity"],
                                                  syn["trading_symbol"],
                                                  transaction_type=exit_txn,
                                                  product=syn.get("product", "MIS"))
                if not (r and r.get("stCode") == 200):
                    syn["status"] = "ERRORED"
            except Exception as e:
                log.error("market exit failed: %s", e)
                syn["status"] = "ERRORED"

        elif status == "complete" and syn["status"] == "CLOSING":
            syn["status"] = "CLOSED"
            syn["closed_at_ms"] = t_now

    # -- price feed --
    def on_tick(self, tick: dict):
        token, ltp = tick.get("tk"), tick.get("iv", tick.get("ltp"))
        if not token or not ltp:
            return
        try:
            ltp = float(ltp)
        except (TypeError, ValueError):
            return
        syn = self.by_token(str(token))
        if not syn:
            return
        strategy, status = syn.get("strategy"), syn.get("status")
        target = float(syn["target_price"])
        side = syn.get("side", "B")

        if strategy == "market_exit" and status == "ACTIVE":
            hit = ltp >= target if side == "B" else ltp <= target
            if hit:
                log.info("target hit %s @ %s — market exit", syn["trading_symbol"], ltp)
                syn["status"] = "CLOSING"
                try:
                    self.broker.cancel(syn["sl_order_id"])
                except Exception as e:
                    log.error("SL cancel failed: %s", e)
                    syn["status"] = "ERRORED"

        elif strategy == "trailing_stop":
            if status == "ACTIVE":
                hit = ltp >= target if side == "B" else ltp <= target
                if hit:
                    mark = ltp
                    syn.update(status="TRAILING", high_water_mark=mark,
                               low_water_mark=mark)
                    self._trail(syn, ltp)
            elif status == "TRAILING":
                self._trail(syn, ltp)

    def _trail(self, syn: dict, ltp: float):
        side = syn.get("side", "B")
        sl_txn = "B" if side == "S" else "S"
        if side == "S":
            # short: track falling lows, SL trails down but stays above price
            low = float(syn.get("low_water_mark", ltp))
            if ltp >= low:
                return
            syn["low_water_mark"] = ltp
            syn["high_water_mark"] = ltp
            new_sl = ltp + float(syn["trail_distance"])
            if new_sl < float(syn["stop_loss_price"]):
                log.info("trail %s: SL %s -> %.2f", syn["trading_symbol"], syn["stop_loss_price"], new_sl)
                try:
                    self.broker.modify_sl(syn["sl_order_id"], syn["quantity"], f"{new_sl:.2f}",
                                          transaction_type=sl_txn)
                    syn["stop_loss_price"] = new_sl
                except Exception as e:
                    log.error("trail modify failed: %s", e)
            return
        if ltp <= float(syn.get("high_water_mark", 0)):
            return
        syn["high_water_mark"] = ltp
        new_sl = ltp - float(syn["trail_distance"])
        if new_sl > float(syn["stop_loss_price"]):
            log.info("trail %s: SL %s -> %.2f", syn["trading_symbol"], syn["stop_loss_price"], new_sl)
            try:
                self.broker.modify_sl(syn["sl_order_id"], syn["quantity"], f"{new_sl:.2f}",
                                      transaction_type=sl_txn)
                syn["stop_loss_price"] = new_sl
            except Exception as e:
                log.error("trail modify failed: %s", e)
