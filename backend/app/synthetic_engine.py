"""Synthetic bracket engine — port of TransTrader controller logic.

Lifecycle per leg: PENDING_ENTRY -> ACTIVE -> {CLOSING -> CLOSED | TRAILING -> CLOSED} | ERRORED
  - entry fill  -> auto-place SL sell
  - LTP >= target + strategy=market_exit -> cancel SL, market-exit
  - strategy=trailing_stop -> on target, ratchet SL up by trail_distance on new highs
Feeds: on_order_update(order_details) from Kotak order socket; on_tick(tick) from price socket.
"""

import logging

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

    # -- order feed --
    def on_order_update(self, od: dict):
        oid = od.get("nOrdNo")
        status = str(od.get("ordSt", "")).lower()
        syn = self.by_any_id(oid)
        if not syn:
            return
        log.info("synthetic %s: %s -> %s (%s)", syn["trading_symbol"], oid, status, syn["status"])

        if oid == syn["entry_order_id"] and status == "complete" and syn["status"] == "PENDING_ENTRY":
            try:
                r = self.broker.place_stop_loss(syn["exchange_segment"], syn["quantity"],
                                                syn["trading_symbol"], syn["stop_loss_price"])
                if r and r.get("stCode") == 200:
                    syn.update(status="ACTIVE", sl_order_id=r.get("nOrdNo"))
                else:
                    syn["status"] = "ERRORED"
            except Exception as e:
                log.error("SL place failed: %s", e)
                syn["status"] = "ERRORED"

        elif oid == syn.get("sl_order_id") and status == "complete" and syn["status"] != "CLOSED":
            syn["status"] = "CLOSED"

        elif oid == syn.get("sl_order_id") and status == "cancelled" and syn["status"] == "CLOSING":
            try:
                r = self.broker.place_market_exit(syn["exchange_segment"], syn["quantity"], syn["trading_symbol"])
                if not (r and r.get("stCode") == 200):
                    syn["status"] = "ERRORED"
            except Exception as e:
                log.error("market exit failed: %s", e)
                syn["status"] = "ERRORED"

        elif status == "complete" and syn["status"] == "CLOSING":
            syn["status"] = "CLOSED"

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

        if strategy == "market_exit" and status == "ACTIVE" and ltp >= target:
            log.info("target hit %s @ %s — market exit", syn["trading_symbol"], ltp)
            syn["status"] = "CLOSING"
            try:
                self.broker.cancel(syn["sl_order_id"])
            except Exception as e:
                log.error("SL cancel failed: %s", e)
                syn["status"] = "ERRORED"

        elif strategy == "trailing_stop":
            if status == "ACTIVE" and ltp >= target:
                syn.update(status="TRAILING", high_water_mark=ltp)
                self._trail(syn, ltp)
            elif status == "TRAILING":
                self._trail(syn, ltp)

    def _trail(self, syn: dict, ltp: float):
        if ltp <= float(syn.get("high_water_mark", 0)):
            return
        syn["high_water_mark"] = ltp
        new_sl = ltp - float(syn["trail_distance"])
        if new_sl > float(syn["stop_loss_price"]):
            log.info("trail %s: SL %s -> %.2f", syn["trading_symbol"], syn["stop_loss_price"], new_sl)
            try:
                self.broker.modify_sl(syn["sl_order_id"], f"{new_sl:.2f}")
                syn["stop_loss_price"] = new_sl
            except Exception as e:
                log.error("trail modify failed: %s", e)
