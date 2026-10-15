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
            if o.get("sl_order_id") == oid or o.get("exit_order_id") == oid:
                return o
        return None

    def by_token(self, token: str) -> dict | None:
        legs = self.by_tokens(str(token))
        return legs[0] if legs else None

    def by_tokens(self, token: str) -> list[dict]:
        """ALL working legs on an instrument token. Two entries on the same
        strike share one token — every leg must be evaluated per tick, not
        just the first found."""
        return [o for o in self.orders.values()
                if o.get("instrument_token") == str(token)
                and o.get("status") in ("ACTIVE", "TRAILING")]

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

    @staticmethod
    def _fill_qty(od: dict) -> float | None:
        """Filled quantity from a feed/book frame, or None when absent/zero.

        Used to place the SL for the ACTUAL fill size (Plan 04 H3): a partial
        fill must never get a full-size protective SL.
        """
        for k in ("fldQty", "filled_quantity", "filledQty", "flQty"):
            v = od.get(k)
            if v in (None, ""):
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if f > 0:
                return f
        return None

    def on_order_update(self, od: dict):
        oid = od.get("nOrdNo")
        status = str(od.get("ordSt", "")).lower()
        syn = self.by_any_id(oid)
        if not syn:
            return
        t_now = int(time.time() * 1000)
        log.info("synthetic %s: %s -> %s (%s)", syn["trading_symbol"], oid, status, syn["status"])

        if oid == syn["entry_order_id"] and status == "complete" \
                and syn["status"] in ("PENDING_ENTRY", "ACTIVE", "TRAILING"):
            # ACTIVE/TRAILING included so a later fill (partial entry whose
            # remainder was left working) can resize the existing SL.
            self._place_sl(syn, od)

        elif oid == syn["entry_order_id"] and syn["status"] == "PENDING_ENTRY" and "reject" in status:
            # Entry never reached the book — no protective SL to place.
            syn["status"] = "ERRORED"
            syn["error"] = f"entry rejected: {od.get('rejRsn', '')}"
            log.error("entry %s (%s) rejected: %s", syn["trading_symbol"], oid, od.get("rejRsn", ""))

        elif oid == syn["entry_order_id"] and syn["status"] == "PENDING_ENTRY" \
                and status in ("cancelled", "canceled", "expired"):
            syn["status"] = "CLOSED"
            syn["exit_reason"] = "ENTRY_" + status.upper()

        elif oid == syn.get("sl_order_id") and status == "complete" and syn["status"] != "CLOSED":
            syn["status"] = "CLOSED"
            syn["closed_at_ms"] = t_now

        elif oid == syn.get("sl_order_id") and status == "cancelled" and syn["status"] == "CLOSING":
            # Kotak sends 'cancelled' for the SL more than once (confirm +
            # status snapshot). Only the first may fire the market exit —
            # otherwise we double-exit (flip the position!) or mark ERRORED
            # on a harmless duplicate.
            if syn.get("exit_order_id"):
                log.info("duplicate SL cancel for %s ignored (exit %s already placed)",
                         syn["trading_symbol"], syn.get("exit_order_id"))
                return
            try:
                side = syn.get("side", "B")
                exit_txn = "B" if side == "S" else "S"
                r = self.broker.place_market_exit(syn["exchange_segment"], syn["quantity"],
                                                  syn["trading_symbol"],
                                                  transaction_type=exit_txn,
                                                  product=syn.get("product", "MIS"))
                if r and r.get("stCode") == 200 and r.get("nOrdNo"):
                    syn["exit_order_id"] = r.get("nOrdNo")
                else:
                    syn["status"] = "ERRORED"
                    syn["error"] = f"market exit rejected: {str(r)[:300]}"
                    log.error("market exit rejected for %s: %s", syn["trading_symbol"], r)
            except Exception as e:
                log.error("market exit failed: %s", e)
                syn["status"] = "ERRORED"
                syn["error"] = str(e)[:300]

        elif oid == syn.get("exit_order_id"):
            if status == "complete":
                syn["status"] = "CLOSED"
                syn["closed_at_ms"] = t_now
            elif "reject" in status:
                syn["status"] = "ERRORED"
                syn["error"] = f"exit {oid} rejected: {od.get('rejRsn', '')}"
                log.error("exit order %s (%s) rejected: %s",
                          oid, syn["trading_symbol"], od.get("rejRsn", ""))

        elif status == "complete" and syn["status"] == "CLOSING":
            syn["status"] = "CLOSED"
            syn["closed_at_ms"] = t_now

    def _place_sl(self, syn: dict, od: dict):
        """Place the protective SL for a filled entry, exactly once.

        Strict fill gate (Plan 04 H3): the SL is placed ONLY when the entry is
        genuinely filled — a positive average fill price must be available, from
        the feed frame or, if absent, fetched once via the broker's order
        history; it is never guessed from the planned entry. The SL is sized to
        the ACTUAL filled quantity. On a partial fill the SL covers the filled
        qty and the unfilled remainder is left working (documented choice); if
        that remainder later fills, the SL is resized to cover it.

        Idempotent: once `sl_placed`/`sl_order_id` is set a re-driven event
        (duplicate feed frame or Plan 03 reconcile) only ever resizes, never
        double-places, so an entry fill that arrived during a feed gap can never
        leave two live SLs.

        Recomputes target/SL from the ACTUAL fill price, not the planned entry
        (ref_ltp+5). A planned stop can end up on the wrong side of the market
        (e.g. fill lower than planned -> SELL SL trigger above LTP), which the
        exchange rejects as 'STOP PRICE IS NOT REASONABLE'.
        """
        side = syn.get("side", "B")
        sl_txn = "B" if side == "S" else "S"
        try:
            ordered = float(syn.get("quantity") or 0)
        except (TypeError, ValueError):
            ordered = 0.0
        filled = self._fill_qty(od)

        if syn.get("sl_placed") or syn.get("sl_order_id"):
            # Already protected. Only react if the fill grew (a partial entry's
            # remainder filled) — resize the SL so the added qty isn't left naked.
            try:
                covered = float(syn.get("sl_quantity") or 0)
            except (TypeError, ValueError):
                covered = 0.0
            if syn.get("sl_order_id") and filled is not None and filled > covered:
                try:
                    self.broker.modify_sl(syn["sl_order_id"], str(int(filled)),
                                          syn["stop_loss_price"], transaction_type=sl_txn)
                    syn["quantity"] = str(int(filled))
                    syn["sl_quantity"] = filled
                    log.info("SL %s resized to %s for %s (remainder filled)",
                             syn["sl_order_id"], int(filled), syn["trading_symbol"])
                except Exception as e:
                    log.error("SL resize failed for %s: %s", syn["sl_order_id"], e)
            return

        # -- strict fill gate: a real price or nothing --
        fill = self._fill_price(od)
        if fill is None:
            try:
                fill = self.broker.average_fill_price(str(syn.get("entry_order_id")))
            except Exception as e:
                log.error("order history fill price for %s failed: %s",
                          syn.get("entry_order_id"), e)
                fill = None
        try:
            fill = float(fill) if fill is not None else None
        except (TypeError, ValueError):
            fill = None
        if not fill or fill <= 0:
            syn["status"] = "ERRORED"
            syn["error"] = "entry complete but no fill price (feed + order history) — SL not placed"
            log.error("entry %s (%s) complete with no fill price — NO SL placed",
                      syn["trading_symbol"], syn.get("entry_order_id"))
            return

        if filled is None or filled <= 0:
            # 'complete' means the order is fully executed, so a missing fldQty
            # on the terminal frame is trusted as a full fill.
            filled = ordered
        if ordered and 0 < filled < ordered:
            log.warning("partial fill for %s (%s): %s of %s filled — SL sized to "
                        "the fill, remainder left working", syn["trading_symbol"],
                        syn.get("entry_order_id"), int(filled), int(ordered))

        syn["sl_placed"] = True
        t_now = int(time.time() * 1000)
        try:
            # stored distances (preferred) — fall back to deriving from plan
            try:
                target_dist = float(syn.get("target_points", 0) or 0)
            except (TypeError, ValueError):
                target_dist = 0.0
            try:
                stop_dist = float(syn.get("stop_points", 0) or 0)
            except (TypeError, ValueError):
                stop_dist = 0.0
            if target_dist and stop_dist:
                if side == "S":  # short: profit when price falls
                    target = max(0.05, fill - target_dist)
                    stop = fill + stop_dist
                else:
                    target = fill + target_dist
                    stop = max(0.05, fill - stop_dist)
                syn["target_price"] = f"{target:.2f}"
                syn["stop_loss_price"] = f"{stop:.2f}"
            syn["fill_price"] = f"{fill:.2f}"
            sl_qty = str(int(filled)) if filled and filled > 0 else syn["quantity"]
            r = self.broker.place_stop_loss(syn["exchange_segment"], sl_qty,
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
                           quantity=sl_qty, sl_quantity=filled,
                           fill_at_ms=t_now, sl_placed_at_ms=t_sl)
                if fill_lat is not None and fill_lat >= 0:
                    syn["fill_latency_ms"] = fill_lat
            else:
                syn["status"] = "ERRORED"
                syn["error"] = str(r)[:300]
                log.error("SL placement rejected for %s: %s", syn["trading_symbol"], r)
        except Exception as e:
            log.error("SL place failed: %s", e)
            syn["status"] = "ERRORED"
            syn["error"] = str(e)[:300]

    def reconcile(self, book: list[dict], positions: dict | None = None) -> dict:
        """Re-drive tracked legs against a fresh broker order book (Plan 03).

        Called after every order-feed (re)connect so a fill/reject/SL event that
        landed while the feed was down is not silently lost. Only legs still
        awaiting a decision are driven, and SL placement is guarded (see
        `_place_sl`), so running this on top of an already-processed fill is a
        no-op. `book` is a list of TransTrader-shaped order dicts (see
        `sfeed_bridge.map_book_entry`); `positions` is accepted for a UI
        snapshot but is not used to auto-re-place SLs.

        Returns a summary dict for the UI reconcile notice.
        """
        by_id: dict[str, dict] = {}
        for od in book or []:
            oid = od.get("nOrdNo")
            if oid:
                by_id[str(oid)] = od
        driven: list[str] = []
        errored: list[str] = []
        for syn in list(self.orders.values()):
            status = syn.get("status")
            try:
                if status == "PENDING_ENTRY":
                    eid = str(syn.get("entry_order_id") or "")
                    od = by_id.get(eid)
                    if od:
                        self.on_order_update(od)
                        driven.append(eid)
                elif status in ("ACTIVE", "TRAILING"):
                    sl = str(syn.get("sl_order_id") or "")
                    od = by_id.get(sl) if sl else None
                    if od is None:
                        syn["status"] = "ERRORED"
                        syn["error"] = "SL order missing from broker book on reconcile"
                        errored.append(sl)
                        log.error("reconcile: %s leg %s SL %s missing -> ERRORED",
                                  syn.get("trading_symbol"), syn.get("entry_order_id"), sl)
                    else:
                        st = str(od.get("ordSt", "")).lower()
                        if st == "complete":
                            self.on_order_update(od)  # position exited via SL
                            driven.append(sl)
                        elif "reject" in st or st in ("cancelled", "canceled", "expired"):
                            syn["status"] = "ERRORED"
                            syn["error"] = f"SL {sl} {st} on reconcile — position unprotected"
                            errored.append(sl)
                            log.error("reconcile: %s SL %s %s -> ERRORED (naked position)",
                                      syn.get("trading_symbol"), sl, st)
                elif status == "CLOSING":
                    xid = str(syn.get("exit_order_id") or "")
                    od = by_id.get(xid) if xid else None
                    if od:
                        self.on_order_update(od)
                        driven.append(xid)
            except Exception as e:
                log.error("reconcile leg err: %s", e)
        return {"checked": len(self.orders), "driven": driven, "errored": errored,
                "positions": len(positions or {})}

    # -- price feed --
    def on_tick(self, tick: dict):
        token, ltp = tick.get("tk"), tick.get("iv", tick.get("ltp"))
        if not token or not ltp:
            return
        try:
            ltp = float(ltp)
        except (TypeError, ValueError):
            return
        for syn in self.by_tokens(str(token)):
            self._eval_leg(syn, ltp)

    def _eval_leg(self, syn: dict, ltp: float):
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
