"""Synthetic bracket engine — port of TransTrader controller logic.

Lifecycle per leg: PENDING_ENTRY -> ACTIVE -> {CLOSING -> CLOSED | TRAILING -> CLOSED} | ERRORED
  - entry fill  -> auto-place SL sell
  - LTP >= target + strategy=market_exit -> cancel SL, market-exit
  - strategy=trailing_stop -> on target, ratchet SL up by trail_distance on new highs
Feeds: on_order_update(order_details) from Kotak order socket; on_tick(tick) from price socket.
"""

import copy
import logging
import threading
import time

from .config import settings

log = logging.getLogger("synthetic")


class SyntheticEngine:
    def __init__(self, broker):
        self.broker = broker  # KotakService
        self.orders: dict[str, dict] = {}  # entry_order_id -> tracker
        # Every public mutation/read takes this lock. The bridge serializes all
        # of them onto one worker thread; the lock additionally protects direct
        # FastAPI-thread reads and legacy/direct callers. Reads return copies so
        # callers can never mutate a live tracker by accident.
        self._lock = threading.RLock()

    # -- CRUD --
    def add(self, d: dict):
        with self._lock:
            self.orders[d["entry_order_id"]] = copy.deepcopy(d)

    def _by_any_id_locked(self, oid: str) -> dict | None:
        if oid in self.orders:
            return self.orders[oid]
        for o in self.orders.values():
            if o.get("sl_order_id") == oid or o.get("exit_order_id") == oid:
                return o
        return None

    def by_any_id(self, oid: str) -> dict | None:
        with self._lock:
            o = self._by_any_id_locked(oid)
            return copy.deepcopy(o) if o is not None else None

    def by_token(self, token: str) -> dict | None:
        legs = self.by_tokens(str(token))
        return legs[0] if legs else None

    def _by_tokens_locked(self, token: str) -> list[dict]:
        return [o for o in self.orders.values()
                if o.get("instrument_token") == str(token)
                and o.get("status") in ("ACTIVE", "TRAILING")]

    def by_tokens(self, token: str) -> list[dict]:
        """ALL working legs on an instrument token. Two entries on the same
        strike share one token — every leg must be evaluated per tick, not
        just the first found."""
        with self._lock:
            return [copy.deepcopy(o) for o in self._by_tokens_locked(str(token))]

    def all(self) -> list[dict]:
        with self._lock:
            return [copy.deepcopy(o) for o in self.orders.values()]

    def _legs_for_symbol_locked(self, symbol: str) -> list[dict]:
        return [o for o in self.orders.values()
                if o.get("trading_symbol") == symbol
                and o.get("status") in ("PENDING_ENTRY", "ACTIVE", "TRAILING", "CLOSING", "ERRORED")]

    def legs_for_symbol(self, symbol: str) -> list[dict]:
        with self._lock:
            return [copy.deepcopy(o) for o in self._legs_for_symbol_locked(symbol)]

    def mark_manual_exit(self, symbol: str, reason: str = "MANUAL_EXIT") -> list[dict]:
        """Close all open trackers for a symbol (manual flatten from Positions).
        The target is purely synthetic (no exchange order), so only live
        entry/SL exchange orders need cancelling by the caller."""
        with self._lock:
            out = self._legs_for_symbol_locked(symbol)
            for o in out:
                o["status"] = "CLOSED"
                o["exit_reason"] = reason
            return [copy.deepcopy(o) for o in out]

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
        """Apply one order frame; broker I/O happens outside `_lock`."""
        with self._lock:
            action = self._on_order_update_locked(od)
        if action is None:
            return
        kind, syn, payload = action
        if kind == "place_sl":
            self._place_sl(syn, payload)
        elif kind == "market_exit":
            self._place_exit(syn, payload)

    def _on_order_update_locked(self, od: dict):
        oid = od.get("nOrdNo")
        status = str(od.get("ordSt", "")).lower()
        syn = self._by_any_id_locked(oid)
        if not syn:
            return None
        t_now = int(time.time() * 1000)
        log.info("synthetic %s: %s -> %s (%s)", syn["trading_symbol"], oid, status, syn["status"])

        if oid == syn["entry_order_id"] and status == "complete" \
                and syn["status"] in ("PENDING_ENTRY", "ACTIVE", "TRAILING"):
            # ACTIVE/TRAILING included so a later fill (partial entry whose
            # remainder was left working) can resize the existing SL.
            return ("place_sl", syn, od)

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
                return None
            side = syn.get("side", "B")
            return ("market_exit", syn, "B" if side == "S" else "S")

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
        return None

    def _place_exit(self, syn: dict, exit_txn: str):
        """Place a market exit outside `_lock`, then commit the result."""
        with self._lock:
            args = (syn["exchange_segment"], syn["quantity"],
                    syn["trading_symbol"])
            sym, product = syn["trading_symbol"], syn.get("product", "MIS")
        try:
            r = self.broker.place_market_exit(
                *args, transaction_type=exit_txn, product=product)
            ok = bool(r and r.get("stCode") == 200 and r.get("nOrdNo"))
            err = f"market exit rejected: {str(r)[:300]}"
        except Exception as e:
            r, ok, err = None, False, str(e)[:300]
            log.error("market exit failed: %s", e)
        with self._lock:
            if ok:
                syn["exit_order_id"] = r.get("nOrdNo")
            else:
                syn["status"] = "ERRORED"
                syn["error"] = err
        if not ok:
            log.error("market exit rejected for %s: %s", sym, r)

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
        with self._lock:
            side = syn.get("side", "B")
            sl_txn = "B" if side == "S" else "S"
            try:
                ordered = float(syn.get("quantity") or 0)
            except (TypeError, ValueError):
                ordered = 0.0
            filled = self._fill_qty(od)
            sym = syn["trading_symbol"]
            entry_id = syn.get("entry_order_id")
            if syn.get("sl_placed") or syn.get("sl_order_id"):
                # Already protected. Only react if the fill grew (a partial
                # entry's remainder filled) — resize the SL so the added qty
                # isn't left naked.
                try:
                    covered = float(syn.get("sl_quantity") or 0)
                except (TypeError, ValueError):
                    covered = 0.0
                sl_id = syn.get("sl_order_id")
                if not (sl_id and filled is not None and filled > covered):
                    return
                stop = syn["stop_loss_price"]
                resize = (sl_id, str(int(filled)), stop)
            else:
                if syn.get("_sl_placing"):
                    return
                syn["_sl_placing"] = True
                resize = None

        if resize is not None:
            sl_id, new_qty, stop = resize
            try:
                self.broker.modify_sl(sl_id, new_qty, stop, transaction_type=sl_txn)
            except Exception as e:
                log.error("SL resize failed for %s: %s", sl_id, e)
                return
            with self._lock:
                syn["quantity"] = new_qty
                syn["sl_quantity"] = filled
            log.info("SL %s resized to %s for %s (remainder filled)",
                     sl_id, new_qty, sym)
            return

        # -- strict fill gate: a real price or nothing --
        fill = self._fill_price(od)
        if fill is None:
            try:
                fill = self.broker.average_fill_price(str(entry_id))
            except Exception as e:
                log.error("order history fill price for %s failed: %s", entry_id, e)
                fill = None
        try:
            fill = float(fill) if fill is not None else None
        except (TypeError, ValueError):
            fill = None
        if not fill or fill <= 0:
            with self._lock:
                syn["status"] = "ERRORED"
                syn["error"] = ("entry complete but no fill price "
                                "(feed + order history) — SL not placed")
                syn.pop("_sl_placing", None)
            log.error("entry %s (%s) complete with no fill price — NO SL placed",
                      sym, entry_id)
            return

        if filled is None or filled <= 0:
            # 'complete' means the order is fully executed, so a missing fldQty
            # on the terminal frame is trusted as a full fill.
            filled = ordered
        if ordered and 0 < filled < ordered:
            log.warning("partial fill for %s (%s): %s of %s filled — SL sized to "
                        "the fill, remainder left working", sym, entry_id,
                        int(filled), int(ordered))

        with self._lock:
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
                    stop_price = fill + stop_dist
                else:
                    target = fill + target_dist
                    stop_price = max(0.05, fill - stop_dist)
                syn["target_price"] = f"{target:.2f}"
                syn["stop_loss_price"] = f"{stop_price:.2f}"
            syn["fill_price"] = f"{fill:.2f}"
            sl_qty = str(int(filled)) if filled and filled > 0 else syn["quantity"]
            exchange = syn["exchange_segment"]
            product = syn.get("product", "MIS")
            stop_price = syn["stop_loss_price"]

        t_now = int(time.time() * 1000)
        try:
            r = self.broker.place_stop_loss(exchange, sl_qty, sym, stop_price,
                                            transaction_type=sl_txn,
                                            product=product)
            t_sl = int(time.time() * 1000)
            ok = bool(r and r.get("stCode") == 200)
            err = str(r)[:300]
        except Exception as e:
            r, t_sl, ok, err = None, int(time.time() * 1000), False, str(e)[:300]
            log.error("SL place failed: %s", e)
        with self._lock:
            if ok:
                placed = syn.get("placed_at_ms")
                try:
                    fill_lat = t_now - int(placed) if placed else None
                except (TypeError, ValueError):
                    fill_lat = None
                syn.update(status="ACTIVE", sl_order_id=r.get("nOrdNo"),
                           quantity=sl_qty, sl_quantity=filled,
                           fill_at_ms=t_now, sl_placed_at_ms=t_sl,
                           sl_placed=True, sl_last_seen_ms=t_sl,
                           sl_missing_streak=0)
                if fill_lat is not None and fill_lat >= 0:
                    syn["fill_latency_ms"] = fill_lat
            else:
                syn["status"] = "ERRORED"
                syn["error"] = err
            syn.pop("_sl_placing", None)
        if not ok:
            log.error("SL placement rejected for %s: %s", sym, r)

    def reconcile(self, book: list[dict], positions: dict | None = None) -> dict:
        # Snapshot actual trackers under the lock, then drive them outside it:
        # reconcile may place an SL (broker I/O) and must not block health reads.
        with self._lock:
            legs = list(self.orders.values())
        return self._reconcile(legs, book, positions)

    def _reconcile(self, legs: list[dict], book: list[dict],
                   positions: dict | None = None) -> dict:
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
        naked: list[str] = []
        now = int(time.time() * 1000)
        for syn in legs:
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
                        with self._lock:
                            streak = int(syn.get("sl_missing_streak") or 0) + 1
                            syn["sl_missing_streak"] = streak
                            seen = int(syn.get("sl_last_seen_ms") or now)
                            syn["sl_last_seen_ms"] = seen
                        # Never declare a leg naked from a single empty/book-lag
                        # snapshot; require N consecutive misses AND a grace
                        # window since the SL was last observed.
                        if (streak >= settings.sl_missing_confirmations
                                and now - seen >= settings.sl_visibility_grace_sec * 1000):
                            with self._lock:
                                if syn.get("status") in ("ACTIVE", "TRAILING"):
                                    syn["status"] = "ERRORED"
                                    syn["error"] = ("SL order missing from broker book "
                                                    f"for {streak} reconciles")
                            errored.append(sl)
                            naked.append(sl)
                            log.error("reconcile: %s leg %s SL %s missing for %d checks "
                                      "-> ERRORED", syn.get("trading_symbol"),
                                      syn.get("entry_order_id"), sl, streak)
                        else:
                            log.warning("reconcile: %s SL %s not in book yet "
                                        "(miss %d/%d, grace %ss) — keeping leg %s",
                                        syn.get("trading_symbol"), sl, streak,
                                        settings.sl_missing_confirmations,
                                        settings.sl_visibility_grace_sec, status)
                    else:
                        with self._lock:
                            syn["sl_missing_streak"] = 0
                            syn["sl_last_seen_ms"] = now
                        st = str(od.get("ordSt", "")).lower()
                        if st == "complete":
                            self.on_order_update(od)  # position exited via SL
                            driven.append(sl)
                        elif "reject" in st or st in ("cancelled", "canceled", "expired"):
                            with self._lock:
                                syn["status"] = "ERRORED"
                                syn["error"] = (f"SL {sl} {st} on reconcile — "
                                                "position unprotected")
                            errored.append(sl)
                            naked.append(sl)
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
        return {"checked": len(legs), "driven": driven, "repaired": [],
                "errored": errored, "naked": naked,
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
        with self._lock:
            legs = self._by_tokens_locked(str(token))
        # Broker I/O (cancel/trail) happens per leg outside the lock.
        for syn in legs:
            self._eval_leg(syn, ltp)

    def _eval_leg(self, syn: dict, ltp: float):
        cancel_id = None
        trail = False
        with self._lock:
            strategy, status = syn.get("strategy"), syn.get("status")
            if not syn.get("target_price"):
                return
            target = float(syn["target_price"])
            side = syn.get("side", "B")

            if strategy == "market_exit" and status == "ACTIVE":
                hit = ltp >= target if side == "B" else ltp <= target
                if hit:
                    log.info("target hit %s @ %s — market exit",
                             syn["trading_symbol"], ltp)
                    syn["status"] = "CLOSING"
                    cancel_id = syn.get("sl_order_id")
            elif strategy == "trailing_stop":
                if status == "ACTIVE":
                    hit = ltp >= target if side == "B" else ltp <= target
                    if hit:
                        syn.update(status="TRAILING", high_water_mark=ltp,
                                   low_water_mark=ltp)
                        trail = True
                elif status == "TRAILING":
                    trail = True

        if cancel_id is not None:
            try:
                self.broker.cancel(cancel_id)
            except Exception as e:
                log.error("SL cancel failed: %s", e)
                with self._lock:
                    if syn.get("status") == "CLOSING":
                        syn["status"] = "ERRORED"
                        syn["error"] = f"SL cancel failed: {str(e)[:200]}"
        if trail:
            self._trail(syn, ltp)

    def _trail(self, syn: dict, ltp: float):
        update = None
        with self._lock:
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
                    update = (syn.get("sl_order_id"), syn.get("quantity"),
                              f"{new_sl:.2f}", sl_txn, new_sl)
            else:
                if ltp <= float(syn.get("high_water_mark", 0)):
                    return
                syn["high_water_mark"] = ltp
                new_sl = ltp - float(syn["trail_distance"])
                if new_sl > float(syn["stop_loss_price"]):
                    update = (syn.get("sl_order_id"), syn.get("quantity"),
                              f"{new_sl:.2f}", sl_txn, new_sl)
            if update is not None:
                log.info("trail %s: SL %s -> %s", syn["trading_symbol"],
                         syn["stop_loss_price"], update[2])
        if update is None:
            return
        sl_id, qty, price, sl_txn, new_sl = update
        try:
            self.broker.modify_sl(sl_id, qty, price, transaction_type=sl_txn)
        except Exception as e:
            log.error("trail modify failed: %s", e)
            return
        with self._lock:
            syn["stop_loss_price"] = new_sl
