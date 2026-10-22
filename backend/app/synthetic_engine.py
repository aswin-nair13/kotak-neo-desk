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
    def __init__(self, broker, book_fetcher=None, sleep_fn=None, clock_fn=None):
        self.broker = broker  # KotakService
        self.orders: dict[str, dict] = {}  # entry_order_id -> tracker
        # Worker-safe order-book provider: returns (mapped_book, ok). Wired to
        # SFeedBridge.fetch_book in production, so cancel confirmation reads the
        # broker book on the single-writer worker and never on the feed loop.
        self.book_fetcher = book_fetcher
        # Exit confirmation is a non-blocking state machine (Plan 06 M3). After
        # each step the engine asks the bridge to queue ONE follow-up reconcile;
        # the engine itself never sleeps/polls on the single-writer worker.
        self.exit_poll_requester = None
        # Kept for backwards-compatible construction; no exit path sleeps now.
        self._sleep = sleep_fn or time.sleep
        self._clock = clock_fn or time.monotonic
        # Every public mutation/read takes this lock. The bridge serializes all
        # of them onto one worker thread; the lock additionally protects direct
        # FastAPI-thread reads and legacy/direct callers. Reads return copies so
        # callers can never mutate a live tracker by accident.
        self._lock = threading.RLock()

    # -- CRUD --
    def add(self, d: dict):
        with self._lock:
            d = copy.deepcopy(d)
            # Plan 06 WP3: the SL guarantee clock starts when the entry is
            # accepted/registered, not when the feed eventually reports a fill.
            if d.get("sl_guarantee_due_ms") is None:
                d["sl_guarantee_due_ms"] = (
                    int(time.time() * 1000)
                    + int(max(0.0, settings.sl_guarantee_sec) * 1000))
            d.setdefault("last_reconcile_ms", None)
            d.setdefault("naked_position", False)
            d.setdefault("blocked_reason", None)
            d.setdefault("exit_pending", False)
            d.setdefault("exit_state", None)
            d.setdefault("exit_ambiguous", False)
            d.setdefault("exit_unknown_streak", 0)
            self.orders[d["entry_order_id"]] = d

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
        elif kind == "cancel_exit":
            # The SL completed while our market exit was already working: the
            # position is closed, so try to stop the (now unwanted) exit order
            # instead of leaving it free to flip the position.
            try:
                self.broker.cancel(payload)
            except Exception as e:  # noqa: BLE001 - best effort, alert below
                log.error("late exit cancel failed for %s (%s): %s",
                          syn.get("trading_symbol"), payload, e)

    @staticmethod
    def _is_reject(st: str) -> bool:
        return "reject" in st

    @staticmethod
    def _is_cancel(st: str) -> bool:
        return st in ("cancelled", "canceled", "expired")

    @staticmethod
    def _is_complete(st: str) -> bool:
        return st in ("complete", "filled")

    @classmethod
    def _is_open(cls, st: str) -> bool:
        return not (cls._is_complete(st) or cls._is_reject(st)
                    or cls._is_cancel(st))

    def _mark_naked(self, syn: dict, reason: str, error: str):
        """ERRORED + explicit naked-position alert (log ERROR + API state)."""
        with self._lock:
            syn["status"] = "ERRORED"
            syn["error"] = error
            syn["naked_position"] = True
            syn["blocked_reason"] = reason
            syn["naked_at_ms"] = int(time.time() * 1000)
        log.error("NAKED POSITION %s entry=%s sl=%s: %s",
                  syn.get("trading_symbol"), syn.get("entry_order_id"),
                  syn.get("sl_order_id"), error)

    def _on_order_update_locked(self, od: dict):
        oid = od.get("nOrdNo")
        status = str(od.get("ordSt", "")).lower()
        syn = self._by_any_id_locked(oid)
        if not syn and oid:
            # Plan 06 M1: adopt an exit whose placement response timed out by
            # its deterministic client tag (Kotak echoes it as GuiOrdId).
            tag = self._order_tag(od)
            if tag:
                for o in self.orders.values():
                    if (not o.get("exit_order_id")
                            and str(o.get("exit_client_tag") or "") == tag):
                        if o.get("status") == "CLOSED":
                            # Never attach a late working exit to a closed leg:
                            # the position is flat and the exit would flip it.
                            if self._is_open(status):
                                log.critical(
                                    "LATE EXIT RISK %s entry=%s: ambiguous exit "
                                    "%s (%s) appeared after CLOSED - cancelling",
                                    o.get("trading_symbol"),
                                    o.get("entry_order_id"), oid, status)
                                return ("cancel_exit", o, str(oid))
                            log.warning("late exit %s already %s for CLOSED %s",
                                        oid, status, o.get("entry_order_id"))
                            return None
                        syn = o
                        syn["exit_order_id"] = str(oid)
                        syn["exit_ambiguous"] = False
                        syn["exit_pending"] = True
                        syn["exit_state"] = "EXIT_CONFIRM"
                        syn["naked_position"] = False
                        if syn.get("status") == "ERRORED":
                            syn["status"] = "CLOSING"
                        syn["blocked_reason"] = None
                        syn.pop("error", None)
                        log.warning("EXIT AMBIGUOUS adopted by feed tag %s -> "
                                    "order %s (%s)", tag, oid, syn["trading_symbol"])
                        break
                if not syn:
                    for o in self.orders.values():
                        if (not o.get("sl_order_id") and not o.get("sl_placed")
                                and str(o.get("sl_client_tag") or "") == tag
                                and o.get("status") != "CLOSED"
                                and (self._is_open(status)
                                     or self._is_complete(status))):
                            syn = o
                            syn["sl_order_id"] = str(oid)
                            syn["sl_placed"] = True
                            syn["sl_ambiguous"] = False
                            syn["sl_tag_missing_streak"] = 0
                            syn["naked_position"] = False
                            syn["blocked_reason"] = None
                            syn.pop("error", None)
                            if syn.get("status") == "ERRORED":
                                syn["status"] = syn.get("sl_prev_status") or "ACTIVE"
                            log.warning("SL AMBIGUOUS adopted by feed tag %s -> "
                                        "order %s (%s)", tag, oid,
                                        syn["trading_symbol"])
                            break
        if not syn:
            return None
        t_now = int(time.time() * 1000)
        log.info("synthetic %s: %s -> %s (%s)", syn["trading_symbol"], oid, status, syn["status"])

        entry_complete_ok = (
            syn["status"] in ("PENDING_ENTRY", "ACTIVE", "TRAILING")
            or (syn["status"] == "ERRORED" and not syn.get("sl_placed")
                and syn.get("blocked_reason") in (
                    "ENTRY_REJECTED", "ENTRY_NOT_FILLED_SL_GUARANTEE")))
        if oid == syn["entry_order_id"] and self._is_complete(status) \
                and entry_complete_ok:
            # ACTIVE/TRAILING included so a later fill (partial entry whose
            # remainder was left working) can resize the existing SL. ERRORED
            # is included only for a not-yet-protected entry (late fill frame),
            # never for an exit failure.
            with self._lock:
                syn["sl_guarantee_alerted"] = False
                syn["entry_cancel_pending"] = False
                if syn.get("blocked_reason") == "ENTRY_NOT_FILLED_SL_GUARANTEE":
                    syn["blocked_reason"] = None
            return ("place_sl", syn, od)

        elif oid == syn["entry_order_id"] and syn["status"] == "PENDING_ENTRY" \
                and self._is_reject(status):
            # Entry never reached the book — no protective SL to place.
            syn["status"] = "ERRORED"
            syn["error"] = f"entry rejected: {od.get('rejRsn', '')}"
            syn["blocked_reason"] = "ENTRY_REJECTED"
            syn["entry_cancel_pending"] = False
            log.error("ENTRY REJECTED %s (%s): %s", syn["trading_symbol"], oid,
                      od.get("rejRsn", ""))

        elif oid == syn["entry_order_id"] and syn["status"] == "PENDING_ENTRY" \
                and self._is_cancel(status):
            # Plan 06 M2: a cancel can race a partial fill. Never close the leg
            # and leave the filled part unprotected — size the SL to the fill.
            filled = self._fill_qty(od)
            syn["entry_cancel_pending"] = False
            if filled and filled > 0:
                if syn.get("sl_placed") or syn.get("sl_order_id"):
                    return None
                syn["error"] = ("entry cancelled after a partial fill "
                                f"({int(filled)}) — placing SL for the fill")
                log.error("PARTIAL ENTRY CANCEL %s (%s): %s filled — placing "
                          "protective SL before closing the tracker",
                          syn["trading_symbol"], oid, int(filled))
                return ("place_sl", syn, od)
            syn["status"] = "CLOSED"
            syn["exit_reason"] = "ENTRY_" + status.upper()

        elif oid == syn.get("sl_order_id") and self._is_complete(status) \
                and syn["status"] != "CLOSED":
            # SL hit: the position is closed by the protective order. If a
            # market exit was already working (target race), stop it if we can
            # so it cannot flip the now-flat position.
            late_exit = syn.get("exit_order_id") if syn["status"] == "CLOSING" else None
            syn["status"] = "CLOSED"
            syn["closed_at_ms"] = t_now
            syn["exit_reason"] = "SL_HIT"
            syn["naked_position"] = False
            syn["blocked_reason"] = None
            syn["exit_pending"] = False
            syn["exit_state"] = None
            log.info("SL HIT %s entry=%s sl=%s", syn["trading_symbol"],
                     syn.get("entry_order_id"), oid)
            if late_exit:
                log.error("LATE EXIT RISK %s: SL %s completed after market exit %s "
                          "was placed — attempting to cancel the exit",
                          syn["trading_symbol"], oid, late_exit)
                return ("cancel_exit", syn, late_exit)

        elif oid == syn.get("sl_order_id") and self._is_cancel(status) \
                and syn["status"] == "CLOSING":
            # Kotak sends 'cancelled' for the SL more than once (confirm +
            # status snapshot). Only the first may fire the market exit —
            # otherwise we double-exit (flip the position!) or mark ERRORED
            # on a harmless duplicate. `_place_exit` re-checks the same guard.
            if syn.get("exit_order_id") or syn.get("_exit_placing"):
                log.info("duplicate SL cancel for %s ignored (exit %s already placed)",
                         syn["trading_symbol"], syn.get("exit_order_id"))
                return None
            side = syn.get("side", "B")
            return ("market_exit", syn, "B" if side == "S" else "S")

        elif oid == syn.get("exit_order_id"):
            if syn["status"] == "CLOSED":
                # A delayed update for an old exit must never reopen/attach to
                # a closed leg. Stop it if it is still working.
                if self._is_open(status):
                    log.critical(
                        "LATE EXIT RISK %s entry=%s: exit %s (%s) after CLOSED "
                        "- cancelling", syn["trading_symbol"],
                        syn.get("entry_order_id"), oid, status)
                    return ("cancel_exit", syn, str(oid))
                log.warning("late terminal exit %s (%s) for CLOSED %s ignored",
                            oid, status, syn.get("entry_order_id"))
                return None
            if self._is_complete(status):
                syn["status"] = "CLOSED"
                syn["closed_at_ms"] = t_now
                syn["exit_reason"] = syn.get("exit_reason") or "TARGET_EXIT"
                syn["naked_position"] = False
                syn["blocked_reason"] = None
                syn["exit_pending"] = False
                syn["exit_state"] = None
            elif self._is_reject(status) or self._is_cancel(status):
                # Plan 06 H1: a rejected/cancelled/expired exit is terminal
                # failure, not "pending forever". The SL is already cancelled
                # (it was confirmed before the exit), so the position is naked.
                reason = "EXIT_REJECTED" if self._is_reject(status) else "EXIT_CANCELLED"
                syn["status"] = "ERRORED"
                syn["error"] = f"exit {oid} {status}: {od.get('rejRsn', '')}"
                syn["blocked_reason"] = reason
                syn["exit_pending"] = False
                syn["exit_state"] = None
                syn["naked_position"] = True
                log.error("EXIT %s %s order=%s: %s", status.upper(),
                          syn["trading_symbol"], oid, od.get("rejRsn", ""))
                log.error("NAKED POSITION %s: market exit %s %s",
                          syn["trading_symbol"], oid, status)

        elif self._is_complete(status) and syn["status"] == "CLOSING":
            syn["status"] = "CLOSED"
            syn["closed_at_ms"] = t_now
        return None

    def _read_book(self):
        """(mapped_book, ok) from the injected worker-safe fetcher.

        Without a fetcher (direct/unit use) a real broker.order_book list/dict
        is used; otherwise confirmation is unavailable and the caller falls
        back to the feed `cancelled` path rather than guessing.
        """
        fetcher = self.book_fetcher
        try:
            if fetcher is not None:
                res = fetcher()
                if isinstance(res, tuple) and len(res) == 2:
                    book, ok = res
                else:
                    book, ok = res, True
                return ((book or []), True) if ok else (None, False)
            # No bridge wired (direct/unit use): try a broker order_book if it
            # returns a real list/dict. A bare Mock/unusable shape stays "no
            # confirmation" so legacy feed-driven tests keep their behaviour.
            raw_fn = getattr(self.broker, "order_book", None)
            if not callable(raw_fn):
                return None, False
            raw = raw_fn()
            rows = None
            if isinstance(raw, list):
                rows = raw
            elif isinstance(raw, dict) and isinstance(raw.get("data"), list):
                rows = raw["data"]
            if rows is None:
                return None, False
            from .sfeed_bridge import map_book_entry
            return ([m for m in (map_book_entry(r) for r in rows) if m], True)
        except Exception as e:  # noqa: BLE001 - book failure must not crash exit
            log.error("cancel-confirm order book failed: %s", e)
            return None, False

    @staticmethod
    def _book_entry(book, oid: str | None) -> dict | None:
        if not oid or not book:
            return None
        for od in book:
            if str(od.get("nOrdNo")) == str(oid):
                return od
        return None

    def _mark_sl_hit(self, syn: dict, sl_id: str | None):
        with self._lock:
            syn["status"] = "CLOSED"
            syn["closed_at_ms"] = int(time.time() * 1000)
            syn["exit_reason"] = "SL_HIT"
            syn["naked_position"] = False
            syn["blocked_reason"] = None
        log.info("SL HIT %s entry=%s sl=%s", syn.get("trading_symbol"),
                 syn.get("entry_order_id"), sl_id)

    @staticmethod
    def _order_tag(od: dict) -> str:
        for k in ("guiOrdId", "GuiOrdId", "tag"):
            v = od.get(k)
            if v not in (None, ""):
                return str(v)
        return ""

    @classmethod
    def _book_by_tag(cls, book, tag: str) -> dict | None:
        if not tag or not book:
            return None
        for od in book:
            if cls._order_tag(od) == str(tag):
                return od
        return None

    def _request_exit_poll(self, delay_sec: float = 0.0):
        """Ask the bridge to queue one follow-up reconcile (never blocks)."""
        fn = self.exit_poll_requester
        if fn is None:
            return
        try:
            try:
                fn(max(0.0, float(delay_sec)))
            except TypeError:
                # Older/custom doubles accept only a zero-argument callback.
                fn()
        except Exception as e:  # noqa: BLE001 - polling is best effort
            log.error("exit poll request failed: %s", e)

    def _safe_cancel(self, oid: str, why: str):
        try:
            self.broker.cancel(oid)
        except Exception as e:  # noqa: BLE001 - best effort
            log.error("cancel %s (%s) failed: %s", oid, why, e)

    @staticmethod
    def _new_exit_tag(syn: dict) -> str:
        entry = str(syn.get("entry_order_id") or "E")
        seq = int(syn.get("exit_attempts") or 0) + 1
        # Deterministic per attempt, short enough for the broker's 20-char tag.
        return f"X{entry}-{seq}"[:20]

    @staticmethod
    def _new_sl_tag(syn: dict) -> str:
        entry = str(syn.get("entry_order_id") or "E")
        seq = max(1, int(syn.get("sl_attempts") or 0))
        # Distinct prefix from exit tags; Kotak's tag limit is 20 chars.
        return f"S{entry}-{seq}"[:20]

    def _adopt_sl_by_tag(self, syn: dict, book, book_ok):
        """Adopt a timed-out SL placement by its client tag (never duplicate)."""
        tag = str(syn.get("sl_client_tag") or "")
        if not tag or not book_ok:
            return "unavailable"
        found = self._book_by_tag(book, tag)
        if not found:
            return "absent"
        st = str(found.get("ordSt", "")).lower()
        if not (self._is_complete(st) or self._is_open(st)):
            return "terminal"
        with self._lock:
            syn["sl_order_id"] = str(found.get("nOrdNo") or "")
            syn["sl_placed"] = True
            syn["sl_ambiguous"] = False
            syn["sl_tag_missing_streak"] = 0
            syn["naked_position"] = False
            syn["blocked_reason"] = None
            syn["error"] = None
            syn["status"] = syn.get("sl_prev_status") or "ACTIVE"
            syn["sl_last_seen_ms"] = int(time.time() * 1000)
        log.warning("SL AMBIGUOUS RESOLVED BY TAG %s entry=%s tag=%s -> %s",
                    syn.get("trading_symbol"), syn.get("entry_order_id"), tag,
                    syn.get("sl_order_id"))
        if self._is_complete(st):
            self.on_order_update(found)
            return "closed"
        return "adopted"

    def _leg_invalid(self, syn: dict) -> str | None:
        required = ("entry_order_id", "trading_symbol", "exchange_segment",
                    "quantity")
        missing = [k for k in required if syn.get(k) in (None, "")]
        if not missing:
            try:
                if float(syn.get("quantity")) <= 0:
                    missing.append("quantity")
            except (TypeError, ValueError):
                missing.append("quantity")
        if not missing:
            return None
        with self._lock:
            syn["status"] = "ERRORED"
            syn["blocked_reason"] = "LEG_INVALID"
            syn["error"] = f"missing/invalid tracker fields: {', '.join(missing)}"
            syn["naked_position"] = True
            syn["exit_pending"] = False
            syn["exit_state"] = None
        log.error("LEG INVALID %s entry=%s: %s — no broker action taken",
                  syn.get("trading_symbol"), syn.get("entry_order_id"),
                  ", ".join(missing))
        return "invalid"

    @staticmethod
    def _call_place_with_tag(fn, args, kwargs, tag):
        """Pass the client tag when the broker supports it (legacy fakes omit it)."""
        try:
            return fn(*args, tag=tag, **kwargs)
        except TypeError as e:
            if "tag" not in str(e):
                raise
            return fn(*args, **kwargs)

    def _place_exit(self, syn: dict, exit_txn: str | None = None,
                    force: bool = False) -> str:
        """Place exactly one market exit outside `_lock` (non-blocking).

        Returns: placed | pending | rejected | ambiguous | invalid | closed.
        A deterministic client tag is stored BEFORE the broker call. On a
        timeout/exception the result is ambiguous (never auto-retried); the
        reconcile/force paths first look for that tag in the book so a fill
        that landed after the timeout is adopted instead of duplicated (M1).
        """
        with self._lock:
            if syn.get("status") == "CLOSED":
                return "closed"
            if syn.get("exit_order_id"):
                return "pending"
            if syn.get("_exit_placing"):
                return "pending"
            if not exit_txn:
                side = syn.get("side", "B")
                exit_txn = "B" if side == "S" else "S"
            missing = self._leg_invalid(syn)
            if missing:
                return missing
            tag = str(syn.get("exit_client_tag") or "") or self._new_exit_tag(syn)
            syn["exit_client_tag"] = tag
            syn["exit_attempts"] = int(syn.get("exit_attempts") or 0) + 1
            syn["_exit_placing"] = True
            args = (syn["exchange_segment"], syn["quantity"],
                    syn["trading_symbol"])
            sym, product = syn["trading_symbol"], syn.get("product", "MIS")
            entry_id = syn.get("entry_order_id")
        try:
            r = self._call_place_with_tag(
                self.broker.place_market_exit, args,
                {"transaction_type": exit_txn, "product": product}, tag)
            ok = bool(r and r.get("stCode") == 200 and r.get("nOrdNo"))
            err = f"market exit rejected: {str(r)[:300]}"
        except Exception as e:  # noqa: BLE001 - timeout/reset => ambiguous
            r, ok, err = None, False, str(e)[:300]
            log.error("market exit ambiguous for %s (tag=%s): %s", sym, tag, e)
            with self._lock:
                syn.pop("_exit_placing", None)
                syn["status"] = "ERRORED"
                syn["error"] = f"EXIT_AMBIGUOUS: {err}"
                syn["blocked_reason"] = "EXIT_AMBIGUOUS"
                syn["exit_ambiguous"] = True
                syn["exit_pending"] = False
                syn["exit_state"] = "BLOCKED"
                # The order may be live; do not label it NAKED, but do alert.
                syn["naked_position"] = False
            log.error("EXIT AMBIGUOUS %s entry=%s tag=%s — will resolve by tag "
                      "before any new exit", sym, entry_id, tag)
            return "ambiguous"
        with self._lock:
            syn.pop("_exit_placing", None)
            if ok:
                syn["exit_order_id"] = r.get("nOrdNo")
                syn["exit_placed_at_ms"] = int(time.time() * 1000)
                syn["exit_pending"] = True
                syn["exit_state"] = "EXIT_CONFIRM"
                syn["exit_ambiguous"] = False
                syn["exit_unknown_streak"] = 0
                syn["naked_position"] = False
                syn["blocked_reason"] = None
                syn.pop("error", None)
        if not ok:
            with self._lock:
                syn["status"] = "ERRORED"
                syn["error"] = err
                syn["blocked_reason"] = "EXIT_REJECTED"
                syn["exit_pending"] = False
                syn["exit_state"] = None
                syn["naked_position"] = True
            log.error("NAKED POSITION %s: market exit rejected: %s", sym, r)
            return "rejected"
        log.info("MARKET EXIT placed %s entry=%s exit=%s tag=%s", sym,
                 syn.get("entry_order_id"), r.get("nOrdNo"), tag)
        self._request_exit_poll()
        return "placed"

    def _replace_sl(self, syn: dict, preserve: bool = False) -> bool:
        """Re-place a missing/dead SL once from the stored fill price."""
        with self._lock:
            attempts = int(syn.get("sl_replace_attempts") or 0)
            limit = max(0, int(settings.sl_replace_max_attempts))
            if attempts >= limit:
                return False
            fill = syn.get("fill_price")
            try:
                fill_f = float(fill)
            except (TypeError, ValueError):
                fill_f = 0.0
            if fill_f <= 0:
                return False
            prev = syn.get("sl_order_id")
            filled = syn.get("sl_quantity") or syn.get("quantity")
            syn["sl_replace_attempts"] = attempts + 1
            syn["previous_sl_order_id"] = prev
            # Plan 06 M4: a repaired trailing SL must keep its trailed trigger,
            # status and water marks. Recomputing from stop_points moves the
            # stop backwards (ACTIVE) and gives back locked-in profit.
            if preserve:
                syn["_preserve_sl"] = {
                    "status": (syn.get("sl_prev_status")
                               or syn.get("naked_prev_status")
                               or syn.get("status") or "ACTIVE"),
                    "target_price": syn.get("target_price"),
                    "stop_loss_price": syn.get("stop_loss_price"),
                    "high_water_mark": syn.get("high_water_mark"),
                    "low_water_mark": syn.get("low_water_mark"),
                }
            # Single entry point: clear the old guard so _place_sl performs the
            # strict fill gate and sets sl_order_id/sl_placed atomically.
            syn["sl_placed"] = False
            syn["sl_order_id"] = None
            syn.pop("_sl_placing", None)
            od = {"nOrdNo": syn.get("entry_order_id"), "ordSt": "complete",
                  "prc": f"{fill_f:.2f}", "fldQty": filled}
        log.warning("reconcile: re-placing SL for %s entry=%s (previous %s)",
                    syn.get("trading_symbol"), syn.get("entry_order_id"), prev)
        self._place_sl(syn, od)
        with self._lock:
            ok = bool(syn.get("sl_placed") and syn.get("sl_order_id"))
            # Keep the old id so a book that still shows it can recover the leg
            # on a later reconcile instead of leaving it untracked. Never do
            # this for an ambiguous replacement: its own tag must be adopted.
            if (not ok and prev and not syn.get("sl_order_id")
                    and not syn.get("sl_ambiguous")):
                syn["sl_order_id"] = prev
            return ok

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
                if (syn.get("sl_ambiguous")
                        and not syn.pop("_sl_tag_absent_confirmed", False)):
                    # A timed-out placement may already be live. Only reconcile
                    # may clear this after an authoritative book miss.
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
                syn.pop("_preserve_sl", None)
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
            preserve = syn.pop("_preserve_sl", None)
            if preserve:
                # Repairing a live bracket: keep the current target/SL (and a
                # trailed stop) untouched.
                for k in ("target_price", "stop_loss_price",
                          "high_water_mark", "low_water_mark"):
                    if preserve.get(k) is not None:
                        syn[k] = preserve[k]
            else:
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
            syn["sl_attempts"] = int(syn.get("sl_attempts") or 0) + 1
            tag = self._new_sl_tag(syn)
            syn["sl_client_tag"] = tag
            syn["sl_tag_missing_streak"] = 0
            syn["sl_prev_status"] = ((preserve or {}).get("status")
                                     or (syn.get("status")
                                         if syn.get("status") not in
                                         ("ERRORED", "PENDING_ENTRY")
                                         else "ACTIVE"))
            syn["sl_ambiguous"] = False

        t_now = int(time.time() * 1000)
        ambiguous = False
        try:
            r = self._call_place_with_tag(
                self.broker.place_stop_loss,
                (exchange, sl_qty, sym, stop_price),
                {"transaction_type": sl_txn, "product": product}, tag)
            t_sl = int(time.time() * 1000)
            ok = bool(r and r.get("stCode") == 200)
            err = str(r)[:300]
        except Exception as e:
            r, t_sl, ok, err = None, int(time.time() * 1000), False, str(e)[:300]
            ambiguous = True
            log.error("SL place ambiguous for %s (tag=%s): %s", sym, tag, e)
        with self._lock:
            if ok:
                placed = syn.get("placed_at_ms")
                try:
                    fill_lat = t_now - int(placed) if placed else None
                except (TypeError, ValueError):
                    fill_lat = None
                new_status = (preserve or {}).get("status") or "ACTIVE"
                syn.update(status=new_status, sl_order_id=r.get("nOrdNo"),
                           quantity=sl_qty, sl_quantity=filled,
                           fill_at_ms=t_now, sl_placed_at_ms=t_sl,
                           sl_placed=True, sl_last_seen_ms=t_sl,
                           sl_missing_streak=0, naked_position=False,
                           blocked_reason=None, sl_guarantee_alerted=False,
                           sl_ambiguous=False, sl_tag_missing_streak=0,
                           error=None)
                if fill_lat is not None and fill_lat >= 0:
                    syn["fill_latency_ms"] = fill_lat
            else:
                syn["status"] = "ERRORED"
                syn["error"] = err
                syn["sl_ambiguous"] = bool(ambiguous)
                syn["blocked_reason"] = ("SL_PLACEMENT_AMBIGUOUS" if ambiguous
                                         else "SL_PLACEMENT_REJECTED")
                # Ambiguous may actually be live; don't call it naked yet.
                syn["naked_position"] = not ambiguous
                syn["sl_tag_missing_streak"] = 0
            syn.pop("_sl_placing", None)
        if not ok:
            log.error("SL placement %s for %s: %s",
                      "ambiguous" if ambiguous else "rejected", sym, r)

    # -- cancel/exit integrity (Plan 06 WP4, non-blocking state machine) --
    def _block_exit(self, syn: dict, reason: str, error: str, naked: bool):
        with self._lock:
            if syn.get("status") == "CLOSED":
                return
            syn["status"] = "ERRORED"
            syn["error"] = error
            syn["blocked_reason"] = reason
            syn["exit_blocked_at_ms"] = int(time.time() * 1000)
            syn["naked_position"] = bool(naked)
            syn["exit_pending"] = False
            syn["exit_state"] = "BLOCKED"
        log.error("EXIT BLOCKED %s entry=%s reason=%s: %s",
                  syn.get("trading_symbol"), syn.get("entry_order_id"),
                  reason, error)

    def _note_unknown(self, syn: dict, kind: str, threshold: int,
                      detail: str = "no new exit will be placed") -> int:
        threshold = max(1, int(threshold))
        with self._lock:
            streak = int(syn.get("exit_unknown_streak") or 0) + 1
            syn["exit_unknown_streak"] = streak
            alert = streak == threshold
            if alert:
                syn["exit_unknown_alerted_at_ms"] = int(time.time() * 1000)
        if alert:
            log.error("EXIT UNKNOWN %s entry=%s (%s) for %d consecutive "
                      "confirmation(s): %s",
                      syn.get("trading_symbol"), syn.get("entry_order_id"),
                      kind, streak, detail)
        return streak

    def _request_sl_cancel(self, syn: dict, sl_id: str, now: float,
                           deadline: float) -> str:
        """One non-blocking cancel attempt, re-armed at its real retry time.

        Every outcome that can leave the leg pending queues the next poll at
        the backoff/deadline boundary, not immediately. A dead order feed then
        still gets its retries instead of falling back to the 30s reconcile.
        """
        def poll_delay(target: float) -> float:
            return max(0.01, min(float(target), deadline) - now)

        requested = False
        with self._lock:
            attempts = int(syn.get("exit_cancel_attempts") or 0)
            max_cancels = max(1, int(settings.exit_confirm_retries) + 1)
            next_at = float(syn.get("exit_next_action_s") or 0.0)
            if now < next_at:
                result = "waiting"
                delay = poll_delay(next_at)
            elif attempts >= max_cancels:
                result = "exhausted"
                delay = poll_delay(deadline)
            else:
                syn["exit_cancel_attempts"] = attempts + 1
                poll = max(0.01, float(settings.exit_confirm_poll_sec))
                backoff = min(poll * (2 ** attempts),
                              max(poll, deadline - now))
                syn["exit_next_action_s"] = now + backoff
                syn["exit_state"] = "CANCEL_PENDING"
                result = "requested"
                delay = poll_delay(now + backoff)
                requested = True
        if not requested:
            self._request_exit_poll(delay)
            return result
        try:
            self.broker.cancel(sl_id)
            log.info("SL cancel requested %s entry=%s (attempt %d/%d, next poll "
                     "+%.2fs)", syn.get("trading_symbol"), sl_id, attempts + 1,
                     max_cancels, delay)
        except Exception as e:  # noqa: BLE001 - StreamReset etc.; retry later
            log.error("SL cancel attempt %d/%d failed for %s: %s",
                      attempts + 1, max_cancels, syn.get("trading_symbol"), e)
        self._request_exit_poll(delay)
        return result

    def _confirm_after_cancel(self, syn: dict, sl_id: str, force: bool) -> str | None:
        """One immediate book check after a cancel request.

        Not a poll loop: production cancels are asynchronous, so this usually
        returns None and the queued reconcile continues later. A fake/synchronous
        broker can report the terminal cancel immediately and let us place the
        exit in the same worker step.
        """
        if self.book_fetcher is None:
            return None
        book, ok = self._read_book()
        if not ok:
            return None
        od = self._book_entry(book, sl_id)
        if od is None:
            return None
        st = str(od.get("ordSt", "")).lower()
        if self._is_complete(st):
            self._mark_sl_hit(syn, sl_id)
            return "sl_hit"
        if self._is_cancel(st) or self._is_reject(st):
            return "exit_placed" if self._place_exit(syn, force=force) == "placed" else "exit_failed"
        return None

    def _drive_exit(self, syn: dict, book=None, book_ok=None, force: bool = False,
                    positions=None, positions_ok=None) -> str:
        """Advance the exit state machine by one non-blocking step.

        States: CANCEL_PENDING -> EXIT_PENDING -> EXIT_CONFIRM ->
        CLOSED | ERRORED | BLOCKED. The worker only makes single broker calls;
        follow-up confirmation is queued through `exit_poll_requester` or driven
        by the next order-feed frame/reconcile.
        """
        if book_ok is None:
            book, ok = self._read_book()
        else:
            ok = bool(book_ok)
            book = book or []
        now = self._clock()
        with self._lock:
            status = syn.get("status")
            ambiguous = bool(syn.get("exit_ambiguous"))
            xid = str(syn.get("exit_order_id") or "")
            sl_id = str(syn.get("sl_order_id") or "")
            if status == "CLOSED":
                return "closed"
            if status == "ERRORED" and not (force or ambiguous):
                return "not_closing"
            if status not in ("CLOSING", "ERRORED"):
                return "not_closing"
            if not syn.get("exit_deadline_s"):
                syn["exit_deadline_s"] = now + max(
                    0.0, float(settings.exit_confirm_timeout_sec))
            tag = str(syn.get("exit_client_tag") or "")
            if force and status == "ERRORED":
                if syn.get("blocked_reason") == "EXIT_REJECTED" and not xid:
                    # A definite reject was never accepted: use a fresh tag.
                    syn["exit_client_tag"] = None
                syn["status"] = "CLOSING"
                syn["blocked_reason"] = None
                syn.pop("error", None)
            if force:
                # Operator retry gets a fresh cancel budget/deadline; never
                # assume the previous attempt made the SL terminal.
                syn["exit_cancel_attempts"] = 0
                syn["exit_next_action_s"] = 0
                syn["exit_unknown_streak"] = 0
                syn["exit_deadline_s"] = now + max(
                    0.0, float(settings.exit_confirm_timeout_sec))
            deadline = float(syn["exit_deadline_s"])
            if not syn.get("exit_state"):
                syn["exit_state"] = "CANCEL_PENDING"
        if self._leg_invalid(syn):
            return "invalid"

        # Resolve an ambiguous placement by its deterministic client tag FIRST.
        if ambiguous and tag:
            found = self._book_by_tag(book, tag) if ok else None
            if found:
                with self._lock:
                    syn["exit_order_id"] = str(found.get("nOrdNo") or "")
                    syn["exit_ambiguous"] = False
                    syn["status"] = "CLOSING"
                    syn["exit_pending"] = True
                    syn["exit_state"] = "EXIT_CONFIRM"
                    syn["naked_position"] = False
                    syn["blocked_reason"] = None
                    syn.pop("error", None)
                    xid = str(syn["exit_order_id"])
                log.warning("EXIT AMBIGUOUS RESOLVED BY TAG %s entry=%s tag=%s "
                            "-> order %s (no duplicate exit)",
                            syn.get("trading_symbol"),
                            syn.get("entry_order_id"), tag, xid)
                self._request_exit_poll()
            else:
                streak = self._note_unknown(syn, "ambiguous tag not in book",
                                            settings.sl_missing_confirmations)
                if force and streak >= max(1, int(settings.sl_missing_confirmations)):
                    with self._lock:
                        syn["exit_client_tag"] = None
                        syn["exit_ambiguous"] = False
                        syn["exit_unknown_streak"] = 0
                        syn["status"] = "CLOSING"
                        xid = ""
                else:
                    self._request_exit_poll(
                        max(0.01, float(settings.exit_confirm_poll_sec)))
                    return "exit_ambiguous"

        if not ok:
            # Book unavailable: never guess the SL is terminal. Alert after N
            # consecutive failed confirmations, but keep driving cancel retries.
            required = max(1, int(settings.sl_missing_confirmations))
            streak = self._note_unknown(syn, "order book unavailable", required)
            with self._lock:
                xid = str(syn.get("exit_order_id") or "")
            if xid:
                if streak >= required:
                    self._block_exit(
                        syn, "EXIT_UNKNOWN",
                        f"exit {xid} unconfirmable: order book unavailable for "
                        f"{streak} consecutive snapshots", naked=True)
                    return "blocked"
                self._request_exit_poll(
                    max(0.01, float(settings.exit_confirm_poll_sec)))
                return "exit_unknown"
            if sl_id:
                res = self._request_sl_cancel(syn, sl_id, now, deadline)
                if ((res in ("exhausted",) or now >= deadline)
                        and self.book_fetcher is not None):
                    self._block_exit(
                        syn, "EXIT_BLOCKED_SL_STILL_WORKING",
                        f"SL {sl_id} not confirmed terminal (book unavailable)",
                        naked=True)
                    return "blocked"
                return "cancel_pending"
            return self._place_exit(syn, force=force)

        with self._lock:
            xid = str(syn.get("exit_order_id") or "")
            sl_id = str(syn.get("sl_order_id") or "")
        exit_od = self._book_entry(book, xid) if xid else None
        sl_od = self._book_entry(book, sl_id) if sl_id else None
        exit_st = str((exit_od or {}).get("ordSt", "")).lower()
        sl_st = str((sl_od or {}).get("ordSt", "")).lower()

        # M6: an SL can complete after the target while the feed is down. The
        # reconcile path must see that even when an exit is already working.
        if sl_od and self._is_complete(sl_st):
            both = bool(exit_od and self._is_complete(exit_st))
            self._mark_sl_hit(syn, sl_id)
            with self._lock:
                syn["exit_pending"] = False
                syn["exit_state"] = None
            if both:
                with self._lock:
                    syn["both_filled"] = True
                    syn["late_exit_risk"] = True
                log.critical("BOTH LEGS FILLED %s entry=%s SL=%s exit=%s — "
                             "positions may be flipped; manual check required",
                             syn.get("trading_symbol"),
                             syn.get("entry_order_id"), sl_id, xid)
            elif xid:
                log.error("LATE EXIT RISK %s: SL %s completed while exit %s "
                          "was working — cancelling the exit",
                          syn.get("trading_symbol"), sl_id, xid)
                self._safe_cancel(xid, "late SL complete")
            return "sl_hit"

        if xid:
            if exit_od is None:
                streak = self._note_unknown(syn, "exit id absent from book",
                                            settings.sl_missing_confirmations)
                if streak >= max(1, int(settings.sl_missing_confirmations)):
                    if force:
                        with self._lock:
                            syn["exit_order_id"] = None
                            syn["exit_client_tag"] = None
                            syn["exit_pending"] = False
                            syn["exit_state"] = "CANCEL_PENDING"
                            syn["exit_unknown_streak"] = 0
                            xid = ""
                        log.warning("FORCE EXIT %s: stale exit id %s absent from "
                                    "book after %d confirmations — retrying once",
                                    syn.get("trading_symbol"), xid, streak)
                    else:
                        self._block_exit(
                            syn, "EXIT_UNKNOWN",
                            f"exit {xid} absent from {streak} consecutive book "
                            "snapshots — state unknown; no automatic retry",
                            naked=True)
                        return "blocked"
                else:
                    self._request_exit_poll(
                        max(0.01, float(settings.exit_confirm_poll_sec)))
                    return "exit_unknown"
            else:
                with self._lock:
                    syn["exit_unknown_streak"] = 0
                if self._is_complete(exit_st):
                    with self._lock:
                        syn["status"] = "CLOSED"
                        syn["closed_at_ms"] = int(time.time() * 1000)
                        syn["exit_reason"] = syn.get("exit_reason") or "TARGET_EXIT"
                        syn["naked_position"] = False
                        syn["blocked_reason"] = None
                        syn["exit_pending"] = False
                        syn["exit_state"] = None
                    log.info("MARKET EXIT COMPLETE %s entry=%s exit=%s",
                             syn.get("trading_symbol"),
                             syn.get("entry_order_id"), xid)
                    return "closed"
                if self._is_reject(exit_st) or self._is_cancel(exit_st):
                    if force:
                        with self._lock:
                            syn["exit_order_id"] = None
                            syn["exit_client_tag"] = None
                            syn["exit_pending"] = False
                            syn["exit_state"] = "CANCEL_PENDING"
                            syn["naked_position"] = False
                            xid = ""
                        log.warning("FORCE EXIT %s: cleared stale exit id %s "
                                    "(%s) — placing exactly one replacement",
                                    syn.get("trading_symbol"), xid, exit_st)
                    else:
                        reason = ("EXIT_REJECTED" if self._is_reject(exit_st)
                                  else "EXIT_CANCELLED")
                        self._block_exit(
                            syn, reason,
                            f"exit {xid} {exit_st}: {(exit_od or {}).get('rejRsn', '')}",
                            naked=True)
                        return "blocked"
                else:
                    with self._lock:
                        syn["status"] = "CLOSING"
                        syn["exit_pending"] = True
                        syn["exit_state"] = "EXIT_CONFIRM"
                        syn["naked_position"] = False
                        syn["blocked_reason"] = None
                        syn.pop("error", None)
                    self._request_exit_poll(
                        max(0.01, float(settings.exit_confirm_poll_sec)))
                    return "exit_pending"

        # No exit id (or a force-cleared stale one): confirm the SL is terminal.
        if sl_id:
            if sl_od is None:
                res = self._request_sl_cancel(syn, sl_id, now, deadline)
                if res == "exhausted" or now >= deadline:
                    self._block_exit(
                        syn, "EXIT_BLOCKED_SL_STILL_WORKING",
                        f"SL {sl_id} not confirmed terminal (missing from book)",
                        naked=True)
                    return "blocked"
                immediate = self._confirm_after_cancel(syn, sl_id, force)
                if immediate == "sl_hit":
                    return "sl_hit"
                if immediate == "exit_placed":
                    return "exit_placed"
                return "cancel_pending"
            elif self._is_cancel(sl_st) or self._is_reject(sl_st):
                pass  # terminal: safe to exit
            else:
                res = self._request_sl_cancel(syn, sl_id, now, deadline)
                if res == "exhausted" or now >= deadline:
                    self._block_exit(
                        syn, "EXIT_BLOCKED_SL_STILL_WORKING",
                        f"SL {sl_id} still {sl_st or 'working'} after cancel retries",
                        naked=False)
                    return "blocked"
                immediate = self._confirm_after_cancel(syn, sl_id, force)
                if immediate == "sl_hit":
                    return "sl_hit"
                if immediate == "exit_placed":
                    return "exit_placed"
                return "cancel_pending"
        return self._place_exit(syn, force=force)

    def _cancel_and_exit(self, syn: dict, wait_exit: bool = False) -> str:
        """Compatibility wrapper: start/advance exit with one non-blocking step."""
        with self._lock:
            if syn.get("status") == "CLOSED":
                return "closed"
            if syn.get("status") == "ERRORED" and not syn.get("exit_ambiguous"):
                return "not_closing"
            if syn.get("status") not in ("CLOSING", "ERRORED"):
                return "not_closing"
            if syn.get("status") == "CLOSING":
                syn["exit_state"] = syn.get("exit_state") or "CANCEL_PENDING"
                syn["exit_pending"] = bool(syn.get("exit_order_id"))
        return self._drive_exit(syn)

    def force_exit(self, entry_id: str, user: str, reason: str) -> dict | None:
        """Authenticated operator escape hatch for a blocked/ERRORED leg.

        Audited (who/when/order id/reason) and idempotent: it still goes through
        the cancel-confirm flow, so a market exit is placed only once and only
        after the SL is confirmed cancelled/terminal.
        """
        with self._lock:
            syn = self._by_any_id_locked(entry_id)
            if syn is None:
                return None
            # Never force-exit an entry that never filled (there is no position
            # to flatten; a market order would open a new one).
            if (syn.get("status") == "PENDING_ENTRY"
                    or (syn.get("status") == "ERRORED"
                        and not syn.get("sl_placed")
                        and not syn.get("fill_price"))):
                # Known leg, but no filled position to flatten. Never send a
                # market order here: it would open a brand-new position.
                return {"force_exit_result": "no_position",
                        "blocked_reason": "NO_POSITION",
                        "entry_order_id": syn.get("entry_order_id"),
                        "trading_symbol": syn.get("trading_symbol"),
                        **copy.deepcopy(syn)}
            prior_reason = syn.get("blocked_reason")
            audit = {
                "user": user,
                "at_ms": int(time.time() * 1000),
                "entry_order_id": syn.get("entry_order_id"),
                "sl_order_id": syn.get("sl_order_id"),
                "exit_order_id": syn.get("exit_order_id"),
                "symbol": syn.get("trading_symbol"),
                "reason": reason,
            }
            syn["force_exit_audit"] = audit
            syn["force_exit_by"] = user
            syn["force_exit_at_ms"] = audit["at_ms"]
            syn["force_exit_reason"] = reason
            if syn.get("status") in ("ERRORED", "ACTIVE", "TRAILING"):
                syn["status"] = "CLOSING"
                syn["blocked_reason"] = None
                if (not syn.get("exit_order_id")
                        and str(prior_reason or "") == "EXIT_REJECTED"):
                    # A rejected/no-id attempt must use a fresh tag on retry.
                    syn["exit_client_tag"] = None
                    syn["exit_ambiguous"] = False
            elif syn.get("status") == "CLOSED" and not syn.get("exit_order_id"):
                out = {"force_exit_result": "closed", "result": "closed",
                       "audit": audit, **copy.deepcopy(syn)}
                out["force_exit_audit"] = audit
                return out
        log.warning("AUDIT force_exit user=%s entry=%s symbol=%s sl=%s exit=%s "
                    "reason=%s", user, audit["entry_order_id"], audit["symbol"],
                    audit["sl_order_id"], audit["exit_order_id"], reason)
        result = self._drive_exit(syn, force=True)
        with self._lock:
            audit["exit_order_id"] = syn.get("exit_order_id")
            audit["result"] = result
            audit["finished_at_ms"] = int(time.time() * 1000)
            syn["force_exit_audit"] = copy.deepcopy(audit)
            out = copy.deepcopy(syn)
        out["force_exit_result"] = result
        out["force_exit_audit"] = audit
        return out

    def note_book_unavailable(self):
        """Per-leg alerting when an authoritative reconcile cannot read the book.

        The bridge deliberately skips applying a failed fetch (so it is not
        mistaken for an empty book), but CLOSING legs must not go unnoticed:
        increment their confirmation counter and alert after N failures (M5).
        """
        with self._lock:
            legs = [o for o in self.orders.values()
                    if o.get("status") in ("CLOSING", "ERRORED")]
        required = max(1, int(settings.sl_missing_confirmations))
        for syn in legs:
            with self._lock:
                status = syn.get("status")
                xid = str(syn.get("exit_order_id") or "")
                already = (status == "ERRORED"
                           and syn.get("blocked_reason") == "EXIT_UNKNOWN")
                deadline = float(syn.get("exit_deadline_s") or 0.0)
            if already:
                continue
            if xid or syn.get("exit_ambiguous") or status == "CLOSING":
                label = ("exit id present; awaiting authoritative book" if xid
                         else "no exit id yet; awaiting authoritative book")
                streak = self._note_unknown(syn, "order book unavailable",
                                            required, label)
                if status != "CLOSING" or streak < required:
                    continue
                # Respect an active exit deadline (when one exists) so one
                # transient book failure cannot error the leg immediately.
                if deadline and float(self._clock()) < deadline:
                    continue
                if xid:
                    detail = (f"exit {xid} unconfirmable: order book "
                              f"unavailable for {streak} consecutive snapshots")
                else:
                    detail = ("no exit id placed: order book unavailable for "
                              f"{streak} consecutive snapshots")
                self._block_exit(syn, "EXIT_UNKNOWN", detail, naked=True)
                log.error("EXIT UNKNOWN ALERT %s entry=%s exit=%s: %s",
                          syn.get("trading_symbol"),
                          syn.get("entry_order_id"), xid or "(none)", detail)

    def reconcile(self, book: list[dict], positions: dict | None = None,
                  positions_ok: bool | None = None) -> dict:
        # Snapshot actual trackers under the lock, then drive them outside it:
        # reconcile may place an SL (broker I/O) and must not block health reads.
        with self._lock:
            legs = list(self.orders.values())
        return self._reconcile(legs, book, positions, positions_ok)

    def _reconcile(self, legs: list[dict], book: list[dict],
                   positions: dict | None = None,
                   positions_ok: bool | None = None) -> dict:
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
        repaired: list[str] = []
        naked: list[str] = []
        now = int(time.time() * 1000)
        with self._lock:
            before = {id(s): s.get("status") for s in legs}
        for syn in legs:
            try:
                status = syn.get("status")
                if status == "CLOSED":
                    tag = str(syn.get("exit_client_tag") or "")
                    late = self._book_by_tag(book, tag) if tag else None
                    late_st = str((late or {}).get("ordSt", "")).lower()
                    late_id = str((late or {}).get("nOrdNo") or "")
                    if late_id and self._is_open(late_st):
                        log.critical(
                            "LATE EXIT RISK %s entry=%s: tagged exit %s (%s) "
                            "is still working after CLOSED - cancelling",
                            syn.get("trading_symbol"),
                            syn.get("entry_order_id"), late_id, late_st)
                        with self._lock:
                            syn["late_exit_risk"] = True
                            syn["late_exit_cancelled_ms"] = int(
                                time.time() * 1000)
                        self._safe_cancel(late_id, "late tagged exit after CLOSED")
                        driven.append(str(syn.get("entry_order_id") or ""))
                    continue
                if status == "PENDING_ENTRY":
                    eid = str(syn.get("entry_order_id") or "")
                    od = by_id.get(eid)
                    if od:
                        st = str(od.get("ordSt", "")).lower()
                        if (self._is_complete(st) or self._is_reject(st)
                                or self._is_cancel(st)):
                            self.on_order_update(od)
                            driven.append(eid)
                        else:
                            self._check_entry_guarantee(syn, st, now, eid, od)
                    else:
                        self._check_entry_guarantee(syn, "missing", now, eid)
                elif status in ("ACTIVE", "TRAILING"):
                    # A partial-fill remainder cancel started by the guarantee
                    # path may still need retries after the SL went ACTIVE.
                    if syn.get("entry_cancel_pending"):
                        self._request_entry_cancel(
                            syn, str(syn.get("entry_order_id") or ""), now)
                    pstate, pdetail = self._position_state(syn, positions,
                                                           positions_ok)
                    sl = str(syn.get("sl_order_id") or "")
                    od = by_id.get(sl) if sl else None
                    if pstate == "flat":
                        if self._flat_confirmation(syn):
                            self._mark_external_flat(syn, sl, od)
                            driven.append(str(syn.get("entry_order_id") or ""))
                        else:
                            with self._lock:
                                syn["blocked_reason"] = "POSITION_FLAT_UNCONFIRMED"
                                syn["naked_position"] = False
                            log.warning("POSITION FLAT UNCONFIRMED %s entry=%s: "
                                        "one flat snapshot, waiting for a second "
                                        "before closing/cancelling SL",
                                        syn.get("trading_symbol"),
                                        syn.get("entry_order_id"))
                        continue
                    self._reset_flat_confirmation(syn)
                    if pstate == "mismatch":
                        self._mark_position_mismatch(syn, pdetail)
                        errored.append(sl or str(syn.get("entry_order_id") or ""))
                        naked.append(sl or str(syn.get("entry_order_id") or ""))
                        continue
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
                            outcome = self._repair_or_error(
                                syn, sl, "missing", positions, positions_ok)
                            if outcome == "repaired":
                                repaired.append(sl)
                            elif outcome != "skipped":
                                errored.append(sl)
                                naked.append(sl)
                        else:
                            log.warning("reconcile: %s SL %s not in book yet "
                                        "(miss %d/%d, grace %ss) — keeping leg %s",
                                        syn.get("trading_symbol"), sl, streak,
                                        settings.sl_missing_confirmations,
                                        settings.sl_visibility_grace_sec, status)
                    else:
                        st = str(od.get("ordSt", "")).lower()
                        if self._is_complete(st):
                            self.on_order_update(od)  # position exited via SL
                            driven.append(sl)
                        elif self._is_reject(st) or self._is_cancel(st):
                            outcome = self._repair_or_error(
                                syn, sl, st, positions, positions_ok)
                            if outcome == "repaired":
                                repaired.append(sl)
                            elif outcome != "skipped":
                                errored.append(sl)
                                naked.append(sl)
                        else:
                            # SL is working: reset the miss/replace episode.
                            with self._lock:
                                syn["sl_missing_streak"] = 0
                                syn["sl_last_seen_ms"] = now
                                syn["sl_replace_attempts"] = 0
                                syn["naked_position"] = False
                elif (status == "ERRORED"
                      and syn.get("blocked_reason") in
                      ("SL_PLACEMENT_REJECTED", "SL_PLACEMENT_AMBIGUOUS")):
                    eid = str(syn.get("entry_order_id") or "")
                    tag_state = self._adopt_sl_by_tag(syn, book, True)
                    if tag_state in ("adopted", "closed"):
                        repaired.append(str(syn.get("sl_order_id") or eid))
                        driven.append(eid)
                        continue
                    if tag_state == "unavailable":
                        continue
                    if tag_state in ("absent", "terminal"):
                        with self._lock:
                            streak = int(syn.get("sl_tag_missing_streak") or 0) + 1
                            syn["sl_tag_missing_streak"] = streak
                            required = max(1, int(settings.sl_missing_confirmations))
                        if tag_state == "absent" and streak < required:
                            log.warning("SL TAG NOT IN BOOK %s entry=%s tag=%s "
                                        "(%d/%d) - not re-placing yet",
                                        syn.get("trading_symbol"), eid,
                                        syn.get("sl_client_tag"), streak, required)
                            continue
                        with self._lock:
                            syn["_sl_tag_absent_confirmed"] = True
                    outcome = self._repair_or_error(
                        syn, "", str(syn.get("blocked_reason")), positions,
                        positions_ok)
                    if outcome == "repaired":
                        repaired.append(str(syn.get("sl_order_id") or eid))
                    elif outcome != "skipped":
                        errored.append(eid)
                        naked.append(eid)
                    driven.append(eid)
                elif (status == "ERRORED"
                      and syn.get("blocked_reason") == "POSITION_MISMATCH"):
                    # A mismatch stays blocked until a fresh authoritative
                    # snapshot is actually okay (or flat twice); an SL merely
                    # reappearing in the book must not oscillate it to ACTIVE.
                    pstate, pdetail = self._position_state(
                        syn, positions, positions_ok)
                    if pstate == "flat":
                        if self._flat_confirmation(syn):
                            self._mark_external_flat(
                                syn, str(syn.get("sl_order_id") or ""), None)
                            driven.append(str(syn.get("entry_order_id") or ""))
                    elif pstate == "ok":
                        self._reset_flat_confirmation(syn)
                        with self._lock:
                            restored = syn.get("mismatch_prev_status") or "ACTIVE"
                            syn["status"] = restored
                            syn["naked_position"] = False
                            syn["blocked_reason"] = None
                            syn["error"] = None
                        log.warning("POSITION MISMATCH CLEARED %s entry=%s: %s "
                                    "-> %s", syn.get("trading_symbol"),
                                    syn.get("entry_order_id"), pdetail, restored)
                        driven.append(str(syn.get("entry_order_id") or ""))
                    else:
                        self._reset_flat_confirmation(syn)
                elif status == "ERRORED" and syn.get("naked_position"):
                    # 06a gap: a missing SL that reappears later must heal the
                    # leg instead of leaving it ERRORED forever.
                    sl = str(syn.get("sl_order_id") or "")
                    od = by_id.get(sl) if sl else None
                    if od is not None:
                        st = str(od.get("ordSt", "")).lower()
                        if not (self._is_reject(st) or self._is_cancel(st)
                                or self._is_complete(st)):
                            with self._lock:
                                syn["status"] = syn.get("naked_prev_status") or "ACTIVE"
                                syn["naked_position"] = False
                                syn["blocked_reason"] = None
                                syn.pop("error", None)
                                syn["sl_missing_streak"] = 0
                                syn["sl_last_seen_ms"] = now
                                syn["sl_replace_attempts"] = 0
                            driven.append(sl)
                            log.info("reconcile: %s SL %s reappeared — leg restored to %s",
                                     syn.get("trading_symbol"), sl, syn.get("status"))
                elif status == "CLOSING":
                    # Always examine a CLOSING leg (even with an exit id) so a
                    # feed-dead late SL fill is caught (M6) and missing exits
                    # cannot wedge silently (M5).
                    res = self._drive_exit(
                        syn, book, True, positions=positions,
                        positions_ok=positions_ok)
                    driven.append(str(syn.get("entry_order_id") or ""))
                    if res == "blocked":
                        errored.append(str(syn.get("entry_order_id") or ""))
                elif status == "ERRORED" and syn.get("exit_ambiguous"):
                    # Resolve a timed-out exit by tag before any retry.
                    res = self._drive_exit(
                        syn, book, True, positions=positions,
                        positions_ok=positions_ok)
                    driven.append(str(syn.get("entry_order_id") or ""))
                    if res == "blocked":
                        errored.append(str(syn.get("entry_order_id") or ""))
                with self._lock:
                    syn["last_reconcile_ms"] = now
            except Exception as e:  # noqa: BLE001 - one bad leg must not stop others
                log.error("reconcile leg err: %s", e)
        # L6: several paths (on_order_update, drive_exit, external-flat) can
        # change a leg during this loop. Recompute the summary from the live
        # trackers so the UI/health never under-report ERRORED/naked legs.
        for syn in legs:
            ids = [str(syn.get(k)) for k in
                   ("entry_order_id", "sl_order_id", "exit_order_id")
                   if syn.get(k)]
            sid = ids[0] if ids else ""
            if (syn.get("status") != before.get(id(syn))
                    and sid and not any(i in driven for i in ids)):
                driven.append(sid)
            if (syn.get("status") == "ERRORED" and sid
                    and not any(i in errored for i in ids)):
                errored.append(sid)
            if (syn.get("naked_position") and not syn.get("exit_pending")
                    and sid and not any(i in naked for i in ids)):
                naked.append(sid)
        return {"checked": len(legs), "driven": driven, "repaired": repaired,
                "errored": errored, "naked": naked,
                "positions": len(positions or {})}

    def _flat_confirmation(self, syn: dict) -> bool:
        """Count consecutive fresh flat snapshots; require N before closing."""
        limit = max(1, int(settings.flat_position_confirmations))
        with self._lock:
            streak = int(syn.get("position_flat_streak") or 0) + 1
            syn["position_flat_streak"] = streak
            return streak >= limit

    def _reset_flat_confirmation(self, syn: dict):
        with self._lock:
            syn["position_flat_streak"] = 0
            if syn.get("blocked_reason") == "POSITION_FLAT_UNCONFIRMED":
                syn["blocked_reason"] = None

    @staticmethod
    def _position_net_shares(pos) -> float:
        """Signed position size in SHARES, matching tracker quantities.

        `kotak_client.positions()` reports F&O `qty` in lots and `qty_shares`
        unsigned; the engine tracks shares. Normalize here using the signed lot
        count and lot size. Equity rows (lot_sz == 0) are already shares.
        """
        if not isinstance(pos, dict):
            return 0.0
        lots = float(pos.get("qty", 0) or 0)
        lot_sz = float(pos.get("lot_sz", 0) or 0)
        if lot_sz > 0:
            return (1.0 if lots > 0 else -1.0 if lots < 0 else 0.0) * abs(lots) * lot_sz
        return lots

    def _position_state(self, syn: dict, positions, positions_ok):
        """(state, detail) from a fresh positions snapshot for this leg.

        state: ok | flat | mismatch | unknown. Only `ok` permits an SL repair
        (Plan 06 H3); `unknown` means the snapshot is missing/failed.
        """
        if positions_ok is False or positions is None:
            return "unknown", "positions snapshot unavailable"
        if not isinstance(positions, dict):
            return "unknown", "positions snapshot not a mapping"
        if not positions and positions_ok is not True:
            # A bare {} is the old "no snapshot supplied" direct-engine call.
            # Only an explicitly successful empty snapshot is authoritative
            # (flat account); never infer flat from an absent one.
            return "unknown", "empty positions snapshot not authoritative"
        sym = str(syn.get("trading_symbol") or "")
        pos = positions.get(sym)
        if pos is None:
            for k, v in positions.items():
                if str(k).upper() == sym.upper():
                    pos, sym = v, str(k)
                    break
        confirmed_fill = bool(syn.get("fill_price") or syn.get("sl_placed"))
        if pos is None:
            if not confirmed_fill:
                return "unknown", "flat snapshot but leg has no confirmed fill"
            return "flat", "no position row"
        try:
            net = self._position_net_shares(pos)
        except (TypeError, ValueError):
            return "mismatch", "position qty/lot size not numeric"
        if abs(net) < 1e-9:
            if not confirmed_fill:
                return "unknown", "flat snapshot but leg has no confirmed fill"
            return "flat", "flat position"
        side = syn.get("side", "B")
        same_side = net > 0 if side == "B" else net < 0
        if not same_side:
            return "mismatch", f"position {net:g} shares is opposite to leg side {side}"
        try:
            expected = abs(float(syn.get("sl_quantity")
                                 or syn.get("quantity") or 0))
        except (TypeError, ValueError):
            expected = 0.0
        if expected and abs(net) + 1e-9 < expected:
            return "mismatch", (f"position {abs(net):g} shares < protected qty "
                                f"{expected:g}")
        return "ok", f"position {net:g} shares"

    def _mark_external_flat(self, syn: dict, sl: str, sl_od: dict | None):
        """Position is already flat (manual/external) — never reopen it."""
        with self._lock:
            if syn.get("status") == "CLOSED":
                return
            syn["status"] = "CLOSED"
            syn["closed_at_ms"] = int(time.time() * 1000)
            syn["exit_reason"] = "EXTERNAL_FLAT"
            syn["naked_position"] = False
            syn["blocked_reason"] = None
            syn["exit_pending"] = False
            syn["exit_state"] = None
            syn["position_flat_streak"] = 0
        log.warning("EXTERNAL FLAT %s entry=%s: broker position is flat — "
                    "closing tracker, no SL repair",
                    syn.get("trading_symbol"), syn.get("entry_order_id"))
        if sl and sl_od is not None:
            st = str(sl_od.get("ordSt", "")).lower()
            if not (self._is_complete(st) or self._is_cancel(st)
                    or self._is_reject(st)):
                self._safe_cancel(sl, "position externally flat")

    def _mark_position_mismatch(self, syn: dict, detail: str):
        with self._lock:
            if syn.get("status") != "ERRORED":
                syn["mismatch_prev_status"] = syn.get("status") or "ACTIVE"
            syn["status"] = "ERRORED"
            syn["error"] = f"POSITION_MISMATCH: {detail}; SL not repaired"
            syn["blocked_reason"] = "POSITION_MISMATCH"
            syn["naked_position"] = True
            syn["exit_pending"] = False
            syn["exit_state"] = "BLOCKED"
            syn["position_flat_streak"] = 0
        log.error("POSITION MISMATCH %s entry=%s: %s — no new SL placed",
                  syn.get("trading_symbol"), syn.get("entry_order_id"), detail)

    def _request_entry_cancel(self, syn: dict, entry_id: str, now: int):
        """One entry-cancel attempt with a retry budget starting at attempt one.

        The SL-guarantee deadline is normally already in the past by the first
        reconcile (30s loop vs 12s guarantee), so it must not gate cancel
        attempts. Retries are bounded by ENTRY_CANCEL_RETRY_SEC/MAX_ATTEMPTS.
        """
        if not entry_id:
            return
        poll_delay = None
        with self._lock:
            if syn.get("entry_cancel_sent_ms"):
                # A confirmed cancel is terminal for this leg; clear any legacy
                # pending flag so ACTIVE reconcile does not retry a no-op.
                syn["entry_cancel_pending"] = False
                return
            attempts = int(syn.get("entry_cancel_attempts") or 0)
            max_attempts = max(1, int(settings.entry_cancel_max_attempts))
            retry_start = int(syn.get("entry_cancel_retry_started_ms") or 0)
            if not retry_start:
                retry_start = now
                syn["entry_cancel_retry_started_ms"] = retry_start
            retry_ms = int(max(0.1, float(settings.entry_cancel_retry_sec)) * 1000)
            deadline = retry_start + retry_ms
            next_ms = int(syn.get("entry_cancel_next_ms") or 0)
            if attempts >= max_attempts or now >= deadline:
                if not syn.get("entry_cancel_failed_alerted"):
                    syn["entry_cancel_failed_alerted"] = True
                    syn["entry_cancel_pending"] = False
                    syn["entry_cancel_next_ms"] = 0
                    syn["blocked_reason"] = "ENTRY_CANCEL_FAILED"
                    syn["error"] = ("entry cancel failed after "
                                    f"{attempts}/{max_attempts} attempt(s) within "
                                    f"{settings.entry_cancel_retry_sec}s - entry "
                                    "may still fill; no SL guarantee")
                    log.critical("ENTRY CANCEL FAILED %s entry=%s - entry may "
                                 "still fill; no SL guarantee",
                                 syn.get("trading_symbol"), entry_id)
                return
            if now < next_ms:
                poll_delay = max(0.01, (next_ms - now) / 1000.0)
                attempts_left = max_attempts - attempts
                log.info("SL GUARANTEE auto-cancel waiting %s entry=%s "
                         "(next %.1fs, %d attempt(s) left)",
                         syn.get("trading_symbol"), entry_id, poll_delay,
                         attempts_left)
            else:
                attempts += 1
                base = max(0.01, float(settings.exit_confirm_poll_sec))
                backoff = min(base * (2 ** min(attempts - 1, 5)), 5.0)
                syn["entry_cancel_attempts"] = attempts
                syn["entry_cancel_next_ms"] = now + int(backoff * 1000)
                syn["entry_cancel_pending"] = True
                syn["entry_cancel_requested_ms"] = now
                poll_delay = backoff
                sym = syn.get("trading_symbol")
        if poll_delay is not None and (now < next_ms):
            self._request_exit_poll(poll_delay)
            return "waiting"
        try:
            r = self.broker.cancel(entry_id)
            ok = not isinstance(r, dict) or r.get("stCode") == 200
            err = None if ok else str(r)[:200]
        except Exception as e:  # noqa: BLE001 - retry within the budget
            ok, err = False, str(e)[:200]
        with self._lock:
            if ok:
                syn["entry_cancel_sent_ms"] = now
                syn["entry_cancel_pending"] = False
                syn["entry_cancel_last_error"] = None
            else:
                syn["entry_cancel_last_error"] = err
        if ok:
            log.warning("SL GUARANTEE auto-cancel sent for %s entry=%s "
                        "(attempt %d)", sym, entry_id, attempts)
        else:
            log.error("SL GUARANTEE auto-cancel attempt %d failed for %s "
                      "entry=%s: %s", attempts, sym, entry_id, err)
            self._request_exit_poll(poll_delay or 0.01)
        return "sent" if ok else "failed"

    def _check_entry_guarantee(self, syn: dict, st: str, now: int, entry_id: str,
                               od: dict | None = None):
        """Alert (and optionally auto-cancel) an entry still without an SL."""
        try:
            due = int(syn.get("sl_guarantee_due_ms") or 0)
        except (TypeError, ValueError):
            due = 0
        if not due or now < due:
            return
        filled = self._fill_qty(od) if od else None
        if filled and filled > 0:
            # Plan 06 L1/M2: never leave a partially filled entry unprotected.
            with self._lock:
                try:
                    ordered = float(syn.get("quantity") or 0)
                except (TypeError, ValueError):
                    ordered = 0.0
                already = bool(syn.get("sl_placed") or syn.get("sl_order_id"))
            if not already:
                log.error("SL GUARANTEE PARTIAL FILL %s entry=%s: %s filled — "
                          "placing SL for the filled qty", syn.get("trading_symbol"),
                          entry_id, int(filled))
                self._place_sl(syn, od)
            open_remainder = self._is_open(st) and (not ordered or filled < ordered)
            if open_remainder:
                self._request_entry_cancel(syn, entry_id, now)
            with self._lock:
                syn["sl_guarantee_alerted"] = True
                syn["guarantee_alerted_at_ms"] = now
            return
        behavior = str(settings.sl_guarantee_behavior or "alert").lower()
        # M2: failed auto-cancel requests keep retrying with backoff even though
        # the initial guarantee alert was already raised (not latched forever).
        if behavior == "auto_cancel" and syn.get("entry_cancel_pending"):
            self._request_entry_cancel(syn, entry_id, now)
        with self._lock:
            if syn.get("sl_guarantee_alerted"):
                return
            syn["sl_guarantee_alerted"] = True
            syn["guarantee_alerted_at_ms"] = now
            syn["blocked_reason"] = "ENTRY_NOT_FILLED_SL_GUARANTEE"
        log.error("SL GUARANTEE MISSED %s entry=%s after %ss (book=%s) — "
                  "no protective SL yet (policy=%s)",
                  syn.get("trading_symbol"), entry_id,
                  settings.sl_guarantee_sec, st, behavior)
        if behavior == "auto_cancel" and entry_id:
            self._request_entry_cancel(syn, entry_id, now)

    def _repair_or_error(self, syn: dict, sl: str, st: str,
                         positions=None, positions_ok=None) -> str:
        """Re-place a missing/dead SL once, else ERRORED + naked alert.

        Plan 06 H3: a fresh successful positions snapshot is mandatory before
        any repair. A flat position is closed as EXTERNAL_FLAT (never reopened);
        an opposite/short position is ERRORED + POSITION_MISMATCH.
        """
        pstate, pdetail = self._position_state(syn, positions, positions_ok)
        if pstate == "unknown":
            self._reset_flat_confirmation(syn)
            with self._lock:
                syn["blocked_reason"] = "POSITIONS_UNAVAILABLE"
                syn["naked_position"] = True
                syn["sl_repair_skipped_ms"] = int(time.time() * 1000)
            log.error("SL REPAIR SKIPPED %s entry=%s SL=%s (%s): %s; no new SL",
                      syn.get("trading_symbol"), syn.get("entry_order_id"),
                      sl, st, pdetail)
            return "skipped"
        if pstate == "flat":
            if not self._flat_confirmation(syn):
                with self._lock:
                    syn["blocked_reason"] = "POSITION_FLAT_UNCONFIRMED"
                    syn["naked_position"] = False
                log.warning("SL REPAIR WAITING %s entry=%s: one flat snapshot, "
                            "not closing/cancelling until confirmed",
                            syn.get("trading_symbol"), syn.get("entry_order_id"))
                return "skipped"
            self._mark_external_flat(syn, sl, None)
            return "closed"
        self._reset_flat_confirmation(syn)
        if pstate == "mismatch":
            self._mark_position_mismatch(syn, pdetail)
            return "errored"
        with self._lock:
            prev_status = syn.get("status")
            if (prev_status == "ERRORED"
                    and syn.get("blocked_reason") in
                    ("SL_PLACEMENT_REJECTED", "SL_PLACEMENT_AMBIGUOUS")):
                prev_status = (syn.get("sl_prev_status")
                               or syn.get("naked_prev_status") or "ACTIVE")
            if prev_status not in ("ACTIVE", "TRAILING"):
                return "errored"
            syn["naked_prev_status"] = prev_status
            attempts = int(syn.get("sl_replace_attempts") or 0)
            limit = max(0, int(settings.sl_replace_max_attempts))
        if attempts < limit and syn.get("fill_price") and self._replace_sl(syn, preserve=True):
            with self._lock:
                syn["sl_missing_streak"] = 0
                syn["sl_last_seen_ms"] = int(time.time() * 1000)
                syn["naked_position"] = False
                syn["blocked_reason"] = None
                syn.pop("error", None)
            log.warning("reconcile: %s SL %s was %s — re-placed as %s",
                        syn.get("trading_symbol"), sl, st,
                        syn.get("sl_order_id"))
            return "repaired"
        with self._lock:
            ambiguous = (syn.get("status") == "ERRORED"
                         and syn.get("blocked_reason")
                         == "SL_PLACEMENT_AMBIGUOUS"
                         and bool(syn.get("sl_ambiguous")))
            absent = (int(syn.get("sl_tag_missing_streak") or 0)
                      >= max(1, int(settings.sl_missing_confirmations)))
        if ambiguous:
            # The replacement may have landed despite the timeout. Keep this
            # reason intact so the next book snapshot can adopt it by tag; do
            # not downgrade to SL_MISSING/naked until the tag is confirmed
            # absent after the bounded retry/replace budget is spent.
            if not (absent and attempts >= limit):
                log.warning("reconcile: %s replacement SL timed out (tag=%s) — "
                            "waiting for tag adoption before declaring naked",
                            syn.get("trading_symbol"), syn.get("sl_client_tag"))
                return "skipped"
            with self._lock:
                syn["sl_ambiguous"] = False
        self._mark_naked(
            syn, "SL_MISSING" if st == "missing" else "SL_TERMINAL",
            f"SL {sl} {st} on reconcile — position unprotected "
            f"(replace attempts={attempts}/{limit})")
        return "errored"

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
                    syn["exit_reason"] = "TARGET_EXIT"
                    cancel_id = syn.get("sl_order_id") or ""
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
            # Do not gate the market exit solely on a feed `cancelled` frame:
            # confirm through the order book, then place at most one exit and
            # watch it to terminal (bounded by EXIT_CONFIRM_TIMEOUT_SEC).
            self._cancel_and_exit(syn, wait_exit=True)
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
