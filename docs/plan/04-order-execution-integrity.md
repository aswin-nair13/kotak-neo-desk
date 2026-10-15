# Plan 04 — Order-Execution Integrity (H3, M5, M6)

Status: **IMPLEMENTED** (H3 strict fill gate + M5 idempotency/button lock + M6
price floor; partial fill = SL sized to the fill with the remainder left
working, re-enable on terminal feed state or a 30 s timeout) · Priority: P0
(trading safety) · Scope:
`backend/app/synthetic_engine.py`, `backend/app/routes/orders.py`,
`backend/app/kotak_client.py`, `backend/app/schemas.py`,
`frontend/src/App.jsx`, `frontend/src/api.js`, tests.

Goal: the protective SL is placed **if and only if** the entry is genuinely
filled; duplicate submissions can never double-order; and derived entry prices
are never non-positive.

---

## H3 — Place the SL iff the entry is properly filled

### Problem (`synthetic_engine.py:75-133`)
The SL path triggers on `oid == entry_order_id and status == "complete" and
syn["status"] == "PENDING_ENTRY"`. Gaps:
- No check that the order is **fully** filled, nor that the fill is real —
  `_fill_price` returning `None` still falls through and places an SL using the
  planned stop.
- **Entry `rejected`/`cancelled`/`expired` is never handled** → the leg sits in
  `PENDING_ENTRY` forever.
- Placing the SL is not guarded against being re-driven (matters for Plan 03's
  reconcile and for duplicate feed frames).
- SL quantity uses the originally ordered qty even if the fill differs.

### Design — strict fill gate
Only place the SL when **all** hold:
1. `ordSt` is the terminal-filled state (`complete`).
2. `fldQty` (filled quantity) is `> 0` and `>= qty` (full fill). For a partial
   fill, **do not place a full-size SL**:
   - Preferred: place the SL for the **actually filled qty** and separately
     decide the remainder (cancel it or keep working) — document the choice.
   - Minimum viable: mark `ERRORED` with reason `PARTIAL_FILL` and alert.
3. A valid average fill price is available: `_fill_price(od) > 0`. If the feed
   frame lacks it, fetch `kotak.order_history(order_id)` once and read the
   average price. If still unavailable → `ERRORED` + alert (do **not** guess).
4. Recompute `target_price` / `stop_loss_price` from the **actual** fill
   (existing logic), side-aware.
5. Place SL with `quantity = filled_qty`.

**Idempotency guard:** set a flag (`sl_placed=True` / `sl_order_id` set) before
returning; re-entry returns early. Reconcile (Plan 03) must call the same
guarded path.

**Terminal entry handling (new):**
```python
elif oid == syn["entry_order_id"] and "reject" in status:
    syn.update(status="ERRORED", error=f"entry rejected: {od.get('rejRsn','')}")
elif oid == syn["entry_order_id"] and status in ("cancelled", "expired"):
    syn.update(status="CLOSED", exit_reason="ENTRY_" + status.upper())
```
No SL placed in any of these cases.

**Alerting:** `ERRORED` legs (entry or SL side) must be visibly surfaced
(`/api/orders/synthetic` already returns `status`/`error`; ensure the UI
highlights `ERRORED` and, if feasible, raise a log at `ERROR` level — ties to
backlog item H3-alert).

### Files
- `backend/app/synthetic_engine.py` — fill gate, terminal-entry states,
  idempotency flag, optional `order_history` price fetch via broker.
- `backend/app/kotak_client.py` — helper `average_fill_price(order_id)` (wraps
  `order_history`).
- `backend/tests/test_bracket_exit.py` — extend.

### Verification
- Entry `complete` with valid fill → SL placed once, recomputed from fill.
- Duplicate `complete` frames → SL placed exactly once.
- Entry `rejected` / `cancelled` → no SL, leg terminal.
- Entry `complete` with `fldQty < qty` → no full-size SL (per chosen policy).
- Entry `complete` with no parseable price and no history → `ERRORED`, no SL.

---

## M5 — Idempotency + in-flight button lock

### Problem
`App.jsx:660-668` buy/sell/straddle buttons are never disabled during the
request; `routes/orders.py:125,139` have no dedupe. A double-click places two
orders. (Row buttons already use `busyKey`.)

### Design
1. **Client-side lock.** Generate an idempotency key on click
   (`crypto.randomUUID()`), set an in-flight state that disables the triggering
   button(s), and pass the key to the server.
2. **Server-side idempotency.** Accept `client_order_tag` (map to the SDK's
   `tag=` → echoed as `GuiOrdId`). Keep a small in-memory TTL cache
   (e.g. 60 s) of seen keys → return the original `entry_order_id`/response
   instead of re-placing. Add to `SingleOrderBody`/`StraddleBody` as optional.
3. **Re-enable policy.** Default: re-enable the button when the request returns
   **and** the entry reaches a terminal state (`complete`/`rejected`/
   `cancelled`) via the WS order feed, with a safety timeout (e.g. 30 s) so a
   dropped feed can't strand the UI. (Owner note: "only enable back once the
   order is placed and filled" — if you want strict fill-gated re-enable, make
   the timeout configurable and default to fill-gated.)
4. Cover single CE/PE and both straddle legs.

### Files
- `frontend/src/App.jsx` — in-flight state for `place()`/`placeRow()`; pass tag;
  re-enable on terminal feed event / timeout.
- `frontend/src/api.js` — no change expected.
- `backend/app/routes/orders.py` — dedupe cache; thread-safe (Lock) and shared
  with reconcile; attach `tag` to `place_order`.
- `backend/app/kotak_client.py` — pass `tag` through (already supported:
  `place_order(tag=...)`).
- `backend/app/schemas.py` — optional `client_order_tag: str | None`.

### Verification
- Double-click (two rapid identical requests with the same tag) → exactly one
  order at the broker; second returns the first's result.
- Button disabled from click until terminal/timeout.
- Distinct clicks produce distinct orders.

---

## M6 — Floor derived entry prices at 0.05

### Problem (`routes/orders.py:67-70`)
`SELL MKT` computes `entry = ref_ltp - 5`, which can be `<= 0` for cheap
premiums; other modes can also fall below the tick floor. The exchange rejects
non-positive prices.

### Design
In `_leg`, before returning, clamp the final price:
```python
entry = round(max(0.05, float(entry)), 2)
```
Apply uniformly to MKT/ML/L-1/L. Keep the existing `offset >= 0` validation.

### Files
- `backend/app/routes/orders.py` — `_leg` final clamp.
- `backend/tests/test_bracket_exit.py` — add a cheap-premium SELL MKT case.

### Verification
- `_leg(..., side="S", ref_ltp=3.0)` returns `0.05`, not `-2.0`.
- Existing symmetry tests
  (`test_leg_symmetry`) still pass for normal premiums.

---

## Cross-cutting risk
Plans 03 and 04 both touch `synthetic_engine.py` and depend on a single,
idempotent "place SL" path. Land H3's guard first (or as the same PR) before
enabling reconcile, and add serialization (backlog M3) to avoid races between
`on_order_update`, `on_tick`, and reconcile.
