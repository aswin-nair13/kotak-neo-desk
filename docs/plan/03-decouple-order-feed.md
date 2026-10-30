# Plan 03 — Decouple Order Feed from Market Feed (H1)

Status: **IMPLEMENTED** (with the minimal Plan 04 H3 idempotency guard; Plan 04
is also implemented) · Priority: P0 (trading safety) · Scope:
`backend/app/sfeed_bridge.py`, `backend/app/synthetic_engine.py`,
`backend/app/ws_manager.py`, `backend/app/main.py`, `backend/app/routes/auth.py`.

## Problem
In `sfeed_bridge.py:_session()` (lines 129–152) the order feed is created inside
the **market** WebSocket scope and cancelled in the `finally` block:

```python
async with client.create_websocket() as ws:      # market feed
    ...
    order_task = asyncio.create_task(self._consume_orders(client))
    try:
        async for msg in ws: ...                  # blocks until market drops
    finally:
        self._ws = None
        order_task.cancel()                       # <-- kills order feed too
```

Compounding it, the SDK's own reconnect is pre-empted: `SFeedWebSocket.__anext__`
raises `StopAsyncIteration` as soon as `_connected` goes False during a reconnect
window (`websocket/feed/client.py:284-289`), so our `async for` ends and the
whole session tears down.

**Consequence:** any market-feed blip stops fill/reject events during the
reconnect gap. If an entry fills in that window, `synthetic_engine.on_order_update`
never runs and the **protective SL is never placed → naked position.** There is
no event replay on reconnect.

## Goal
The order feed must survive market-feed drops (and vice-versa), each with its
own independent supervisor/reconnect. After every (re)connect, reconcile missed
state against the broker so nothing is silently lost.

## Design
1. **Two independent supervised tasks on the bridge loop.**
   - `_supervise_market()` — owns `create_websocket`, subscriptions, tick loop.
   - `_supervise_orders()` — owns `create_order_feed`, order/position loop.
   - `_run()` runs both via `asyncio.gather(...)` (or two tasks) and only exits
     when `self._stop` is set.
2. **Own reconnect loops.** Wrap each session in `try/except` + `await sleep`
   (reuse `retry_sec`). Do not rely on the SDK's internal reconnect being
   pre-empted by `__anext__`; if the iteration ends for any reason other than
   `self._stop`, recreate the socket after a backoff.
   - Consider passing `max_reconnect_attempts=0` to the SDK clients so reconnect
     is owned in one place, avoiding double-reconnect races.
3. **Reconciliation on every successful order-feed (re)connect** (and on
   market-feed reconnect for price-relevant state):
   - Fetch `kotak.order_book()` and `kotak.positions()`.
   - For each tracked leg:
     - `PENDING_ENTRY` with a `complete` entry in the book → drive
       `on_order_update` (place SL) using the real fill.
     - `PENDING_ENTRY` with `rejected`/`cancelled` entry → mark leg accordingly
       (see Plan 04 for terminal-entry handling).
     - `ACTIVE`/`TRAILING` with a missing/terminal SL → re-place or mark
       `ERRORED` and alert.
   - Broadcast a `{"kind":"reconcile", ...}` notice so the UI can refresh.
4. **Do not block the loop.** Reconciling calls the broker synchronously; run
   them via `run_in_executor` (see backlog M2). Until M2 lands, keep reconcile
   itself off the tick path and guarded by a lock.
5. **`stop()`** must stop both supervisors and join the thread (already joins
   `self._thread`; ensure both tasks observe `self._stop`).

## Files / touch points
- `backend/app/sfeed_bridge.py` — split `_session`/`_supervise`; add
  `_reconcile()`; keep `map_feed_message` / `map_order_data` pure.
- `backend/app/synthetic_engine.py` — add an idempotent
  `reconcile(order_book, positions)` entry point (must not double-place an SL
  if one already exists — see Plan 04).
- `backend/app/ws_manager.py` — add a reconcile broadcast helper if needed.
- `backend/app/main.py` — lifespan unchanged in spirit (`bridge.stop()`).

## Verification (offline, extend `tests/test_sfeed_bridge.py`)
- Fake market ws that raises/ends while the order feed keeps producing → assert
  the order feed task is **not** cancelled and `engine.on_order_update` still
  fires.
- Simulate a fill arriving only in the order book (feed gap) → reconcile places
  the SL exactly once.
- `stop()` tears down both supervisors; thread exits within the join timeout.
- No duplicate SL when reconcile runs on top of an already-processed fill.

## Risks / rollback
- Requires Plan 04's idempotency guard to safely re-drive events; land 04 first
  or together.
- Reconciling while `on_order_update` runs needs serialization (backlog M3) to
  avoid races. Rollback = revert `sfeed_bridge.py` (single-file).
