# Plan 06 — Order-Feed Liveness, SL Guarantee & Exit Integrity

Status: **DRAFT — not implemented.** · Priority: **P0 (trading safety)** · Scope:
`backend/app/sfeed_bridge.py`, `backend/app/synthetic_engine.py`,
`backend/app/kotak_client.py`, `backend/app/routes/orders.py`,
`backend/app/routes/market.py`, `backend/app/main.py`, `frontend/src/App.jsx`,
`frontend/src/api.js`, tests, `docker-compose.yml` / config.

Source: live incident log `logs/compose_logs_2026-10-21.txt` (times below are
the log-prefix **UTC**; add 5:30 for IST).

---

## 1. Incident summary (what actually happened)

13 entry orders were placed between 08:11 and 09:59; only 6 got an SL.

- Last SL placed: **09:29:26** (`order 261021000647985`, trigger 316.60).
- Target hit on that leg at **09:30:00** (`SENSEX26O2273300CE @ 296.05`).
- Its SL cancel (`POST /quick/order/cancel`, body `on=261021000647985`) failed
  with HTTP/2 `<StreamReset ... remote_reset:True>` after **~61 s**
  (`compose_logs_2026-10-21.txt:775`).
- Last order-feed event ever received: **09:31:02**
  (`261021000647985 -> modify validation pending (CLOSING)`, `:779`).
- No reconnect, no reconcile, no order updates for the rest of the session.
- Six entries after that (**09:36:47, 09:42:11, 09:44:34, 09:54:05, 09:56:05,
  09:59:25**, all `SENSEX26O2273400CE`) have **no SL, no fill confirmation, and
  no synthetic lifecycle logs** (`:840, :875, :886, :949, :962, :974`).

**Excluded from this plan** (owner confirms these were manual, not defects):

- 08:24:10 entry cancellation (`:203-205`) — manual cancel from the broker app.
- 09:30 target exit not sent by this app — manually flattened in the broker app.
- 08:13:22 / 09:05:18 SL trigger modifications (`:152-155`, `:415-418`) —
  manual modifications in the broker app.

Those manual actions are not failures in themselves, but they exposed a real
gap: the app does not converge after external intervention (see RC5). The
manual exit also masked the real stuck-`CLOSING` defect (RC3), which still
needs fixing.

---

## 2. Root causes

### RC1 (primary) — Blocking broker I/O on the feed event loop

`on_tick` runs on the bridge event loop (`sfeed_bridge.py:216-225`) and
`_eval_leg` calls `self.broker.cancel(...)` synchronously
(`synthetic_engine.py:372-376`). That cancel took **61 s** (SDK timeout + retry),
so **both** the market and order feed supervisors were blocked for 61 s on the
same loop. This is exactly backlog **M2** ("stall tick/order consumption and WS
ping/pong → disconnects, added latency"), now confirmed in production.

A separate 61 s stall occurred on entry #1 (`place_order` logged 08:10:52,
request returned 08:11:53, `:128` vs `:132`). That one ran on the FastAPI
threadpool, so it did not starve the feed, but it delayed SL placement by a
minute (see RC7).

### RC2 — No feed liveness detection; reconnect only when the iterator ends

`_order_session` does `async with ... as feed: async for msg in feed`
(`sfeed_bridge.py:229-239`). If the SDK stream stops yielding without ending,
the `async for` never returns, so `_supervise_orders` never retries. There is
no last-message watchdog. `reconcile()` runs **only** on (re)connect
(`sfeed_bridge.py:237`), so with a hung feed the safety net never runs; the last
reconcile in the log is 09:03:25 (`:391`).

### RC3 — Target exit is gated on a feed event that may never arrive

Target hit sets `CLOSING` then calls `cancel()` (`synthetic_engine.py:367-376`).
The actual market exit is only placed when a subsequent **`cancelled` order
event** arrives (`synthetic_engine.py:124-149`). If the cancel fails at the HTTP
layer — as it did at 09:31:01 — the SDK merely logs `api_request_failed` and
returns, so `_eval_leg`'s `except` never fires; the leg stays `CLOSING` and no
exit is placed. `reconcile()` only drives `CLOSING` legs when `exit_order_id`
exists (`synthetic_engine.py:339-344`); here it is `None`, so the leg is stuck
forever.

### RC4 — SL placement depends solely on a live feed `complete` event

`_place_sl` is only reached from `on_order_update` when the entry reports
`complete` (`synthetic_engine.py:103-107`). With RC1/RC2 there is no guarantee
the event arrives. There is no periodic order-book poll that would discover
"entry filled, no SL exists" and repair it.

### RC5 — Manual/external actions are not reconciled

The broker-app exit, cancel and SL modify prove the engine only knows what the
live feed tells it:

- A manual **exit** creates an order id the engine does not know; `by_any_id`
  returns `None` and `on_order_update` silently ignores it
  (`synthetic_engine.py:94-99`). The tracker stays `ACTIVE`; a still-working SL
  can later fire and re-open/flip the position.
- A manual **SL modify** is only logged (`synthetic_engine.py:101`); the new
  trigger is never read into `stop_loss_price`, and `map_order_data` does not
  even carry a trigger price (`sfeed_bridge.py:38-53`). The tracker's stop and
  the UI keep showing the old price.
- A manual **cancel** works only because the feed delivered `cancelled`; with a
  dead feed it would not be seen either.

### RC6 — Errors are swallowed and can look like "flat"

`KotakService.positions()` catches every failure and returns `{}`
(`kotak_client.py:406-413`), while the route returns **200 OK**
(`market.py:108-111`). Positions timeouts at 08:20:11, 08:56:41 and 09:28:10
(`:176`, `:364`, `:734`) were therefore indistinguishable from "no positions".
Quote failures similarly return `None` / 502 with the opaque code `(0)`
(`:44-49`, `:593-596`).

### RC7 — SDK retry / duplicate-order risk on timeout

The two ~61 s stalls are consistent with a 30 s SDK timeout plus one retry
(`timeout: 30`, `:44`). If the first attempt reached the exchange, the retry can
create a second order that the engine never tracks (unknown id is ignored). The
`tag` is set on entries (`orders.py:144-145`) but not on SL/exit orders, and
nothing queries the book by tag after a timeout.

### RC8 (secondary) — Watch-call storm and unbounded subscriptions

**458** `POST /api/market/watch` calls in one session, in bursts of ~40–60 per
second (`:440-557`, `:658-733`, `:788-818`, `:884-948`), and 257 position polls.
`ensure_option` only ever adds; `_wanted` grows 6 → 12 and is never pruned
(`sfeed_bridge.py:126-140`). This likely aggravates the Kotak REST timeouts in
RC6 (the client comment already warns about 429/dropped connections).

---

## 3. Invariants this plan must establish

1. **No broker HTTP call ever runs on the feed event loop.** All
   place / modify / cancel / order-book / positions calls go through one
   command worker with explicit timeouts.
2. **No exchange position is left without a live SL or an explicit ALERT**
   for longer than `SL_GUARANTEE_SEC` (default 15 s).
3. **Never exit twice and never flip a position.** Exit is idempotent; a failed
   cancel never triggers a second market exit.
4. **The engine converges without the feed.** A periodic reconcile repairs
   missed fills, missing SLs, and stuck `CLOSING`.
5. **External (broker-app) actions converge:** flat position ⇒ tracker
   `CLOSED` and any working SL cancelled; externally modified SL ⇒ tracker
   trigger updated.
6. **UI never shows a stale/failed read as a valid empty state.**

---

## 4. Work packages

### WP1 — P0: Off-loop, single-writer broker command worker (M2 + M3)

**Problem:** RC1, RC7; also races between feed/UI/reconcile mutations (M3).

**Design:**
- Add a `BrokerWorker` (dedicated `ThreadPoolExecutor(max_workers=1)` or a
  bounded command queue on a dedicated thread) owning every synchronous broker
  call: `place_order`, `place_stop_loss`, `modify_sl`, `cancel`,
  `order_book`, `positions`, `average_fill_price`.
- All engine transitions are posted to a single serialized queue
  (single-writer) so `on_tick`, `on_order_update`, reconcile and HTTP routes
  cannot interleave a `CLOSING`/cancel/exit sequence.
- Per-call timeout (config `BROKER_CALL_TIMEOUT_SEC`, default e.g. 10 s) and a
  hard cap on total retries. Never let one command block the loop for 30–60 s.
- Emit a `command` lifecycle log: `queued/sent/ok/failed/timeout` with order id
  and elapsed ms, so a 61 s stall is immediately visible.

**Files:** new `backend/app/broker_worker.py` (or inside `sfeed_bridge.py`),
`synthetic_engine.py`, `kotak_client.py`, `routes/orders.py`.

**Verification:** unit test with a fake broker that sleeps 2 s (and 65 s) — ticks
and order updates continue to be processed; no WS keepalive gap; engine state
serialized under concurrent ticks + updates + reconcile.

---

### WP2 — P0: Order-feed liveness watchdog + periodic reconcile

**Problem:** RC2.

**Design:**
- Split the order session into **reader** and **handler** tasks (bounded queue).
  The reader only reads the socket; the handler applies events.
- Track `last_order_msg_ms` and `last_order_ok_ms`; a watchdog checks every
  `FEED_WATCHDOG_SEC` (e.g. 5 s). If no message/heartbeat for
  `FEED_STALE_SEC` (e.g. 30 s), force-close the socket and recreate the session
  (backoff, existing `retry_sec`).
- Run `reconcile()` on every connect **and periodically** every
  `RECONCILE_SEC` (default 30 s), independent of connect events. Run it through
  `BrokerWorker` so it never blocks the loop.
- Reconcile summary must include `checked / driven / repaired / errored /
  naked` and be pushed to the UI.
- Extend `/api/health` and the UI with feed age (`last_order_msg_ms`,
  `last_tick_ms`) so a silent feed is visible before it costs an SL.

**Files:** `sfeed_bridge.py`, `synthetic_engine.py`, `main.py`,
`frontend/src/App.jsx`.

**Verification:** fake feed that yields once then hangs — watchdog reconnects
within `FEED_STALE_SEC + retry_sec`; reconcile runs on the new session and on
schedule. Fake feed that ends normally — existing behavior preserved.

---

### WP3 — P0: SL guarantee / naked-position guard

**Problem:** RC4.

**Design:**
- On entry acceptance, register the leg and set
  `sl_guarantee_due_ms = now + SL_GUARANTEE_SEC`.
- Every reconcile tick, for every `PENDING_ENTRY` leg, look up the entry in the
  broker book:
  - terminal filled → `_place_sl` (idempotent, existing fill-price gate);
  - terminal rejected/cancelled/expired → terminal state;
  - still open after the guarantee window → alert (or optional auto-cancel
    policy), never silently wait.
- For every `ACTIVE`/`TRAILING` leg, verify the SL exists and is working in the
  book. If missing/rejected/cancelled → re-place **once** from the stored fill
  price, otherwise `ERRORED` + `naked_position` alert.
- Persist the last successful reconcile timestamp per leg and show it in the UI.
- Never place a duplicate SL: existing `sl_placed`/`sl_order_id` guard stays the
  single entry point.

**Files:** `synthetic_engine.py` (reconcile + alerting), `sfeed_bridge.py`
(schedule), `routes/orders.py` (expose `last_reconcile_ms`), UI.

**Verification:** simulate 6 accepted entries with a dead feed and a book that
shows all 6 filled → all 6 SLs placed exactly once on the next reconcile tick.
SL missing from the book for an `ACTIVE` leg → re-placed once / `ERRORED` on
second failure.

---

### WP4 — P0: Cancel/exit integrity (no stuck `CLOSING`)

**Problem:** RC3.

**Design:**
- Do **not** gate the market exit solely on the feed's `cancelled` event.
- Replace `broker.cancel()` + wait with a command that returns a result and
  polls the order book (via `BrokerWorker`) until the SL is terminal
  (`cancelled`/`complete`/`rejected`) or a deadline passes:
  - SL already **complete** → position was closed by the SL; **do not** market
    exit; mark `CLOSED` with `exit_reason=SL_HIT`.
  - SL **cancelled** (confirmed) → place exactly one market exit (idempotent
    `exit_order_id` guard), then wait for exit terminal.
  - cancel **fails/timeouts** → retry with backoff; if still not confirmed:
    `ERRORED` + `EXIT_BLOCKED_SL_STILL_WORKING` alert. **Do not** fire a market
    exit while an SL may still be working (double-exit/flip risk).
- `reconcile()` must drive `CLOSING` legs with **no** `exit_order_id`:
  re-check the SL, confirm cancel, then exit. A leg must never remain `CLOSING`
  across two reconcile ticks.
- Make cancel/exit cancel-safe on duplicate requests (existing duplicate-cancel
  guard is good; extend it to the exit path).
- Add an explicit operator action "Force exit (manual confirm)" for the
  blocked case, which cancels the SL and exits in one audited flow.

**Files:** `synthetic_engine.py`, `sfeed_bridge.py` (cancel confirmation),
`routes/orders.py` (force-exit endpoint), UI.

**Verification:** fake cancel returns StreamReset / never delivers `cancelled` →
leg is retried and either exits once or ends `ERRORED` + alert; no second exit.
SL completes during cancel → no market exit. Duplicate `cancelled` frames →
exactly one exit.

---

### WP5 — P1: Reconcile manual / external actions

**Problem:** RC5 — and this is what the owner had to do by hand all day.

**Design:**
- Periodic `positions()` snapshot (through `BrokerWorker`, with freshness
  tracking) plus `order_report()`:
  - Tracker `ACTIVE`/`TRAILING`/`CLOSING` but net position for the symbol is
    **flat/absent on two consecutive fresh snapshots** → cancel any working
    SL/entry for that symbol, mark `CLOSED`, `exit_reason=EXTERNAL_FLAT`. Never
    act on a single snapshot that could be the result of the RC6 empty-body
    timeout.
  - Tracker `PENDING_ENTRY` but entry is terminal at the broker → drive the
    terminal state (cancel/reject/fill).
  - SL trigger changed at the broker (feed or book) → update
    `stop_loss_price`/`sl_quantity` in the tracker. Extend `map_order_data` /
    `map_book_entry` to carry `trigger_price` (`trgPrc`/`trigger_price`).
- Expose `last_synced_ms` and `source=feed|reconcile|external` per leg.
- Never auto-re-place an SL while a manual modify is in flight; book state wins
  after it settles.

**Files:** `synthetic_engine.py`, `sfeed_bridge.py`, `kotak_client.py`
(positions/book helpers), UI.

**Verification:** fake broker book/positions with an unknown manual exit order
and flat position → leg closes, working SL is cancelled once. Manual trigger
modify reflected in tracker/UI. Single empty positions response (timeout) does
**not** close a live leg.

---

### WP6 — P1: In-app order management (so the broker app is not needed)

**Problem:** owner had no way to cancel from this app; the backend endpoint
exists but there is no UI (`routes/orders.py:241-247`, no frontend usage).

**Design:**
- Add an **Orders** panel fed by `GET /api/orders/orderbook`:
  working/pending/rejected order rows, symbol, side, qty, price, trigger,
  filled qty, status, age.
- Row actions: **Cancel** (`POST /api/orders/cancel/{id}`), **Modify SL**
  (`POST /api/orders/modify-sl/{id}?trigger=...`, quantity from the tracker),
  and **Exit at market** (existing `/exit-position`).
- Confirmation for destructive actions; disable while in flight; show the
  server result (not just HTTP 200).
- Add "Cancel all working orders for this symbol" (with confirmation), which
  loops the SL/entry ids from the tracker.
- All actions go through `BrokerWorker`, never direct from the request thread
  into a half-updated engine.

**Files:** `frontend/src/App.jsx`, `frontend/src/api.js` (no new endpoints
needed except force-exit if WP4 adds it), `routes/orders.py`.

**Verification:** manual cancel of an entry/SL from the UI reaches the broker
and the tracker transitions correctly; double-click cannot double-cancel; the
broker-app workaround is no longer needed.

---

### WP7 — P1: Stop swallowing market-data errors

**Problem:** RC6.

**Design:**
- `positions()` and quote helpers return `(data, error, fetched_at)` or raise;
  the route returns **502** on broker failure (or 200 with
  `{"stale":true,"error":...,"positions":<last_good>}` — pick one and document).
- Never return an empty map with 200 for a failed call.
- UI: show "positions unavailable / last updated HH:MM:SS" instead of an empty
  table; same for watchlist LTPs.
- Fix the `(0)` error path to include the real exception text; keep credentials
  out of logs.

**Files:** `kotak_client.py`, `routes/market.py`, `frontend/src/App.jsx`.

**Verification:** simulated positions timeout → 502/stale flag; UI shows the
error and does not imply flat. Simulated recovery clears the banner.

---

### WP8 — P2: Timeout/retry duplicate-order protection

**Problem:** RC7.

**Design:**
- Set the SDK HTTP timeout explicitly (`BROKER_CALL_TIMEOUT_SEC`) and disable
  unbounded internal retry; own the retry policy.
- Put a `tag` on **every** order (entry, SL, exit) with a deterministic id
  (client tag / leg id / action), so duplicates are identifiable.
- On timeout/uncertain result: do **not** blindly retry a place. Query
  `order_report()` and match by tag/symbol/qty/side before retrying; if found,
  adopt the order id into the tracker.
- Reconcile must not treat an unknown id as "ignore" when it carries one of our
  tags; log/alert on it.

**Files:** `kotak_client.py`, `routes/orders.py`, `synthetic_engine.py`.

**Verification:** fake broker that accepts the first call then times out —
exactly one broker order after retry; discovered tag is adopted. Simulated
timeout with no order → one retry.

---

### WP9 — P2: Fix the watch-call storm / subscription growth

**Problem:** RC8.

**Design:**
- Frontend: send `/market/watch` only when the watchlist set actually changes
  (debounced), not on every render/tick; coalesce changes into one request.
- Backend: dedupe identical legs, coalesce `ensure_option` calls, and rate-limit
  per session.
- Add reference-counted subscriptions / an `unwatch` path and prune `_wanted`
  (already backlogged as "`_wanted` grows unbounded").
- Optionally cap `batch-quotes` legs per call (already 50) and reuse the TTL
  cache.

**Files:** `frontend/src/App.jsx`, `routes/market.py`, `sfeed_bridge.py`.

**Verification:** watchlist edits produce one watch call; no burst on tick
updates; subscription count tracks the live watchlist.

---

### WP10 — P2: Observability / alerts

**Design:**
- Log explicit transitions: `SL placed`, `SL rejected`, `SL hit`, `target hit`,
  `exit placed`, `exit rejected`, `naked position`, `feed stale`, `reconcile`.
- Every reconcile summary: `checked/driven/repaired/errored/naked`, with ids.
- `/api/health`: `feed_order_age_ms`, `feed_tick_age_ms`, `last_reconcile_ms`,
  `open_legs`, `naked_legs`, `errored_legs`.
- UI banner for `ERRORED`, naked, stale feed, stale positions.
- Keep the “reconnect exhausted” log but make it clear it is expected (SDK
  auto-reconnect disabled on purpose) and that the supervisor owns it.

**Verification:** simulated feed hang raises a visible alert within
`FEED_STALE_SEC`; a missing SL is visible in `/api/health` and the UI.

---

### WP11 — P3: Small safety / hygiene items

- Warn (or block) when a new entry is placed on a symbol with an existing
  working/pending leg — the six stacked `73400CE` shorts would have been
  visible before they became untracked.
- Warn when a SELL limit is above LTP / BUY limit below LTP by more than
  `MAX_PASSIVE_OFFSET` (the 08:23 order at `ref+10` never filled).
- Fix favicon 404.
- Make log timestamps consistent (UTC prefix vs `+05:30` JSON), or add a single
  timezone note to the compose logging config.
- Prune old `CLOSED` trackers (or archive) so `by_any_id`/reconcile stay small.

---

## 5. Sequencing

1. **WP1** (off-loop command worker + single-writer) — unblocks everything and
   removes the 61 s loop stall that killed the feed.
2. **WP2** (watchdog + periodic reconcile) — makes the feed self-healing and
   guarantees a safety net without the feed.
3. **WP3** (SL guarantee) + **WP4** (cancel/exit integrity) — the two direct
   causes of the missing SLs and the stuck exit.
4. **WP5** (manual/external reconcile) — supports the current broker-app
   workflow immediately; should land before relying on manual actions again.
5. **WP6** (in-app cancel/modify/exit UI) — removes the need for the broker app.
6. **WP7** + **WP8** — honest errors and duplicate-order safety.
7. **WP9** + **WP10** — load and observability.
8. **WP11** — hygiene.

Suggested PRs: `06a` = WP1 + WP2 (infrastructure), `06b` = WP3 + WP4 (SL/exit),
`06c` = WP5 + WP6 (manual sync + UI), `06d` = WP7 + WP8, `06e` = WP9 + WP10 +
WP11.

---

## 6. Tests to add

Reuse the existing fake-broker/fake-feed style in `backend/tests`:

- `test_feed_watchdog.py` — feed yields once then hangs → forced reconnect;
  reconcile runs on connect and periodically.
- `test_sl_guarantee.py` — entry fills only in the order book (no feed event) →
  SL placed once on reconcile; dead feed + 6 fills → 6 SLs.
- `test_exit_integrity.py` — cancel StreamReset / no `cancelled` event → retry,
  no double exit, terminal or `ERRORED`; SL completes during cancel → no exit;
  duplicate cancels → one exit.
- `test_external_sync.py` — manual exit (unknown id) + flat positions → tracker
  `CLOSED`, SL cancelled; manual trigger modify → tracker updated; one empty
  positions snapshot → no action.
- `test_offloop_broker.py` — 2 s and 65 s fake broker calls do not stall ticks
  or order handling.
- `test_timeout_dedupe.py` — accepted-then-timeout place → one order, tag
  adopted.
- Frontend: watch change → one debounced call; cancel/modify/SL confirm flow.

Run: `cd backend; uv run pytest tests/ -q` (currently 25 passing).

---

## 7. Acceptance criteria (incident replay)

Given a replay of 2026-10-21:

- The 09:30 cancel StreamReset does **not** block tick/order processing; the
  feed stays alive (watchdog age < `FEED_STALE_SEC`).
- If the feed still dies, the periodic reconcile places the SLs for the six
  09:36–09:59 fills within `SL_GUARANTEE_SEC` of seeing them in the book, and
  raises a visible alert if it cannot.
- The 09:30 leg either exits once at market after a confirmed cancel, or ends
  `ERRORED` with `EXIT_BLOCKED_SL_STILL_WORKING` — never silently `CLOSING`.
- A manual broker-app exit converges to `CLOSED` within one reconcile interval,
  and its leftover working SL is cancelled.
- A manual broker-app SL modify is reflected in the tracker/UI.
- Positions/quote timeouts surface as errors/stale, never as an empty
  portfolio.
- `/api/health` and the UI show feed/reconcile freshness and naked/errored legs
  at all times.
- Cancelling an entry or SL is possible from this app; no broker-app workaround
  required.

---

## 8. Risks / rollback

- WP1 touches every broker call path; land behind a small feature flag
  (`BROKER_WORKER=1`) and keep the old synchronous path selectable for one
  release. Rollback = flag off.
- Reconcile can place/cancel real orders. Every repair must be idempotent and
  guarded by `sl_order_id`/`exit_order_id`; add a dry-run mode first
  (`RECONCILE_DRY_RUN=1`) that only logs intended actions, run it for a session,
  then enable.
- External-flat detection must require two fresh snapshots; otherwise an RC6
  empty response could cancel a live SL.
- Forcing a market exit while an SL may still be working is forbidden; keep the
  operator force-exit manual and audited.

---

## 9. Open questions

1. Auto-cancel vs alert when an entry stays `PENDING_ENTRY` beyond
   `SL_GUARANTEE_SEC` (default: alert only; configurable).
2. For a failed SL cancel at target, do we allow a market exit with a
   same-size opposite order once the SL is **confirmed cancelled**, or require
   an operator force-exit if confirmation fails twice? (default: retry, then
   block + alert).
3. Should external-flat reconciliation cancel a still-working SL automatically,
   or only alert? (default: auto-cancel — a flat position must not keep a live
   SL, which can flip the position).
