# Plan 05 — Backlog: Remaining Audit Findings

Status: **NOT IMPLEMENTED** · Priority: mixed (P2/P3) · Scope: various.

Deferred/leftover items from the audit, to be tackled after the P0 groups
(01, 03, 04). Each entry has a suggested approach and acceptance criteria.

---

## Deferred by owner

### H2 — No state persistence / no startup reconciliation
`SyntheticEngine.orders` is memory-only. `docker restart unless-stopped` loses
every tracker while broker SL orders may remain → trailing/market-exit never
fires and live positions are unmanaged.

Suggested approach (future "separate storage session"): persist trackers to
SQLite (WAL) or Redis; on boot, reconcile against `positions()` +
`order_report()` before enabling trading. Depends on Plan 03's reconcile
routine — reuse it.
Acceptance: restart mid-session re-attaches to open legs; no duplicate SL.

### C6 — JWT hygiene (accepted for now)
Default secret allowed (warn-only), 8 h TTL, no revocation, token in
`localStorage`. When revisited: fail closed on default secret, shorten TTL,
consider httpOnly cookie + CSRF or in-memory token.

---

## Medium — correctness & concurrency

### M1 — Validate symbols against the scrip master; bound `lots`
`routes/orders.py:46,105` builds `f"{instrument}{expiry}{strike}{option_type}"`
from free-form `expiry`/`strike`. If `token_for` misses, `instrument_token`
becomes the literal `"None"` (truthy) and the bridge subscribes a bogus token.
`SingleOrderBody.lots` (`schemas.py:23`) has no upper bound (fat-finger risk).
- Approach: reject if the symbol is absent from the token map (400
  "unknown contract"); validate `strike`/`expiry` shape; add `lots` bounds
  (`ge=1, le=<policy>`).
- Acceptance: bad strike/expiry → 400, no broker call; `lots` above the cap →
  422.

### M2 — Blocking broker I/O on the feed event loop
`cancel/modify_sl/place_stop_loss` are synchronous HTTP calls executed inside
`async for msg in ws` (`synthetic_engine.py:203,233,246`;
`sfeed_bridge.py:146,186`). They stall tick/order consumption and WS ping/pong
(→ disconnects, added latency).
- Approach: run broker calls on a `ThreadPoolExecutor` via
  `run_in_executor`, or route all broker commands through a single dedicated
  worker queue (also fixes M3).
- Acceptance: a simulated slow (2 s) broker call does not stop tick/order
  processing; no missed WS keepalives.

### M3 — Engine is not thread-safe
`orders` and tracker dicts are mutated from the sfeed thread and read by
FastAPI's threadpool with no lock → lost updates and CLOSING/cancel/exit races.
- Approach: single-writer command queue, or a lock around all engine mutations;
  simplest safe step is serializing every mutation onto the bridge loop.
- Acceptance: concurrency stress test (many ticks + order updates + reconcile)
  shows no torn/duplicate transitions.

### M4 — `Hub.broadcast` mutates while iterating; `hub.latest` cross-thread
`ws_manager.py:29-33` iterates `self.clients` while `await send_text` yields,
so `register/unregister` can raise `RuntimeError: Set changed size during
iteration`. Iterate `list(self.clients)`. Also `hub.latest` is written from the
bridge thread and read on the loop.
- Approach: snapshot the client set; make `latest` access explicit/atomic.
- Acceptance: connect/disconnect storm during broadcasts raises no exception.

---

## Low / hardening

| Item | Location | Fix |
|------|----------|-----|
| No login rate-limit/lockout | `routes/auth.py:15-19` | Add per-IP throttle / backoff. |
| `app_login(body: dict)` unvalidated | `routes/auth.py:16` | Use a Pydantic model. |
| Container runs as root; no healthcheck/limits | `Dockerfile`, `docker-compose.yml` | Add `USER`, `HEALTHCHECK`, `mem_limit`/`cpus`. |
| UFW refresh never deletes old IPs | `deploy/ufw-refresh.sh:9-15` | Actually `ufw delete` prior home-IP rules before adding; a stale dynamic IP stays whitelisted forever otherwise. |
| Blocking DNS in async middleware | `auth.py:78` | Resolve hostnames in an executor / cache off-loop. |
| `initiate_login` leaks pending clients | `kotak_client.py:57-62` | Discard/overwrite any existing `pending_client`. |
| `_wanted` subscription grows unbounded | `sfeed_bridge.py` | Unsubscribe removed watchlist legs; cap the set. |
| No CSP/security headers | `frontend/index.html` | Add CSP + `X-Content-Type-Options`, `Referrer-Policy`. |

---

## Performance / latency

| Item | Location | Fix | Expected effect |
|------|----------|-----|-----------------|
| `by_tokens` O(n) scan per tick | `synthetic_engine.py:37-43` | Index trackers by `instrument_token` (`dict[str, list]`). | O(1) tick dispatch at scale. |
| One coroutine scheduled per tick | `sfeed_bridge.py:194-201` | Coalesce ticks per client (e.g. 50 ms flush). | Fewer main-loop wakeups. |
| N per-symbol quote calls | `routes/market.py:93-105` | Single batched `/batch-quotes` (endpoint caps 50/call); keep the TTL cache. | Fewer REST round trips, fewer 429s. |
| JSON serialization on every broadcast | `ws_manager.py:28` | Use `orjson`; push only watched symbols per client. | Lower CPU per message. |
| Worker model | deploy | Uvicorn single worker (stateful); ensure `uvloop` (already via `uvicorn[standard]`). | Avoid split state. |

---

## Suggested sequencing
1. Plan 01 (network/transport) — P0.
2. Plan 04 (execution integrity) — P0, lands the idempotent SL path.
3. Plan 03 (feed decoupling) — P0, reuses Plan 04's guarded SL + reconcile.
4. Plan 02 (broker TLS) — P1.
5. M2 + M3 together (off-loop broker worker + single-writer) — unlocks safe
   reconcile and removes latency stalls.
6. M1, M4, then Low/Perf; H2 as a dedicated storage session.
