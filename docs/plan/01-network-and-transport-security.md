# Plan 01 — Network & Transport Security (C1, C2, C3, C4)

Status: **IMPLEMENTED** (C2 = Tailscale option A) · Priority: P0 · Scope: `backend/app/main.py`,
`backend/app/auth.py`, `backend/app/routes/auth.py`, `backend/app/sfeed_bridge.py`
(only where it feeds the socket), `frontend/src/api.js`, `frontend/src/App.jsx`,
`Caddyfile`, `docker-compose.yml`, `deploy/*`.

Goal: close the unauthenticated `/ws` leak, serve real TLS, make the IP
allowlist correct, and stop the wildcard-CORS exposure.

---

## C1 — Authenticate and allowlist the `/ws` feed

### Problem
`main.py:77-85` accepts any WebSocket and `hub.register`s it. The socket pushes
`tick`, `order`, and `position` messages (`sfeed_bridge.py:189,192`) — i.e. the
full live order/position feed — to **any** client. Serveless auth is applied
nowhere on this path, and WebSockets bypass CORS, so any website a logged-in
user visits can open `wss://<host>/ws`.

### Design
1. **First-message auth (preferred).** Client sends `{"token": "<jwt>"}` as the
   very first frame. Server validates with the existing JWT logic, and closes
   with code 1008 if it doesn't arrive within a short window (e.g. 3 s).
   - Avoids putting the token in the URL/query string (proxy/access logs).
2. Reuse `jwt.decode(...)` + `sub` check. Refactor the token check in
   `auth.py` into a helper `decode_user_token(token) -> str | None` used by both
   `require_user` and the WS handler.
3. **Origin allowlist.** Validate the `Origin` header on the upgrade against a
   configured list; reject if absent/mismatched in production. (Same-origin SPA
   background + the deploy host.)
4. **Unregister on all exits.** Ensure `hub.unregister` runs on auth failure and
   on `WebSocketDisconnect`; currently only the disconnect path does it.
5. **Send nothing before auth.** Do not register the client until the first
   frame validates.

### Files / touch points
- `backend/app/main.py` — `ws_feed` handler: accept → await auth frame →
  validate → register → keepalive loop; close on timeout/mismatch.
- `backend/app/auth.py` — extract `decode_user_token`; add `ALLOWED_ORIGINS`
  handling.
- `backend/app/config.py` — `allowed_origins: str = ""` (comma-separated).
- `frontend/src/api.js` — on socket open, immediately send `{"token": <jwt>}`
  (token is already in `localStorage`).
- `frontend/src/App.jsx:112-153` — `ws.onopen` sends the auth frame; handle
  close code 1008 by clearing token and prompting re-login.

### Verification
- New test: unauthenticated socket → server closes 1008, receives no messages.
- New test: valid token → receives ticks; invalid → closed.
- Manual: open `wss://<host>/ws` from `websocat` without token → no data.
- Existing WS-consuming UI still receives ticks after login.

### Risks / rollback
- UI must send the auth frame before the server pushes anything; if ordering is
  wrong the UI shows no ticks. Keep server tolerant of a client `ping` frame but
  reject data use pre-auth. Rollback = revert the two files (no data migration).

---

## C2 — Serve real TLS (no domain required)

### Problem
`Caddyfile:6` is `:80 { reverse_proxy app:8000 }`; the TLS block is commented.
Login password, TOTP, JWT, and all order traffic are sent in cleartext. README
claims HTTPS.

### Options (pick ONE; recommendations in `README.md`)
- **A. Tailscale (recommended).** No public 80/443. `tailscale serve` or bind
  Caddy to the tailnet IP (`deploy/tailscale.md`). Trusted `*.ts.net` cert,
  no domain, nothing exposed beyond the tailnet.
- **B. `sslip.io` / `nip.io` + Let's Encrypt.** Domain-free, browser-trusted:
  ```caddy
  # <DASHED-IP>.sslip.io  e.g. 13-233-1-2.sslip.io
  13-233-1-2.sslip.io {
      reverse_proxy app:8000
  }
  ```
  Requires security-group + UFW to allow inbound 80 (ACME HTTP-01) and 443.
  Caddy auto-provisions/renews. No wildcard; one hostname per IP.
- **C. Caddy `tls internal`.** Self-signed; browser warnings. Fallback only.

### Files / touch points
- `Caddyfile` — replace `:80` block with the chosen option; if B, keep a small
  `:80` redirect or let Caddy handle redirect automatically.
- `deploy/ec2-setup.sh` — adjust UFW ports for the chosen option (open 443;
  open 80 only if using an ACME HTTP-01 hostname; for Tailscale, close 80/443).
- `docker-compose.yml` — expose 443 on the caddy service when going public.
- `README.md` — correct the HTTPS/quickstart/deploy text.

### Verification
- `curl -v https://<host>` shows a valid, trusted chain; `http://` → 301 to
  `https://`.
- Browser padlock valid (options A/B). `wss://` works from the UI.
- Confirm no cleartext: capture on the client and verify no plaintext
  credential/order frames.

### Risks / rollback
- ACME rate limits (option B) — retry via the other provider (`nip.io` ↔
  `sslip.io`). Keep the old `:80` block commented in-file for instant rollback.

---

## C3 — Correct the IP allowlist (defense in depth)

### Problem (`auth.py:58-119`)
- Live `.env` has `IP_ALLOWLIST=` **empty** → guard is a no-op.
- Trusts client-supplied `X-Forwarded-For` (`:115-116`) with no trusted-proxy
  config → spoofable.
- Only matches paths starting with `/api` (`:114`) → `/ws` and static bypass.
- Raises `HTTPException` from `BaseHTTPMiddleware.dispatch` → **HTTP 500**, not
  403 (verified on Starlette 0.48.0). Also `BaseHTTPMiddleware` passes
  `websocket` scopes straight through, so it cannot guard `/ws` at all.

### Design
1. Set `IP_ALLOWLIST` in `.env` (security group remains the primary control).
2. Reimplement as a **pure ASGI middleware** handling `scope["type"] in
   ("http", "websocket")` so WS is covered and responses are clean.
   - On deny: for http → `403` JSON response; for websocket → `websocket.close`
     with `1008` before accept.
3. Determine client IP from `request.client.host` by default; only honor
   `X-Forwarded-For` when a new `TRUST_PROXY: bool` setting is enabled **and**
   run Uvicorn with `--proxy-headers --forwarded-allow-ips=<caddy-service>` so
   the header is trustworthy. Otherwise ignore XFF.
4. Keep hostname/CIDR/wildcard parsing as-is (it's fine); move DNS resolution
   off the event loop (shared with backlog item).
5. Cover all paths when enabled (configurable prefix, default: everything).

### Files / touch points
- `backend/app/auth.py` — replace `IPAllowlistMiddleware` with ASGI version;
  add `TRUST_PROXY`, `ALLOWLIST` unchanged.
- `backend/app/config.py` — `trust_proxy: bool = False`.
- `backend/app/main.py` — mount order; add Uvicorn proxy-headers flags in
  `Dockerfile`/CMD or compose command.
- `.env.example` — document `TRUST_PROXY` and the Caddy service name.
- `.env` (operator action, not committed) — set `IP_ALLOWLIST`.

### Verification
- New tests: deny returns **403** (not 500) for http; WS from disallowed IP
  closes 1008; XFF ignored when `TRUST_PROXY=false`; honored when true.
- Manual spoof test: send `X-Forwarded-For: <allowed>` from a non-allowed IP →
  still denied with `TRUST_PROXY=false`.

### Risks / rollback
- Wrong `TRUST_PROXY` behind a proxy could lock out the operator — document and
  default to `false`. Rollback = revert middleware file.

---

## C4 — Restrict CORS

### Problem (`main.py:53-54`)
`allow_origins=["*"]` with `allow_credentials=True` echoes any Origin with
credentials allowed. The SPA is served same-origin (FastAPI mounts
`frontend/dist`), and dev uses the Vite proxy, so **CORS is unnecessary**.

### Design
- Default: remove `CORSMiddleware` entirely (same-origin only).
- Optional: add `ALLOWED_ORIGINS` setting; when set, configure CORS with
  `allow_credentials=False` and explicit origins, restricted methods/headers.

### Files / touch points
- `backend/app/main.py` — drop/adjust `add_middleware(CORSMiddleware, ...)`.
- `backend/app/config.py` — optional `allowed_origins`.
- `.env.example` — document.

### Verification
- Cross-origin `fetch` to `/api/*` from a foreign origin gets no CORS allow
  header (browser blocks).
- Same-origin UI works; dev Vite proxy works.

### Risks / rollback
- If any future client is cross-origin it must be added to `ALLOWED_ORIGINS`.
  Rollback = re-add the permissive middleware (discouraged).
