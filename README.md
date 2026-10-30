# KotakAPI Trader

Manual-discretion web trading desk on top of
[kotak-neo-python](https://github.com/Kotak-Neo/kotak-neo-python) (PyPI
`kotakneoapi` >= 3.0.7). You place orders from the browser; the backend holds the
Kotak session and runs the synthetic bracket-order engine on a low-latency box
(Mumbai `ap-south-1` recommended).

Phase 1 is **manual only** — no auto-strategy. The algo hook is
`backend/app/synthetic_engine.py::on_tick`.

> Suggested repository slug: **`kotak-neo-trader`** (used in the commands below).

## Features

- Single CE / PE buy and **Buy-Both straddle** (MKT / ML / L-1 entry modes).
- **Synthetic BO**: entry fill -> auto SL; target hit -> market exit (cancel SL,
  then exit) or trailing-stop ratchet.
- Live price ticks + order feed over one authenticated `/ws` socket; positions
  polling; cancel / modify-SL / exit from the UI.
- Expiry + strike pickers built from the broker scrip master (NIFTY step 50,
  SENSEX step 100 fallback).
- Reconciliation, feed watchdog, SL-guarantee and exit-integrity guards
  (see `docs/plan/`).

## Prerequisites

| Tool | Version | Used for |
|------|---------|----------|
| Python + [uv](https://docs.astral.sh/uv/) | 3.12+ | backend |
| Node.js + npm | 20+ | frontend |
| Docker + Compose | current | production image |
| Tailscale | current | recommended TLS / private network |

You also need Kotak Neo API credentials (consumer key, mobile number, UCC, MPIN)
and the Kotak mobile app for the daily TOTP.

## Local setup

```bash
git clone https://github.com/<you>/kotak-neo-trader.git
cd kotak-neo-trader

# 1. Create the root .env (Docker Compose and the backend both read this file)
cp .env.example .env          # PowerShell: Copy-Item .env.example .env
```

Fill in `.env` before starting:

```bash
# From backend/: generate the app password hash and a JWT secret
cd backend
uv sync
uv run python -c "import bcrypt; print(bcrypt.hashpw(b'YOUR-PASSWORD', bcrypt.gensalt()).decode())"
uv run python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Put the bcrypt output in `APP_PASSWORD_BCRYPT` and the random string in
`JWT_SECRET`. Then set `KOTAK_CONSUMER_KEY`, `KOTAK_MOBILE_NUMBER`, `KOTAK_UCC`
and `KOTAK_MPIN`. For local use leave `IP_ALLOWLIST=` empty; for a deployed box
set it to a tailnet CIDR such as `100.64.0.0/10`.

Run the backend (from `backend/`):

```bash
uv run uvicorn app.main:app --reload
```

Run the frontend in a second shell (from `frontend/`):

```bash
npm install
npm run dev
```

Open <http://localhost:5173>, log in with `APP_USERNAME` + your password, then
use **Kotak TOTP login** and type the live TOTP from the Kotak app.

## Tests and build

```bash
# backend (from backend/)
uv run --group dev pytest tests/ -q     # 243 tests

# frontend production build (from frontend/)
npm run build
```

## Deploy to EC2 (Docker + Tailscale, no domain needed)

Tested target: Ubuntu 24.04, `ap-south-1`. The stack publishes Caddy on loopback
only; `tailscale serve` terminates TLS with a trusted `*.ts.net` certificate.
Public inbound ports stay closed except SSH from your IP/CIDR.

```bash
# On the EC2 box
git clone https://github.com/<you>/kotak-neo-trader.git
cd kotak-neo-trader

cp .env.example .env && nano .env
# Required production values:
#   APP_PASSWORD_BCRYPT, JWT_SECRET
#   KOTAK_CONSUMER_KEY, KOTAK_MOBILE_NUMBER, KOTAK_UCC, KOTAK_MPIN
#   IP_ALLOWLIST=100.64.0.0/10
#   ALLOWED_ORIGINS=https://<machine>.<tailnet>.ts.net

./deploy/ec2-setup.sh <your-home-ip-or-cidr>   # Docker, UFW (SSH only), Tailscale
# Dynamic home IP? refresh only the SSH rule from cron:
#   */5 * * * * /path/to/repo/deploy/ufw-refresh.sh <ddns-host>
sudo tailscale up                              # join the tailnet (install it on your PC too)
docker compose up -d --build
sudo tailscale serve --bg --https=443 http://127.0.0.1:80
```

Browse `https://<machine>.<tailnet>.ts.net`. Both the tailnet and the app login
are required. See `deploy/tailscale.md` for details and `Caddyfile` for the
public-hostname (`sslip.io` + Let's Encrypt) fallback.

## Configuration

All settings live in the repo-root `.env` (see `.env.example` for the full,
commented list). The main groups are:

| Variables | Purpose |
|-----------|---------|
| `APP_USERNAME`, `APP_PASSWORD_BCRYPT`, `JWT_SECRET`, `JWT_TTL_MIN` | Browser login / JWT session |
| `IP_ALLOWLIST`, `IP_ALLOWLIST_REFRESH_SEC`, `TRUST_PROXY` | Network allowlist (IP, CIDR, wildcard, or DDNS hostname) |
| `ALLOWED_ORIGINS` | Browser Origin allowlist for `/ws` and optional CORS |
| `KOTAK_CONSUMER_KEY`, `KOTAK_MOBILE_NUMBER`, `KOTAK_UCC`, `KOTAK_MPIN` | Broker login (server-side only) |
| `SCRIP_MASTER_CA_BUNDLE` | Optional pinned CA bundle for the scrip-master download |
| `NIFTY_LOT`, `SENSEX_LOT` | Lot sizes used by the UI/engine |
| `FEED_*`, `RECONCILE_SEC`, `SL_*`, `EXIT_*`, `ORDER_*`, `WATCH_*` | Safety timers, retry budgets and reconcile behaviour (Plan 06) |

The app logs a warning at startup if `APP_PASSWORD_BCRYPT` is empty or
`JWT_SECRET` is still the default/placeholder (`change-me...`).

## Security model

1. **Transport**: Tailscale TLS (recommended) or Caddy + a public hostname.
2. **Network allowlist**: `IP_ALLOWLIST` covers `/api` and `/ws`; mismatches get
   HTTP `403` / WebSocket `1008`. `X-Forwarded-For` is honored only with
   `TRUST_PROXY=true` behind a trusted proxy.
3. **App login**: bcrypt password -> short-lived JWT (`APP_USERNAME`).
4. **Authenticated feed**: `/ws` requires the JWT as its first frame and an
   allowed `Origin`; nothing is streamed before auth.
5. **Broker secrets**: consumer key / UCC / MPIN stay in the server `.env`; the
   browser only sends TOTP digits for the morning login.
6. **No wildcard CORS**: same-origin by default; cross-origin needs an explicit
   `ALLOWED_ORIGINS`.

## Before the first live order

1. Confirm the Kotak status panel shows a live session and the scrip master
   loaded (expiries/strikes appear).
2. Place a **1-lot** MIS order and verify the protective SL in the Kotak app.
3. Test modify-SL, cancel and exit once; confirm the leg closes in both UIs.
4. Keep the Kotak app available as a manual fallback.
5. Treat a backend restart as a state reset: synthetic-leg tracking is in-memory
   (persistence is deferred), so verify open positions in the broker app.

## Repo layout

```
backend/app/
  main.py config.py auth.py kotak_client.py
  broker_worker.py sfeed_bridge.py synthetic_engine.py ws_manager.py
  routes/{auth,market,orders}.py
backend/tests/                 # 243 tests, SDK v3 compatibility pinned
frontend/src/{App.jsx,api.js}  # Vite + React UI
deploy/                        # EC2 bootstrap + Tailscale notes
docs/plan/                     # security/architecture remediation plans
Dockerfile docker-compose.yml Caddyfile
```

## Known limitations / roadmap

- No auto-strategy; Phase 2 plugs into `synthetic_engine.py::on_tick`.
- State persistence across restarts is deferred (Plan 05, H2).
- JWT is a single shared app session with no refresh/revocation (Plan 05, C6
  accepted for now).
- Legacy TransTrader conveniences such as L-2 entry presets are not implemented.

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Login returns `401` | `APP_PASSWORD_BCRYPT` is set to a real bcrypt hash, not the placeholder |
| `403` / WebSocket `1008` | Client IP is in `IP_ALLOWLIST`; `ALLOWED_ORIGINS` matches the browser URL |
| Scrip master / TLS error | Network access to the broker CSV host; only pin `SCRIP_MASTER_CA_BUNDLE` if you have a verified chain |
| Orders rejected with no instrument token | Scrip master did not load; re-login and check the backend log |
| Feed shows stale | Check `/api/health` and the order-feed watchdog logs; reconcile runs every `RECONCILE_SEC` |
| `SL_MODIFY_TIMEOUT_SEC` startup error | It must be greater than `BROKER_CALL_TIMEOUT_SEC` + 1s |
