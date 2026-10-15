# KotakAPI Trader

Manual-discretion web trading interface on top of [kotak-neo-python](https://github.com/Kotak-Neo/kotak-neo-python).
Phase 1 (this repo): **no auto-strategy** — you place orders yourself from your PC, execution happens on a
low-latency EC2 box (Mumbai `ap-south-1` recommended) that holds the Kotak session + synthetic-BO engine.
Phase 2 hook: `backend/app/synthetic_engine.py::on_tick` is where algo triggers will plug in.

## Feature parity with TransTrader (PyQt)
- Single CE / PE buy + **Buy-Both straddle** (MKT / ML / L-1 / L entry modes, same +5/-1 emulation)
- **Synthetic BO** (SDK v3 exposes no native BO kwargs): entry fill → auto SL; target hit → market-exit
  (cancel SL + MKT sell) **or** trailing-stop ratchet
- Live price ticks + order feed over one `/ws` socket; positions poll; cancel / modify-SL
- Expiry dropdown + strike picker from scrip master (NIFTY step 50, SENSEX step 100 fallback)

> SDK: PyPI **`kotakneoapi` ≥ 3.0.7** (same as SwingTrader) — *not* the old `neo-api-client` 2.x package
> TransTrader uses. `backend/tests/test_sdk_compat.py` pins our calls to the v3 signatures.

## Security (unauthorized-access hardening)
1. **Tailscale TLS (recommended)**: no public 80/443. `tailscale serve` terminates
   TLS with a trusted `*.ts.net` cert and proxies to loopback Caddy (`deploy/tailscale.md`).
   UFW opens only 22 (`deploy/ec2-setup.sh`). A public `sslip.io` hostname remains a fallback.
2. **Caddy** fronts the app; published to `127.0.0.1:80` only under Tailscale.
3. **IP allowlist middleware**: `IP_ALLOWLIST` rejects non-listed IPs on **all** paths
   (`/api` **and** `/ws`) with `403`/WebSocket `1008`. `X-Forwarded-For` is honored only
   when `TRUST_PROXY=true` behind a trusted proxy.
4. **App login**: bcrypt password → short-lived JWT (`APP_USERNAME` / `APP_PASSWORD_BCRYPT`).
5. **Authenticated live feed**: the `/ws` socket requires the JWT as its **first frame**
   and an allowed `Origin`; nothing is streamed before auth (closes `1008`).
6. **No wildcard CORS**: same-origin only; cross-origin requires explicit `ALLOWED_ORIGINS`.
7. **Kotak creds server-side only**: consumer key / UCC / MPIN in EC2 `.env`, never in git or the browser.
   Morning login = app login → click *Kotak TOTP login* → type live TOTP (same as TransTrader OTP step).

## Quickstart (local, uv)
```powershell
Copy-Item .env.example .env   # fill secrets; hash password first
# backend
cd backend; uv sync; uv run uvicorn app.main:app --reload
uv run --group dev pytest tests/ -q   # SDK compat checks
# frontend (new shell)
cd frontend; npm install; npm run dev   # http://localhost:5173
```

## Deploy (fresh EC2, Tailscale TLS — no domain needed)
```bash
# on EC2 (Ubuntu, ap-south-1):
git clone <this-repo> && cd kotak-trader
cp .env.example .env && nano .env   # secrets + IP_ALLOWLIST=100.64.0.0/10 + ALLOWED_ORIGINS=https://<machine>.<tailnet>.ts.net
./deploy/ec2-setup.sh <your-ip>     # installs Docker + Tailscale, opens only SSH
sudo tailscale up                  # join the tailnet (install on your PC too)
docker compose up -d --build
sudo tailscale serve --bg --https=443 http://127.0.0.1:80
# browse https://<machine>.<tailnet>.ts.net (tailnet + app login required)
# fallback without Tailscale: use a public <IP>.sslip.io hostname in the Caddyfile
```

## Repo layout
```
backend/app/{main,config,auth,kotak_client,synthetic_engine,ws_manager}.py
backend/app/routes/{auth,orders,market}.py
frontend/src/{App.jsx,api.js}   # Vite React — ticket, straddle, synthetic legs, positions, live feed
Dockerfile docker-compose.yml Caddyfile deploy/ec2-setup.sh
```

## TODO (from TransTrader)
- [ ] Expiry configurable in UI (currently `EXPIRY` const in `routes/orders.py`)
- [ ] Positions panel with one-click exit
- [ ] L-1/L-2 buttons per CE/PE leg
