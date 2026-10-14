# KotakAPI Trader

Manual-discretion web trading interface on top of [kotak-neo-python](https://github.com/Kotak-Neo/kotak-neo-python).
Phase 1 (this repo): **no auto-strategy** — you place orders yourself from your PC, execution happens on a
low-latency EC2 box (Mumbai `ap-south-1` recommended) that holds the Kotak session + synthetic-BO engine.
Phase 2 hook: `backend/app/synthetic_engine.py::on_tick` is where algo triggers will plug in.

## Feature parity with TransTrader (PyQt)
- Single CE / PE buy + **Buy-Both straddle** (MKT / ML / L-1 / L entry modes, same +5/-1 emulation)
- **Synthetic BO**: entry fill → auto SL; target hit → market-exit (cancel SL + MKT sell) **or** trailing-stop ratchet
- **Native BO/CO** passthrough (`product=BO` + square_off/stop_loss values) when the exchange accepts it
- Live price ticks + order feed over one `/ws` socket; positions poll; cancel / modify-SL
- Strike picker helper (NIFTY step 50, SENSEX step 100)

## Security (unauthorized-access hardening)
1. **AWS SG + UFW**: only YOUR home IP → 22/80/443 (`deploy/ec2-setup.sh`).
2. **Caddy TLS**: HTTPS; HTTP→HTTPS redirect once a domain is attached.
3. **IP allowlist middleware**: `IP_ALLOWLIST` rejects non-listed IPs at `/api`.
4. **App login**: bcrypt password → short-lived JWT (`APP_USERNAME` / `APP_PASSWORD_BCRYPT`).
5. **Kotak creds server-side only**: consumer key / UCC / MPIN in EC2 `.env`, never in git or the browser.
   Morning login = app login → click *Kotak TOTP login* → type live TOTP (same as TransTrader OTP step).

## Quickstart (local)
```powershell
Copy-Item .env.example .env   # fill secrets; hash password first
# backend
cd backend; python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt; uvicorn app.main:app --reload
# frontend (new shell)
cd frontend; npm install; npm run dev   # http://localhost:5173
```

## Deploy (fresh EC2, no domain yet)
```bash
# on EC2 (Ubuntu, ap-south-1):
git clone <this-repo> && cd kotak-trader
cp .env.example .env && nano .env   # fill all secrets + IP_ALLOWLIST=<your-pc-ip>
./deploy/ec2-setup.sh <your-pc-ip>
docker compose up -d --build
# browse http://<EC2-IP> (allowlisted IP + app login required)
# later: point DNS at Elastic IP, switch Caddyfile to domain block, docker compose up -d
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
