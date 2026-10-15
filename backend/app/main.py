"""KotakAPI Trader — FastAPI backend.

Run locally:  uvicorn app.main:app --reload   (from backend/)
Serves: /api/* + /ws live feed + frontend/dist static (prod).
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .auth import IPAllowlistMiddleware, decode_user_token, origin_allowed
from .config import ALLOWED_ORIGINS, settings
from .kotak_client import KotakService
from .routes import auth as auth_routes
from .routes import market as market_routes
from .routes import orders as orders_routes
from .sfeed_bridge import SFeedBridge
from .synthetic_engine import SyntheticEngine
from .ws_manager import Hub

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("startup")

if not settings.app_password_bcrypt:
    log.warning("APP_PASSWORD_BCRYPT is empty — /api/auth/login will 401. "
                "Set it in the repo-root .env (see .env.example).")
if settings.jwt_secret == "change-me":
    log.warning("JWT_SECRET is still the default — set a random 32+ char value in .env.")

BUILD = "sfeed-bridge"


@asynccontextmanager
async def lifespan(app: FastAPI):
    hub.main_loop = asyncio.get_running_loop()
    yield
    bridge.stop()


kotak = KotakService(consumer_key=settings.kotak_consumer_key)
engine = SyntheticEngine(broker=kotak)
hub = Hub()
hub.set_engine(engine)
bridge = SFeedBridge(hub=hub, engine=engine, kotak=kotak)

app = FastAPI(title="KotakAPI Trader", lifespan=lifespan)
app.add_middleware(IPAllowlistMiddleware)
# The SPA is served same-origin (prod) and the Vite dev server proxies /api and
# /ws, so no CORS is needed by default. Only enable an explicit, credentialed-off
# policy when ALLOWED_ORIGINS is configured for a cross-origin client.
if ALLOWED_ORIGINS:
    app.add_middleware(CORSMiddleware, allow_origins=sorted(ALLOWED_ORIGINS),
                       allow_credentials=False, allow_methods=["GET", "POST"],
                       allow_headers=["Authorization", "Content-Type"])

# wire singletons into route modules
auth_routes.kotak = kotak
auth_routes.bridge = bridge
orders_routes.kotak = kotak
orders_routes.engine = engine
orders_routes.hub = hub
orders_routes.bridge = bridge
market_routes.kotak = kotak
market_routes.bridge = bridge

app.include_router(auth_routes.router, prefix="/api/auth", tags=["auth"])
app.include_router(orders_routes.router, prefix="/api/orders", tags=["orders"])
app.include_router(market_routes.router, prefix="/api/market", tags=["market"])


@app.get("/api/health")
def health():
    return {"ok": True, "build": BUILD, "kotak_logged_in": kotak.logged_in,
            "synthetic_open": len([o for o in engine.all() if o["status"] not in ("CLOSED",)])}


WS_AUTH_TIMEOUT_SEC = 3.0


def _ws_token(frame: str) -> str | None:
    try:
        msg = json.loads(frame)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(msg, dict):
        return None
    token = msg.get("token")
    return token if isinstance(token, str) and token else None


@app.websocket("/ws")
async def ws_feed(ws: WebSocket):
    # Origin allowlist is checked before accept; mismatches get a policy close.
    if not origin_allowed(ws.headers.get("origin")):
        await ws.close(code=1008)
        return

    await ws.accept()

    # First-frame auth: nothing is sent or registered until a valid JWT arrives.
    # Keep reading (tolerating stray/keepalive frames) until the short deadline.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WS_AUTH_TIMEOUT_SEC
    authed = False
    while not authed:
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            frame = await asyncio.wait_for(ws.receive_text(), timeout=remaining)
        except (asyncio.TimeoutError, WebSocketDisconnect):
            break
        if decode_user_token(_ws_token(frame)) is not None:
            authed = True

    if not authed:
        await ws.close(code=1008)
        return

    await hub.register(ws)
    try:
        while True:
            await ws.receive_text()  # keepalive; server pushes ticks/orders
    except WebSocketDisconnect:
        pass
    finally:
        await hub.unregister(ws)


# prod: serve React build
DIST = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"
if DIST.exists():
    app.mount("/", StaticFiles(directory=str(DIST), html=True), name="static")
