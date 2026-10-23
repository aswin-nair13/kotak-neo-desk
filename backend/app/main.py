"""KotakAPI Trader — FastAPI backend.

Run locally:  uvicorn app.main:app --reload   (from backend/)
Serves: /api/* + /ws live feed + frontend/dist static (prod).
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .auth import IPAllowlistMiddleware, decode_user_token, origin_allowed
from .broker_worker import CommandWorker
from .config import ALLOWED_ORIGINS, settings
from .kotak_client import KotakService
from .routes import auth as auth_routes
from .routes import market as market_routes
from .routes import orders as orders_routes
from .sfeed_bridge import SFeedBridge
from .synthetic_engine import SyntheticEngine
from .ws_manager import Hub

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
# Log timestamps are local wall-clock time of the server process. Docker compose
# pins TZ=Asia/Kolkata so container logs match IST and the engine's time.time()
# epochs; if you run outside Docker set TZ explicitly.
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
    read_worker.start()
    yield
    bridge.stop()
    read_worker.stop()


kotak = KotakService(consumer_key=settings.kotak_consumer_key)
engine = SyntheticEngine(broker=kotak)
worker = CommandWorker(
    "engine-worker", max_queue=settings.broker_worker_queue,
    stall_ms=int(settings.broker_stall_log_sec * 1000),
    stop_timeout=settings.worker_stop_timeout_sec,
    call_timeout=settings.broker_call_timeout_sec)
# Read-only UI calls bypass the mutation worker so a slow book/positions read
# can never delay a queued cancel/exit. The SDK read itself is not cancellable;
# the caller gets a timeout while the read thread finishes independently.
read_worker = CommandWorker(
    "broker-reader", max_queue=32,
    stall_ms=int(settings.broker_stall_log_sec * 1000),
    stop_timeout=settings.worker_stop_timeout_sec,
    call_timeout=settings.orderbook_timeout_sec)
hub = Hub()
hub.set_engine(engine)
bridge = SFeedBridge(hub=hub, engine=engine, kotak=kotak, worker=worker)
# Cancel confirmation (Plan 06 WP4) reads the book through the same worker-safe
# path reconcile uses; the engine never calls the SDK from a tick/feed thread.
engine.book_fetcher = bridge.fetch_book
# Exit state machine follow-up: the engine never sleeps/polls on the worker; it
# asks the bridge to queue one confirm reconcile after a cancel/exit step.
engine.exit_poll_requester = bridge.request_exit_poll

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
orders_routes.worker = worker
orders_routes.read_worker = read_worker
market_routes.kotak = kotak
market_routes.bridge = bridge
market_routes.read_worker = read_worker

app.include_router(auth_routes.router, prefix="/api/auth", tags=["auth"])
app.include_router(orders_routes.router, prefix="/api/orders", tags=["orders"])
app.include_router(market_routes.router, prefix="/api/market", tags=["market"])


@app.get("/api/health")
def health():
    feed = bridge.health()
    w = feed.get("worker", {})
    legs = engine.all()
    open_legs = [o for o in legs if o["status"] != "CLOSED"]
    naked = [o for o in open_legs
             if o.get("naked_position") and not o.get("exit_pending")]
    errored = [o for o in open_legs if o["status"] == "ERRORED"]
    # /api/health is reachable without a user JWT (IP allowlist only), so it
    # must never expose broker order ids. Counts are enough for the UI/banner.
    feed = dict(feed)
    summary = feed.get("last_reconcile")
    if isinstance(summary, dict):
        feed["last_reconcile"] = {
            k: (len(v) if isinstance(v, list) else v)
            for k, v in summary.items()}
    intents = engine.entry_intent_health()
    for rows in intents.values():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, dict):
                row.pop("entry_order_id", None)
                row.pop("superseded_by", None)
    return {"ok": True, "build": BUILD, "kotak_logged_in": kotak.logged_in,
            "synthetic_open": len(open_legs),
            "open_legs": len(open_legs),
            "naked_legs": len(naked),
            "blocked_legs": len([o for o in legs if o.get("blocked_reason")]),
            "errored_legs": len(errored),
            "entry_intents": intents,
            "order_age_ms": feed.get("order_age_ms"),
            "tick_age_ms": feed.get("tick_age_ms"),
            "feed_order_age_ms": feed.get("order_age_ms"),
            "feed_tick_age_ms": feed.get("tick_age_ms"),
            "feed_stale": feed.get("feed_stale"),
            "feed_stale_age_ms": feed.get("feed_stale_age_ms"),
            "last_reconcile_ms": feed.get("last_reconcile_ms"),
            "last_reconcile_ok_ms": feed.get("last_reconcile_ok_ms"),
            "last_reconcile_error": feed.get("last_reconcile_error"),
            "last_reconcile": feed.get("last_reconcile"),
            "last_positions_ms": feed.get("last_positions_ms"),
            "last_positions_ok": feed.get("last_positions_ok"),
            "last_positions_error": feed.get("last_positions_error"),
            "feed": feed,
            "worker": w,
            "worker_queue_depth": w.get("queue_depth"),
            "worker_last_error": w.get("last_error")}


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


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    """Browsers request /favicon.ico by default; answer instead of a 404.

    The SPA also links an inline SVG favicon (frontend/index.html), so this is
    only a quiet fallback for cached/legacy requests.
    """
    return Response(status_code=204)


# prod: serve React build
DIST = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"
if DIST.exists():
    app.mount("/", StaticFiles(directory=str(DIST), html=True), name="static")
