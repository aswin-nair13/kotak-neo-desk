"""Fan-out hub: Kotak WS callbacks -> browser WebSockets + synthetic engine."""

import asyncio
import json
import logging

log = logging.getLogger("ws")


class Hub:
    def __init__(self):
        self.clients: set = set()
        self.engine = None
        self.latest: dict = {}  # token -> last tick

    def set_engine(self, engine):
        self.engine = engine

    async def register(self, ws):
        self.clients.add(ws)

    async def unregister(self, ws):
        self.clients.discard(ws)

    async def broadcast(self, msg: dict):
        dead = []
        text = json.dumps(msg)
        for ws in self.clients:
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)

    # called from Kotak SDK threads -> schedule onto event loop
    def on_ticks(self, message, loop: asyncio.AbstractEventLoop):
        try:
            if not isinstance(message, dict):
                message = json.loads(message)
        except Exception:
            return
        if message.get("type") == "stock_feed":
            for tick in message.get("data", []) or []:
                if tick.get("tk"):
                    self.latest[str(tick["tk"])] = tick
                if self.engine:
                    try:
                        self.engine.on_tick(tick)
                    except Exception as e:
                        log.error("engine tick err: %s", e)
                asyncio.run_coroutine_threadsafe(self.broadcast({"kind": "tick", "tick": tick}), loop)
        elif message.get("type") == "order_feed":
            try:
                payload = json.loads(message["data"])
                if payload.get("type") == "order":
                    od = payload.get("data", {})
                    if self.engine and od.get("nOrdNo"):
                        try:
                            self.engine.on_order_update(od)
                        except Exception as e:
                            log.error("engine order err: %s", e)
                    asyncio.run_coroutine_threadsafe(
                        self.broadcast({"kind": "order", "order": od}), loop)
            except Exception as e:
                log.error("order feed parse err: %s", e)
