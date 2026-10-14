"""Fan-out hub: SFeed bridge -> browser WebSockets.

The SFeedBridge (sfeed_bridge.py) pushes tick/order/position messages here from
its own thread; broadcast() always runs on the FastAPI event loop.
"""

import json


class Hub:
    def __init__(self):
        self.clients: set = set()
        self.engine = None
        self.latest: dict = {}  # token -> last tick
        self.main_loop = None  # set at app startup; bridge pushes onto it

    def set_engine(self, engine):
        self.engine = engine

    async def register(self, ws):
        self.clients.add(ws)

    async def unregister(self, ws):
        self.clients.discard(ws)

    async def broadcast(self, msg: dict):
        dead = []
        text = json.dumps(msg, default=str)
        for ws in self.clients:
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)
