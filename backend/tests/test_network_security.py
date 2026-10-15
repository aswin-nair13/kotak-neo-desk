"""Network/transport security tests (plan 01 — C1/C3/C4).

Covers: /ws first-frame JWT auth + origin allowlist, the pure-ASGI IP allowlist
(real 403s, WS 1008, XFF honored only under TRUST_PROXY), and no wildcard CORS.
Fully offline — no Kotak SDK calls.
"""
import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import auth, main  # noqa: E402
from app.auth import create_token  # noqa: E402
from app.config import settings  # noqa: E402


@pytest.fixture()
def client():
    main.hub.clients.clear()
    with TestClient(main.app) as c:
        yield c
    main.hub.clients.clear()


def _wait_registered(timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if main.hub.clients:
            return True
        time.sleep(0.01)
    return False


def test_ws_unauthenticated_closes_1008(client, monkeypatch):
    monkeypatch.setattr(auth, "ALLOWLIST", set())
    monkeypatch.setattr(main, "WS_AUTH_TIMEOUT_SEC", 0.05)
    with pytest.raises(WebSocketDisconnect) as ei:
        with client.websocket_connect("/ws") as ws:
            ws.receive_text()
    assert ei.value.code == 1008


def test_ws_invalid_token_closes_1008(client, monkeypatch):
    monkeypatch.setattr(auth, "ALLOWLIST", set())
    with pytest.raises(WebSocketDisconnect) as ei:
        with client.websocket_connect("/ws") as ws:
            ws.send_text(json.dumps({"token": "not-a-jwt"}))
            ws.receive_text()
    assert ei.value.code == 1008


def test_ws_valid_token_receives_broadcast(client, monkeypatch):
    monkeypatch.setattr(auth, "ALLOWLIST", set())
    monkeypatch.setattr(auth, "ALLOWED_ORIGINS", set())
    with client.websocket_connect("/ws") as ws:
        ws.send_text(json.dumps({"token": create_token()}))
        assert _wait_registered(), "socket was not registered after auth"
        fut = asyncio.run_coroutine_threadsafe(
            main.hub.broadcast({"kind": "tick", "tick": {"tk": "Nifty 50", "ltp": "1", "ts": "Nifty 50"}}),
            main.hub.main_loop)
        fut.result(timeout=2)
        assert ws.receive_json()["kind"] == "tick"


def test_ws_bad_origin_closes_1008(client, monkeypatch):
    monkeypatch.setattr(auth, "ALLOWLIST", set())
    monkeypatch.setattr(auth, "ALLOWED_ORIGINS", {"https://good.example"})
    with pytest.raises(WebSocketDisconnect) as ei:
        with client.websocket_connect("/ws", headers={"origin": "https://evil.example"}) as ws:
            ws.receive_text()
    assert ei.value.code == 1008


def test_http_denied_returns_403_not_500(client, monkeypatch):
    monkeypatch.setattr(auth, "ALLOWLIST", {"1.2.3.4"})
    assert client.get("/api/health").status_code == 403


def test_xff_ignored_when_not_trusted(client, monkeypatch):
    monkeypatch.setattr(auth, "ALLOWLIST", {"1.2.3.4"})
    monkeypatch.setattr(settings, "trust_proxy", False)
    r = client.get("/api/health", headers={"X-Forwarded-For": "1.2.3.4"})
    assert r.status_code == 403


def test_xff_honored_when_trusted(client, monkeypatch):
    monkeypatch.setattr(auth, "ALLOWLIST", {"1.2.3.4"})
    monkeypatch.setattr(settings, "trust_proxy", True)
    r = client.get("/api/health", headers={"X-Forwarded-For": "1.2.3.4"})
    assert r.status_code == 200


def test_ws_disallowed_ip_closes_1008(client, monkeypatch):
    monkeypatch.setattr(auth, "ALLOWLIST", {"1.2.3.4"})
    with pytest.raises(WebSocketDisconnect) as ei:
        with client.websocket_connect("/ws") as ws:
            ws.receive_text()
    assert ei.value.code == 1008


def test_no_wildcard_cors(client):
    r = client.get("/api/health", headers={"Origin": "https://evil.example"})
    headers = {k.lower() for k in r.headers}
    assert "access-control-allow-origin" not in headers
