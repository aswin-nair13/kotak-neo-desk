"""JWT session auth + IP allowlist middleware.

Protection layers (see README):
  1. Caddy TLS / Tailscale (encrypted transport) + UFW + AWS security group (deploy/).
  2. This middleware: drops requests from non-allowlisted IPs (if IP_ALLOWLIST set).
  3. JWT login: POST /api/auth/login with APP_USERNAME + password -> bearer token.
  4. Kotak secrets stay server-side; browser only ever sends TOTP digits for the morning login.
"""

from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
import bcrypt

import asyncio
import fnmatch
import ipaddress
import json
import socket
import time

from .config import ALLOWED_ORIGINS, ALLOWLIST, settings

bearer = HTTPBearer(auto_error=False)


def hash_app_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_app_password(plain: str) -> bool:
    if not settings.app_password_bcrypt:
        return False
    try:
        return bcrypt.checkpw(plain.encode(), settings.app_password_bcrypt.strip().encode())
    except (ValueError, AttributeError):
        return False


def create_token() -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=settings.jwt_ttl_min)
    return jwt.encode({"sub": settings.app_username, "exp": exp}, settings.jwt_secret, algorithm="HS256")


def decode_user_token(token: str | None) -> str | None:
    """Validate a bearer/JWT and return the subject, or None if invalid/expired.

    Shared by require_user (HTTP) and the /ws first-frame auth so both paths use
    exactly one token check.
    """
    if not token:
        return None
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
        if payload.get("sub") != settings.app_username:
            return None
        return payload["sub"]
    except (JWTError, ValueError):
        return None


def origin_allowed(origin: str | None) -> bool:
    """True when the Origin is acceptable for the /ws upgrade.

    Empty ALLOWED_ORIGINS = no enforcement (keeps the Vite dev proxy working).
    Configure ALLOWED_ORIGINS (e.g. https://desk.example.ts.net) in production.
    """
    if not ALLOWED_ORIGINS:
        return True
    return origin in ALLOWED_ORIGINS


async def require_user(creds: HTTPAuthorizationCredentials | None = Depends(bearer)) -> str:
    if creds is None or not creds.credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Login required")
    sub = decode_user_token(creds.credentials)
    if sub is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid/expired session")
    return sub


class IPAllowlistMiddleware:
    """Dynamic-IP tolerant allowlist, implemented as a pure ASGI middleware so it
    covers WebSocket upgrades too (BaseHTTPMiddleware passes ws scopes straight
    through) and returns clean 403s instead of 500s.

    Supports single IPs, CIDR ranges, wildcards (49.37.*.*) and DDNS hostnames.
    Hostnames are re-resolved every IP_ALLOWLIST_REFRESH_SEC so a changing home IP
    keeps working as long as your DDNS client updates the name (see .env.example).
    Empty allowlist = allow all (still behind app login, but not recommended).
    X-Forwarded-For is honored only when TRUST_PROXY is on and uvicorn is run with
    --proxy-headers against a trusted proxy; otherwise the peer address is used.
    For a fully static-free setup, prefer Tailscale (no public port at all) — see README.
    """

    def __init__(self, app):
        self.app = app
        self._host_cache: dict[str, tuple[float, set[str]]] = {}

    def _client_ip(self, scope) -> str:
        if settings.trust_proxy:
            for name, value in scope.get("headers", []):
                if name == b"x-forwarded-for":
                    xff = value.decode("latin-1").split(",")[0].strip()
                    if xff:
                        return xff
        client = scope.get("client")
        return client[0] if client else ""

    def _resolve_host(self, host: str) -> set[str]:
        now = time.time()
        cached = self._host_cache.get(host)
        if cached and now - cached[0] < settings.ip_allowlist_refresh_sec:
            return cached[1]
        try:
            _, _, ips = socket.gethostbyname_ex(host)
            self._host_cache[host] = (now, set(ips))
            return set(ips)
        except OSError:
            return cached[1] if cached else set()

    def _allowed(self, client_ip: str) -> bool:
        try:
            ip_obj = ipaddress.ip_address(client_ip)
        except ValueError:
            return False
        for entry in ALLOWLIST:
            if not entry:
                continue
            if "/" in entry:  # CIDR
                try:
                    if ip_obj in ipaddress.ip_network(entry, strict=False):
                        return True
                except ValueError:
                    pass
                continue
            if "*" in entry:  # wildcard
                if fnmatch.fnmatch(client_ip, entry):
                    return True
                continue
            try:  # single IP
                if ip_obj == ipaddress.ip_address(entry):
                    return True
                continue
            except ValueError:
                pass  # not an IP -> treat as hostname (DDNS)
            if client_ip in self._resolve_host(entry):
                return True
        return False

    async def __call__(self, scope, receive, send):
        if not ALLOWLIST or scope.get("type") not in ("http", "websocket"):
            return await self.app(scope, receive, send)
        client_ip = self._client_ip(scope)
        # DNS resolution can block; keep it off the event loop.
        if await asyncio.to_thread(self._allowed, client_ip):
            return await self.app(scope, receive, send)
        if scope["type"] == "http":
            body = json.dumps({"detail": f"IP {client_ip} not allowlisted"}).encode()
            await send({
                "type": "http.response.start",
                "status": 403,
                "headers": [(b"content-type", b"application/json"),
                            (b"content-length", str(len(body)).encode())],
            })
            await send({"type": "http.response.body", "body": body})
        else:
            await send({"type": "websocket.close", "code": 1008})
