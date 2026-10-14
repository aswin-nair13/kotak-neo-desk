"""JWT session auth + IP allowlist middleware.

Protection layers (see README):
  1. Caddy TLS (HTTPS) + UFW + AWS security group (deploy/).
  2. This middleware: drops requests from non-allowlisted IPs (if IP_ALLOWLIST set).
  3. JWT login: POST /api/auth/login with APP_USERNAME + password -> bearer token.
  4. Kotak secrets stay server-side; browser only ever sends TOTP digits for the morning login.
"""

from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from starlette.middleware.base import BaseHTTPMiddleware

import fnmatch
import ipaddress
import socket
import time

from .config import ALLOWLIST, settings

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer = HTTPBearer(auto_error=False)


def verify_app_password(plain: str) -> bool:
    if not settings.app_password_bcrypt:
        return False
    return pwd_ctx.verify(plain, settings.app_password_bcrypt)


def create_token() -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=settings.jwt_ttl_min)
    return jwt.encode({"sub": settings.app_username, "exp": exp}, settings.jwt_secret, algorithm="HS256")


async def require_user(creds: HTTPAuthorizationCredentials | None = Depends(bearer)) -> str:
    if creds is None or not creds.credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Login required")
    try:
        payload = jwt.decode(creds.credentials, settings.jwt_secret, algorithms=["HS256"])
        if payload.get("sub") != settings.app_username:
            raise ValueError("bad subject")
        return payload["sub"]
    except (JWTError, ValueError):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid/expired session")


class IPAllowlistMiddleware(BaseHTTPMiddleware):
    """Dynamic-IP tolerant allowlist.

    Supports single IPs, CIDR ranges, wildcards (49.37.*.*) and DDNS hostnames.
    Hostnames are re-resolved every IP_ALLOWLIST_REFRESH_SEC so a changing home IP
    keeps working as long as your DDNS client updates the name (see .env.example).
    Empty allowlist = allow all (still behind app login, but not recommended).
    For a fully static-free setup, prefer Tailscale (no public port at all) — see README.
    """

    def __init__(self, app):
        super().__init__(app)
        self._host_cache: dict[str, tuple[float, set[str]]] = {}

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

    async def dispatch(self, request: Request, call_next):
        if ALLOWLIST and request.url.path.startswith("/api"):
            client_ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
                         or (request.client.host if request.client else ""))
            if not self._allowed(client_ip):
                raise HTTPException(status_code=403, detail=f"IP {client_ip} not allowlisted")
        return await call_next(request)
