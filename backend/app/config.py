from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Single .env at the repo root (same file docker compose uses), resolved by
# absolute path so the server finds it whether you launch from backend/ or root.
# Real env vars still take precedence over the file.
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(REPO_ROOT / ".env"), extra="ignore")
    app_username: str = "trader"
    app_password_bcrypt: str = ""
    jwt_secret: str = "change-me"
    jwt_ttl_min: int = 480
    # Comma-separated. Each entry may be:
    #   single IP      49.37.123.45
    #   CIDR           49.37.0.0/16  (covers a dynamic ISP pool)
    #   wildcard       49.37.*.*     (same as 49.37.0.0/16)
    #   hostname       myhome.ddns.net (DDNS — re-resolved every N sec)
    ip_allowlist: str = ""
    ip_allowlist_refresh_sec: int = 300
    # Only honor X-Forwarded-For for the client IP when the app sits behind a
    # trusted proxy (Caddy). Keep false unless uvicorn is run with
    # --proxy-headers --forwarded-allow-ips=<proxy> (see Dockerfile/compose).
    trust_proxy: bool = False
    # Comma-separated allowed browser Origins for the /ws upgrade and CORS.
    # Empty = no Origin enforcement (dev Vite proxy) and no CORS middleware.
    allowed_origins: str = ""
    # Optional: restrict EC2 security-group/UFW updates to a DDNS name (see deploy/).

    kotak_consumer_key: str = ""
    kotak_mobile_number: str = ""
    kotak_ucc: str = ""
    kotak_mpin: str = ""

    default_product: str = "MIS"
    nifty_lot: int = 75
    sensex_lot: int = 20
    positions_poll_sec: int = 30


settings = Settings()

ALLOWLIST = {ip.strip() for ip in settings.ip_allowlist.split(",") if ip.strip()}

ALLOWED_ORIGINS = {o.strip() for o in settings.allowed_origins.split(",") if o.strip()}
