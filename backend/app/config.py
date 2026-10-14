from pydantic_settings import BaseSettings


class Settings(BaseSettings):
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
    # Optional: restrict EC2 security-group/UFW updates to a DDNS name (see deploy/).

    kotak_consumer_key: str = ""
    kotak_mobile_number: str = ""
    kotak_ucc: str = ""
    kotak_mpin: str = ""

    default_product: str = "MIS"
    nifty_lot: int = 75
    sensex_lot: int = 20
    positions_poll_sec: int = 30

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

ALLOWLIST = {ip.strip() for ip in settings.ip_allowlist.split(",") if ip.strip()}
