import re
from pathlib import Path

from pydantic import field_validator, model_validator
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
    # Optional CA bundle (PEM path) for the scrip-master CSV download. Empty =
    # the default certifi/OS trust store (TLS always verified — never disabled).
    scrip_master_ca_bundle: str = ""

    default_product: str = "MIS"
    nifty_lot: int = 75
    sensex_lot: int = 20
    positions_poll_sec: int = 30

    # Order-feed liveness + periodic reconcile (Plan 06 WP2).
    feed_stale_sec: float = 30.0       # message-silence WARNING threshold
    feed_watchdog_sec: float = 5.0     # how often the watchdog checks liveness
    # Conservative idle fallback when the SDK exposes no connection signal.
    feed_idle_reconnect_sec: float = 300.0
    reconcile_sec: float = 30.0        # periodic broker reconcile interval
    # Watchlist subscription coalescing (Plan 06 WP9). Identical per-session
    # watch requests are dropped; rapid changes are merged and applied once.
    watch_min_interval_ms: int = 500
    # Browser tabs that disappear without unwatch (reload/crash/network loss)
    # are pruned after this long without a watch/heartbeat call.
    watch_session_ttl_sec: float = 900.0
    # Reconcile safety: never treat one missing SL as proof it is gone.
    sl_visibility_grace_sec: float = 10.0
    sl_missing_confirmations: int = 2
    # A single flat snapshot may be a lagging position row; require this many.
    flat_position_confirmations: int = 2
    # SL guarantee / naked-position guard (Plan 06 WP3).
    # An accepted entry must have a live protective SL within this window.
    sl_guarantee_sec: float = 12.0
    # alert = log/raise the alert only; auto_cancel also cancels an entry that
    # is still unfilled after the guarantee window.
    sl_guarantee_behavior: str = "alert"
    # How many times reconcile may re-place a missing/rejected/cancelled SL
    # before marking the leg ERRORED + naked_position. Never duplicate a live SL.
    sl_replace_max_attempts: int = 1
    # Manual SL modify window (Plan 06 WP5): while a modify is in flight, never
    # auto-re-place the old stop. Book/feed state wins once it settles/expires.
    sl_modify_timeout_sec: float = 15.0
    # Manual SL cancel intent is honoured even if the broker call errors/times
    # out; reconcile treats a missing SL as intentionally cancelled.
    sl_cancel_timeout_sec: float = 8.0
    # A CLOSED leg that still sees a working SL retries the cancel this many
    # times, then raises a bounded CRITICAL alert (no infinite cancel loop).
    closed_sl_cancel_max_attempts: int = 3
    # Caller timeout for UI reads served by the dedicated read worker; a wedged
    # read never occupies the single-writer mutation worker.
    orderbook_timeout_sec: float = 8.0
    # Cancel/exit integrity (Plan 06 WP4): confirm the SL is terminal in the
    # order book before firing the market exit. All values are wall-clock.
    exit_confirm_timeout_sec: float = 8.0
    exit_confirm_poll_sec: float = 0.5
    exit_confirm_retries: int = 2
    # Entry auto-cancel retry budget starts at the first attempt, not at the
    # (already-expired) SL guarantee deadline.
    entry_cancel_retry_sec: float = 60.0
    entry_cancel_max_attempts: int = 3
    # Single-writer worker (Plan 06 WP1).
    broker_worker_queue: int = 1000
    broker_call_timeout_sec: float = 10.0
    broker_stall_log_sec: float = 5.0  # log an ERROR when a command exceeds this
    worker_stop_timeout_sec: float = 5.0
    # Duplicate-order protection (Plan 06 WP8): a timed-out/uncertain place is
    # checked against the broker book by its deterministic client tag before a
    # retry. This is the TOTAL attempts per placement (1 = never retry).
    order_place_max_attempts: int = 2
    # Bounded window for re-reading the order report after an ambiguous place.
    # A single immediate read can miss a just-accepted order (book lag), which
    # would otherwise allow a duplicate retry.
    order_place_confirm_sec: float = 1.5
    # Superseded/abandoned entry tags are retained so a late first-attempt order
    # is adopted/alerted, never ignored. Bounded by age/count.
    order_tag_ttl_sec: float = 21600.0
    order_tag_registry_max: int = 1000
    # Optional prefix for every generated client tag (entry/SL/exit). Empty keeps
    # the legacy short tags (e.g. SE1-1) so existing adoption data still matches.
    # Max 6 chars: action + unique ref tail + attempt must still fit Kotak's 20.
    order_tag_prefix: str = ""

    # Entry/placement hygiene (Plan 06 WP11).
    # Block a second working bracket on the same symbol by default; an operator
    # may explicitly opt in with ALLOW_STACKED_ENTRIES=true.
    allow_stacked_entries: bool = False
    # Warn (never reject) when a passive entry limit sits this far from LTP.
    max_passive_offset: float = 5.0
    # Bound CLOSED trackers so long-running sessions do not grow without limit.
    # Open/ERRORED legs and CLOSED legs with live/cancel-pending orders are never
    # pruned. CLOSED_TRACKERS_MAX=0 disables only the count cap (age still applies).
    closed_retention_sec: float = 86400.0
    closed_trackers_max: int = 500

    @field_validator("sl_missing_confirmations", "flat_position_confirmations",
                     "entry_cancel_max_attempts",
                     "closed_sl_cancel_max_attempts",
                     "order_place_max_attempts")
    @classmethod
    def _at_least_one_confirmation(cls, v: int) -> int:
        # A single snapshot/attempt is never enough for these safety gates.
        if int(v) < 1:
            raise ValueError("confirmation/attempt counts must be >= 1")
        return int(v)

    @field_validator("sl_guarantee_behavior")
    @classmethod
    def _known_behavior(cls, v: str) -> str:
        value = str(v or "").strip().lower()
        if value not in ("alert", "auto_cancel"):
            raise ValueError("SL_GUARANTEE_BEHAVIOR must be alert|auto_cancel")
        return value

    @field_validator("sl_replace_max_attempts", "exit_confirm_retries")
    @classmethod
    def _non_negative_attempts(cls, v: int) -> int:
        if int(v) < 0:
            raise ValueError("attempt/retry counts must be >= 0")
        return int(v)

    @field_validator("order_place_confirm_sec")
    @classmethod
    def _non_negative_confirm_window(cls, v: float) -> float:
        if float(v) < 0:
            raise ValueError("ORDER_PLACE_CONFIRM_SEC must be >= 0")
        return float(v)

    @field_validator("order_tag_ttl_sec")
    @classmethod
    def _positive_tag_ttl(cls, v: float) -> float:
        if float(v) <= 0:
            raise ValueError("ORDER_TAG_TTL_SEC must be > 0")
        return float(v)

    @field_validator("order_tag_registry_max")
    @classmethod
    def _positive_tag_registry(cls, v: int) -> int:
        if int(v) < 1:
            raise ValueError("ORDER_TAG_REGISTRY_MAX must be >= 1")
        return int(v)

    @field_validator("watch_min_interval_ms", "closed_trackers_max")
    @classmethod
    def _non_negative_counts(cls, v: int) -> int:
        if int(v) < 0:
            raise ValueError("count/interval values must be >= 0")
        return int(v)

    @field_validator("max_passive_offset")
    @classmethod
    def _non_negative_offset(cls, v: float) -> float:
        if float(v) < 0:
            raise ValueError("MAX_PASSIVE_OFFSET must be >= 0")
        return float(v)

    @field_validator("closed_retention_sec")
    @classmethod
    def _positive_closed_retention(cls, v: float) -> float:
        if float(v) <= 0:
            raise ValueError("CLOSED_RETENTION_SEC must be > 0")
        return float(v)

    @field_validator("order_tag_prefix")
    @classmethod
    def _safe_tag_prefix(cls, v: str) -> str:
        value = str(v or "")
        if value and not re.fullmatch(r"[A-Za-z0-9_-]{1,6}", value):
            raise ValueError(
                "ORDER_TAG_PREFIX must be 0-6 chars: A-Z, a-z, 0-9, _ or -")
        return value

    @field_validator("sl_guarantee_sec", "exit_confirm_timeout_sec",
                     "exit_confirm_poll_sec", "sl_visibility_grace_sec",
                     "reconcile_sec", "entry_cancel_retry_sec",
                     "sl_modify_timeout_sec", "sl_cancel_timeout_sec",
                     "orderbook_timeout_sec", "watch_session_ttl_sec")
    @classmethod
    def _positive_timing(cls, v: float) -> float:
        # A zero/negative timer would make the guarantees fire instantly or
        # never; reject it at startup rather than wedge live exit handling.
        if float(v) <= 0:
            raise ValueError("timing values must be > 0")
        return float(v)

    @model_validator(mode="after")
    def _modify_window_covers_broker_call(self):
        # A manual modify must still be pending when the slow broker call
        # settles, otherwise reconcile re-places the old SL first.
        margin = 1.0
        if float(self.sl_modify_timeout_sec) <= (
                float(self.broker_call_timeout_sec) + margin):
            raise ValueError(
                "SL_MODIFY_TIMEOUT_SEC must exceed BROKER_CALL_TIMEOUT_SEC "
                f"by more than {margin:g}s")
        return self


settings = Settings()

ALLOWLIST = {ip.strip() for ip in settings.ip_allowlist.split(",") if ip.strip()}

ALLOWED_ORIGINS = {o.strip() for o in settings.allowed_origins.split(",") if o.strip()}
