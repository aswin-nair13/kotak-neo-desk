from fastapi import APIRouter, Depends, HTTPException

from ..auth import create_token, require_user, verify_app_password
from ..config import settings
from ..kotak_client import KotakService
from ..schemas import KotakCompleteBody

router = APIRouter()

# singletons wired in main.py
kotak: KotakService | None = None
bridge = None


@router.post("/login")
def app_login(body: dict):
    if body.get("username") != settings.app_username or not verify_app_password(body.get("password", "")):
        raise HTTPException(status_code=401, detail="Bad credentials")
    return {"token": create_token(), "user": settings.app_username}


@router.post("/kotak/initiate", dependencies=[Depends(require_user)])
def kotak_initiate():
    assert kotak is not None
    kotak.initiate_login()
    return {"ok": True, "msg": "Enter TOTP from authenticator/MCX app"}


@router.post("/kotak/complete", dependencies=[Depends(require_user)])
def kotak_complete(body: KotakCompleteBody):
    assert kotak is not None
    try:
        kotak.complete_login(settings.kotak_mobile_number, settings.kotak_ucc, body.totp, settings.kotak_mpin)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    # Feeds run on the server via the SFeed bridge (SDK v3 async WS on a
    # background thread). Legacy client.subscribe() was removed in v2.2+.
    try:
        if bridge is not None:
            bridge.start()
    except Exception as e:
        return {"ok": True, "warn": f"logged in, feed bridge failed: {e}"}
    return {"ok": True}


@router.get("/kotak/status", dependencies=[Depends(require_user)])
def kotak_status():
    assert kotak is not None
    out = {"logged_in": kotak.logged_in}
    if kotak.logged_in:
        # Scrip-master health drives trading_enabled (Plan 02 C5): the UI blocks
        # order entry when the symbol->token map failed to load.
        out["scrip_master"] = kotak.master_status()
        out["trading_enabled"] = kotak.master_ok
    return out


@router.get("/kotak/verify", dependencies=[Depends(require_user)])
def kotak_verify():
    """Session-gated probe (margin/limits). ok=true means OMS accepts this session;
    ok=false with 401 detail means the TOTP/MPIN login is dead despite looking live."""
    assert kotak is not None
    return kotak.session_check()


@router.get("/kotak/egress-ip", dependencies=[Depends(require_user)])
def kotak_egress_ip():
    """Public IP Kotak sees. Must be a whitelisted static IPv4 or orders 401 (stCode 100008)."""
    assert kotak is not None
    return kotak.egress_ip()
