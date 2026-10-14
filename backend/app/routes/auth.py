from fastapi import APIRouter, Depends, HTTPException

from ..auth import create_token, require_user, verify_app_password
from ..config import settings
from ..kotak_client import KotakService
from ..schemas import KotakCompleteBody
from ..ws_manager import Hub

router = APIRouter()

# singletons wired in main.py
kotak: KotakService | None = None
hub: Hub | None = None


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
    # (re)subscribe index + order feed on the server, near exchange
    try:
        c = kotak.client
        import asyncio
        loop = asyncio.get_event_loop()
        c.on_message = lambda m: hub.on_ticks(m, loop) if hub else None
        c.on_open = lambda r: None
        c.on_close = lambda r: None
        c.subscribe(instrument_tokens=[
            {"instrument_token": "Nifty 50", "exchange_segment": "nse_cm"},
            {"instrument_token": "SENSEX", "exchange_segment": "bse_cm"}], isIndex=True)
        c.subscribe_to_orderfeed()
    except Exception as e:
        return {"ok": True, "warn": f"logged in, WS subscribe failed: {e}"}
    return {"ok": True}


@router.get("/kotak/status", dependencies=[Depends(require_user)])
def kotak_status():
    assert kotak is not None
    return {"logged_in": kotak.logged_in}
