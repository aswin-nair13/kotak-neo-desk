"""Plan 02 — broker scrip-master TLS verification (C5).

Covers: TLS always verified (verify=True) with an optional CA-bundle override,
a failed download raises instead of silently returning an empty map, and such a
failure surfaces as a trading-disabled state that blocks order entry.
Fully offline — the HTTP layer and SDK client are stubbed.
"""
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import kotak_client  # noqa: E402
from app.config import settings  # noqa: E402
from app.kotak_client import KotakService, ScripMasterError  # noqa: E402


class _Resp:
    text = "pTrdSymbol,pSymbol\nNIFTY25N1825450CE,12345\nSENSEX25N1875000PE,67890\n"

    def raise_for_status(self):
        return None


class _MonthlyResp:
    # Includes weekly (25N18, 25D18) and monthly (25NOV) codes plus the
    # pExpiryDate column the real master's CSV carries. Epochs are ordered
    # 25N18 < 25NOV < 25D18, so a lexical sort (D < N) would be wrong.
    text = (
        "pTrdSymbol,pSymbol,pExpiryDate\n"
        "NIFTY25N1825450CE,1,1763500000\n"
        "NIFTY25NOV25000CE,2,1764000000\n"
        "NIFTY25D1825000CE,3,1766000000\n"
    )

    def raise_for_status(self):
        return None


@pytest.fixture()
def svc():
    s = KotakService(consumer_key="dummy")
    fake = MagicMock()
    fake.scrip_master.return_value = "https://broker.example/scrip.csv"
    s.client = fake
    s.login_date = date.today()
    return s, fake


def test_download_verifies_tls_by_default(svc, monkeypatch):
    s, _ = svc
    monkeypatch.setattr(settings, "scrip_master_ca_bundle", "")
    seen = {}

    def fake_get(url, timeout=None, verify=None, **kw):
        seen["verify"] = verify
        return _Resp()

    monkeypatch.setattr(kotak_client.requests, "get", fake_get)
    tokens = s._scrip_master("nse_fo")
    assert seen["verify"] is True
    assert tokens == {"NIFTY25N1825450CE": "12345", "SENSEX25N1875000PE": "67890"}


def test_ca_bundle_override_is_used(svc, monkeypatch):
    s, _ = svc
    monkeypatch.setattr(settings, "scrip_master_ca_bundle", r"C:\certs\broker.pem")
    seen = {}
    monkeypatch.setattr(
        kotak_client.requests, "get",
        lambda url, timeout=None, verify=None, **kw: (seen.update(verify=verify), _Resp())[1])
    s._scrip_master("nse_fo")
    assert seen["verify"] == r"C:\certs\broker.pem"


def test_tls_failure_raises_not_silent_empty(svc, monkeypatch):
    s, _ = svc

    def boom(*a, **kw):
        raise requests.exceptions.SSLCertVerificationError(
            "unable to get local issuer certificate")

    monkeypatch.setattr(kotak_client.requests, "get", boom)
    with pytest.raises(ScripMasterError):
        s._scrip_master("nse_fo")


def test_load_master_records_error_and_disables_trading(svc, monkeypatch):
    s, _ = svc

    def boom(*a, **kw):
        raise requests.exceptions.ConnectionError("host unreachable")

    monkeypatch.setattr(kotak_client.requests, "get", boom)
    s.nse_fo_tokens = s._load_master("nse_fo")
    s.bse_fo_tokens = s._load_master("bse_fo")
    assert s.nse_fo_tokens == {}
    assert not s.master_ok
    assert "nse_fo" in s.master_errors
    assert s.master_status()["ok"] is False


def test_master_ok_when_both_load(svc, monkeypatch):
    s, _ = svc
    monkeypatch.setattr(kotak_client.requests, "get", lambda *a, **kw: _Resp())
    s.nse_fo_tokens = s._load_master("nse_fo")
    s.bse_fo_tokens = s._load_master("bse_fo")
    assert s.master_ok
    assert s.master_status()["ok"] is True


def test_monthly_expiries_and_strikes_are_visible(svc, monkeypatch):
    s, _ = svc
    monkeypatch.setattr(kotak_client.requests, "get", lambda *a, **kw: _MonthlyResp())
    s.nse_fo_tokens = s._load_master("nse_fo")
    s.bse_fo_tokens = s._load_master("bse_fo")
    assert s.expiries("NIFTY") == ["25N18", "25NOV", "25D18"]
    assert s.master_strikes("NIFTY", "25NOV", "CE") == ["25000"]


def test_expiries_fall_back_to_code_order_without_master_df():
    s = KotakService(consumer_key="dummy")
    s.nse_fo_tokens = {
        "NIFTY25D1825000CE": "1",
        "NIFTY25NOV25000CE": "2",
        "NIFTY25N1825450CE": "3",
    }
    assert s.expiries("NIFTY") == ["25N18", "25NOV", "25D18"]


def test_refresh_masters_clears_prior_error(svc, monkeypatch):
    s, _ = svc

    def boom(*a, **kw):
        raise requests.exceptions.ConnectionError("temporary")

    monkeypatch.setattr(kotak_client.requests, "get", boom)
    s.nse_fo_tokens = s._load_master("nse_fo")
    assert not s.master_ok
    monkeypatch.setattr(kotak_client.requests, "get", lambda *a, **kw: _Resp())
    status = s.refresh_masters()
    assert status["ok"] is True
    assert s.master_errors == {}


def test_order_entry_blocked_when_master_unavailable(svc, monkeypatch):
    s, _ = svc
    from app.routes import orders as orders_routes

    monkeypatch.setattr(orders_routes, "kotak", s)
    s.master_errors = {"nse_fo": "tls"}
    with pytest.raises(HTTPException) as ei:
        orders_routes._require_master()
    assert ei.value.status_code == 503


def test_order_entry_allowed_when_master_ok(svc, monkeypatch):
    s, _ = svc
    from app.routes import orders as orders_routes

    monkeypatch.setattr(orders_routes, "kotak", s)
    s.nse_fo_tokens = {"A": "1"}
    s.bse_fo_tokens = {"B": "2"}
    orders_routes._require_master()  # must not raise
