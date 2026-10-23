"""Plan 06 WP7: market-data errors are structured, never a flat-looking 200.

A failed positions/quote read must be a 502 (or 504/503 for a worker timeout /
full queue) with sanitized error text. The UI keeps its last good snapshot; the
backend must never return `200 {positions:{}}` for a failed broker call.
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.broker_worker import WorkerQueueFull
from app.kotak_client import BrokerDataError, sanitize_error_text
from app.routes import market as market_routes
from app.schemas import WatchBody


class PositionsBroker:
    def __init__(self, result=None, exc=None):
        self.result = result
        self.exc = exc
        self.calls = 0

    def positions(self, strict=False):
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return self.result or {}


class QuoteBroker:
    def __init__(self, prices=None, exc=None):
        self.prices = prices or {}
        self.exc = exc
        self.calls = 0

    def positions(self, strict=False):
        return {}

    def quote_ltp(self, symbol, exchange, strict=False):
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return self.prices[symbol]

    def token_for(self, symbol, exchange):
        return "tok"


class TimeoutWorker:
    def __init__(self, exc):
        self.exc = exc
        self.timeouts = []

    def submit_wait(self, fn, *args, timeout=None, **kwargs):
        self.timeouts.append(timeout)
        raise self.exc


@pytest.fixture(autouse=True)
def _restore():
    old_kotak = market_routes.kotak
    old_worker = market_routes.read_worker
    yield
    market_routes.kotak = old_kotak
    market_routes.read_worker = old_worker


def _body():
    return WatchBody(legs=[{
        "instrument": "NIFTY", "expiry": "25N18",
        "strike": "25000", "option_type": "CE",
    }])


def _body_json(res):
    return json.loads(res.body)


def test_positions_broker_timeout_is_502_not_200_empty():
    market_routes.read_worker = None
    market_routes.kotak = PositionsBroker(exc=TimeoutError("positions stalled"))

    res = market_routes.positions()
    assert res.status_code == 502
    body = _body_json(res)
    assert body["ok"] is False and body["stale"] is True
    assert body["positions"] == {}
    assert "positions" in body["error"].lower()


def test_positions_broker_data_error_is_502_with_real_text():
    market_routes.read_worker = None
    market_routes.kotak = PositionsBroker(
        exc=BrokerDataError("positions failed: broker said no"))

    res = market_routes.positions()
    assert res.status_code == 502
    assert "broker said no" in _body_json(res)["error"]


def test_positions_recovery_clears_error():
    broker = PositionsBroker(exc=BrokerDataError("temporary"))
    market_routes.read_worker = None
    market_routes.kotak = broker
    assert market_routes.positions().status_code == 502

    broker.exc = None
    broker.result = {"X": {"qty": 20}}
    res = market_routes.positions()
    assert res == {"positions": {"X": {"qty": 20}}}


def test_batch_quote_failure_is_502_with_error_and_no_fake_zero():
    market_routes.read_worker = None
    market_routes.kotak = QuoteBroker(
        exc=BrokerDataError("quote request failed: upstream reset"))

    res = market_routes.batch_quotes(_body())
    assert res.status_code == 502
    body = _body_json(res)
    assert body["ok"] is False and body["stale"] is True
    assert "upstream reset" in body["error"]
    sym = "NIFTY25N1825000CE"
    assert body["ltps"][sym] is None
    assert "upstream reset" in body["errors"][sym]


def test_single_quote_failure_is_502():
    market_routes.kotak = QuoteBroker(exc=BrokerDataError("no instrument token"))

    res = market_routes.quote(instrument="NIFTY", expiry="25N18",
                              strike="25000", option_type="CE")
    assert res.status_code == 502
    assert "no instrument token" in _body_json(res)["error"]


def test_batch_quote_partial_failure_is_502_with_partial_prices():
    class PartialBroker(QuoteBroker):
        def quote_ltp(self, symbol, exchange, strict=False):
            if symbol.endswith("PE"):
                raise BrokerDataError("upstream reset for PE")
            return 123.4

    market_routes.read_worker = None
    market_routes.kotak = PartialBroker()
    body = WatchBody(legs=[
        {"instrument": "NIFTY", "expiry": "25N18",
         "strike": "25000", "option_type": "CE"},
        {"instrument": "NIFTY", "expiry": "25N18",
         "strike": "25000", "option_type": "PE"},
    ])

    res = market_routes.batch_quotes(body)
    assert res.status_code == 502
    data = _body_json(res)
    assert data["ok"] is False and data["stale"] is True
    assert data["ltps"]["NIFTY25N1825000CE"] == 123.4
    assert data["ltps"]["NIFTY25N1825000PE"] is None
    assert "upstream reset" in data["errors"]["NIFTY25N1825000PE"]


def test_read_worker_timeout_maps_to_504_for_positions_quote_batch():
    market_routes.kotak = QuoteBroker()
    market_routes.read_worker = TimeoutWorker(TimeoutError("stalled"))

    for call in (
        market_routes.positions,
        lambda: market_routes.quote(instrument="NIFTY", expiry="25N18",
                                    strike="25000", option_type="CE"),
        lambda: market_routes.batch_quotes(_body()),
    ):
        with pytest.raises(HTTPException) as err:
            call()
        assert err.value.status_code == 504


def test_read_worker_queue_full_maps_to_503():
    market_routes.kotak = PositionsBroker(result={})
    market_routes.read_worker = TimeoutWorker(WorkerQueueFull("full"))

    with pytest.raises(HTTPException) as err:
        market_routes.positions()
    assert err.value.status_code == 503


def test_error_text_is_sanitized():
    text = sanitize_error_text("token=super-secret sid=abc123 connection reset")
    assert "super-secret" not in text
    assert "abc123" not in text
    assert "connection reset" in text


@pytest.mark.parametrize("raw,secrets", [
    ("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.signature",
     ["eyJhbGci", "signature"]),
    ("access_token: 'secret-value'", ["secret-value"]),
    ("{'access_token': 'secret-value'}", ["secret-value"]),
    ('{"access_token":"secret-value"}', ["secret-value"]),
    (("consumer_key='ck_live_123' api_key='aik' password='pw' mpin='1234' "
      "ucc='AB12' mobile='9999999999'"),
     ["ck_live_123", "aik", "pw", "1234", "AB12", "9999999999"]),
    (("sessionToken=st-1 view_token='vt-2' trade_token=tt-3 "
      "api_secret=as-4 consumer_secret='cs-5' totp=123456 "
      "session='raw-session'"),
     ["st-1", "vt-2", "tt-3", "as-4", "cs-5", "123456", "raw-session"]),

])
def test_error_text_redacts_common_credential_shapes(raw, secrets):
    text = sanitize_error_text(raw)
    for secret in secrets:
        assert secret not in text

