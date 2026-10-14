"""SDK compat tests: KotakService must only use kwargs from kotakneoapi v3 signatures.

Runs offline — SDK client is stubbed, calls are validated with inspect against the
real installed NeoAPI. Catches the 2.x -> 3.x breakages (place_order BO kwargs,
modify_order quantity, order_history order_id).
"""
import inspect
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from neo_api_client import NeoAPI  # noqa: E402

from app.kotak_client import KotakService  # noqa: E402
from app import schemas  # noqa: E402


def bind(fn, kwargs):
    inspect.signature(fn).bind(object(), **kwargs)


@pytest.fixture()
def svc():
    s = KotakService(consumer_key="dummy")
    fake = MagicMock()
    fake.place_order.return_value = {"stCode": 200, "nOrdNo": "1"}
    fake.modify_order.return_value = {"stCode": 200}
    fake.cancel_order.return_value = {"stCode": 200}
    fake.order_report.return_value = {"stCode": 200, "data": []}
    s.client = fake
    s.login_date = date.today()
    return s, fake


def test_place_matches_v3(svc):
    s, fake = svc
    s.place_order(exchange_segment="nse_fo", product="MIS", order_type="L",
                  quantity="75", trading_symbol="NIFTY25N1825450CE",
                  transaction_type="B", price="100")
    bind(NeoAPI.place_order, fake.place_order.call_args.kwargs)


def test_stop_and_exit_match_v3(svc):
    s, fake = svc
    s.place_stop_loss("nse_fo", "75", "NIFTY25N1825450CE", "90")
    bind(NeoAPI.place_order, fake.place_order.call_args.kwargs)
    s.place_market_exit("nse_fo", "75", "NIFTY25N1825450CE")
    bind(NeoAPI.place_order, fake.place_order.call_args.kwargs)


def test_modify_passes_quantity(svc):
    s, fake = svc
    s.modify_sl("123", "75", "95.5")
    bind(NeoAPI.modify_order, fake.modify_order.call_args.kwargs)


def test_cancel_and_book(svc):
    s, fake = svc
    s.cancel("123")
    bind(NeoAPI.cancel_order, fake.cancel_order.call_args.kwargs)
    s.order_book()
    bind(NeoAPI.order_report, fake.order_report.call_args.kwargs)


def test_login_signatures():
    inspect.signature(NeoAPI.__init__).bind(object(), consumer_key="x", environment="prod")
    inspect.signature(NeoAPI.totp_login).bind(object(), mobile_number="m", ucc="u", totp="t")
    inspect.signature(NeoAPI.totp_validate).bind(object(), mpin="m")


def test_bo_rejected_synthetic_only():
    schemas.SingleOrderBody(instrument="NIFTY", expiry="25N18", option_type="CE",
                            strike="25450", product="MIS")
    with pytest.raises(Exception):
        schemas.SingleOrderBody(instrument="NIFTY", expiry="25N18", option_type="CE",
                                strike="25450", product="BO")


def test_app_imports():
    import app.main  # noqa: F401
    import app.routes.orders, app.routes.market, app.routes.auth  # noqa: F401
    import app.synthetic_engine, app.ws_manager  # noqa: F401
