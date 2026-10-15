"""Same-strike scale-in + quote-cache regressions.
  - Two entries on one strike create two independent trackers and BOTH must
    react to ticks (previously only the first found leg fired target/trail).
  - quote_ltp() must serve repeat polls from cache (Kotak 429s REST bursts).
"""
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.kotak_client import KotakService  # noqa: E402
from app.synthetic_engine import SyntheticEngine  # noqa: E402


def _leg(eid, token="tok9", tgt="120.00"):
    return {"entry_order_id": eid, "sl_order_id": f"SL-{eid}", "trading_symbol": "SAME",
            "status": "ACTIVE", "strategy": "market_exit", "target_price": tgt,
            "stop_loss_price": "90.00", "trail_distance": 5, "high_water_mark": 0.0,
            "low_water_mark": 0.0, "quantity": "20", "exchange_segment": "bse_fo",
            "instrument_token": token, "side": "B", "product": "MIS",
            "target_points": "20", "stop_points": "10"}


def test_both_legs_fire_on_tick():
    broker = MagicMock()
    broker.cancel.return_value = {"stCode": 200}
    eng = SyntheticEngine(broker=broker)
    eng.add(_leg("E1"))
    eng.add(_leg("E2"))
    assert len(eng.by_tokens("tok9")) == 2
    eng.on_tick({"tk": "tok9", "ltp": "130"})
    assert eng.orders["E1"]["status"] == "CLOSING"
    assert eng.orders["E2"]["status"] == "CLOSING"
    assert broker.cancel.call_count == 2


def test_quote_cache_serves_repeats():
    s = KotakService(consumer_key="dummy")
    fake = MagicMock()
    fake.quotes.return_value = {"data": [{"ltp": "42.5"}]}
    s.client = fake
    s.login_date = date.today()
    s.nse_fo_tokens = {"SYM": "111"}
    s.quote_ttl_sec = 60
    assert s.quote_ltp("SYM", "nse_fo") == 42.5
    assert s.quote_ltp("SYM", "nse_fo") == 42.5
    assert fake.quotes.call_count == 1  # second poll served from cache
    s.quote_ttl_sec = 0  # expired -> live call again
    assert s.quote_ltp("SYM", "nse_fo") == 42.5
    assert fake.quotes.call_count == 2
