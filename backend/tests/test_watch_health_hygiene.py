"""Plan 06 WP9-WP11: watch coalescing, observability/alerts, hygiene.

Covers:
  - watch requests dedupe legs and ref-count subscriptions per session;
  - /api/health exposes feed-age/open/naked/errored/reconcile aliases;
  - a hung order feed logs one FEED STALE alert and clears on recovery;
  - stacked entries are blocked by default (opt-in config);
  - passive limits far from LTP return a warning but still place;
  - CLOSED trackers are pruned by age/cap, open legs are never pruned;
  - /favicon.ico does not 404.
"""
import asyncio
import logging
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings
from app.routes import market as market_routes
from app.routes import orders as orders_routes
from app.schemas import SingleOrderBody, UnwatchBody, WatchBody, WatchLeg
from app.sfeed_bridge import SFeedBridge
from app.synthetic_engine import SyntheticEngine


def _now_ms():
    return int(time.time() * 1000)


def _bridge():
    hub = MagicMock()
    hub.main_loop = None
    engine = MagicMock()
    engine.all.return_value = []
    kotak = MagicMock()
    kotak.client = MagicMock()
    return SFeedBridge(hub=hub, engine=engine, kotak=kotak, retry_sec=0.01)


# ---------------- WP9: watch endpoint + ref counting ----------------
class WatchBroker:
    def __init__(self):
        self.tokens = {}

    def token_for(self, symbol, exchange):
        return self.tokens.get(symbol)


class WatchBridge:
    def __init__(self):
        self.calls = []
        self.unwatched = []

    def watch_options(self, keys, session_id=None):
        self.calls.append((set(keys), session_id))
        return {"deduped": False, "throttled": False,
                "session_id": session_id or "default"}

    def unwatch(self, session_id=None):
        self.unwatched.append(session_id)
        return {"removed": True, "released_options": 2,
                "watch_sessions": 0}


def test_watch_dedupes_legs_and_coalesces_one_bridge_call(monkeypatch):
    broker = WatchBroker()
    broker.tokens = {"NIFTY25N1825000CE": "1", "SENSEX25N1874200PE": "2"}
    bridge = WatchBridge()
    monkeypatch.setattr(market_routes, "kotak", broker)
    monkeypatch.setattr(market_routes, "bridge", bridge)

    body = WatchBody(session_id="s1", legs=[
        WatchLeg(instrument="NIFTY", expiry="25N18", strike="25000",
                 option_type="CE"),
        WatchLeg(instrument="NIFTY", expiry="25N18", strike="25000",
                 option_type="CE"),
        WatchLeg(instrument="SENSEX", expiry="25N18", strike="74200",
                 option_type="PE"),
    ])
    out = market_routes.watch(body)
    assert out["legs"] == 2 and out["subscribed"] == 2
    assert out["tokens"]["NIFTY25N1825000CE"] == "1"
    assert len(bridge.calls) == 1
    keys, session = bridge.calls[0]
    assert session == "s1"
    assert keys == {("nse_fo", "1"), ("bse_fo", "2")}


def test_unwatch_releases_the_session(monkeypatch):
    bridge = WatchBridge()
    monkeypatch.setattr(market_routes, "bridge", bridge)
    out = market_routes.unwatch(UnwatchBody(session_id="s1"))
    assert out["ok"] is True and out["removed"] is True
    assert bridge.unwatched == ["s1"]


def test_bridge_watch_is_ref_counted_and_deduped():
    b = _bridge()
    k1, k2 = ("nse_fo", "1"), ("nse_fo", "2")
    first = b.watch_options({k1}, "a", min_interval_ms=0)
    assert first["deduped"] is False and k1 in b._wanted
    assert b.watch_options({k1}, "a", min_interval_ms=0)["deduped"] is True

    b.watch_options({k1, k2}, "b", min_interval_ms=0)
    b.unwatch("a")
    assert k1 in b._wanted, "token still needed by session b"
    b.unwatch("b")
    assert k1 not in b._wanted and k2 not in b._wanted
    b.stop()


def test_bridge_rate_limits_rapid_changes_without_dropping_them():
    b = _bridge()
    k1, k2 = ("nse_fo", "1"), ("nse_fo", "2")
    b.watch_options({k1}, "a", min_interval_ms=5000)
    out = b.watch_options({k2}, "a", min_interval_ms=5000)
    assert out["throttled"] is True
    assert out["subscribe_in_ms"] > 0
    # The new set is already recorded; the timer applies it after the cooldown.
    assert k2 in b._wanted
    b.stop()


def test_prune_wanted_keeps_live_legs_and_watch_sessions():
    b = _bridge()
    b.ensure_options({("nse_fo", "1"), ("nse_fo", "2")})
    b.watch_options({("nse_fo", "3")}, "s", min_interval_ms=0)
    b.engine.all.return_value = [
        {"instrument_token": "1", "exchange_segment": "nse_fo",
         "status": "ACTIVE"},
    ]
    assert b.prune_wanted() == 1
    assert b._wanted == {("nse_fo", "1"), ("nse_fo", "3")}
    b.stop()


def test_watch_session_ttl_expires_without_heartbeat(monkeypatch):
    monkeypatch.setattr(settings, "watch_session_ttl_sec", 1.0)
    b = _bridge()
    k1 = ("nse_fo", "1")
    b.watch_options({k1}, "tab", min_interval_ms=0)
    b._watch_last_ms["tab"] = _now_ms() - 2000

    assert b.prune_wanted() == 1
    assert "tab" not in b._watch_sessions
    assert k1 not in b._wanted
    b.stop()


def test_identical_watch_request_refreshes_ttl_without_resubscribe(monkeypatch):
    monkeypatch.setattr(settings, "watch_session_ttl_sec", 1.0)
    b = _bridge()
    k1 = ("nse_fo", "1")
    b.watch_options({k1}, "tab", min_interval_ms=0)
    b._watch_last_ms["tab"] = _now_ms() - 2000

    out = b.watch_options({k1}, "tab", min_interval_ms=0)
    assert out["deduped"] is True
    assert b.prune_wanted() == 0
    assert k1 in b._wanted
    b.stop()


def test_prune_wanted_keeps_tokens_when_engine_read_fails():
    b = _bridge()
    k1 = ("nse_fo", "1")
    b.ensure_options({k1})
    b.engine.all.side_effect = RuntimeError("engine busy")

    assert b.prune_wanted() == 0
    assert k1 in b._wanted
    b.stop()


class _SyncWs:
    def __init__(self):
        self.subs = []
        self.unsubs = []
        self.attempts = 0
        self.fail_first = False
        self.on_sub = None

    async def subscribe_scrips(self, toks):
        self.attempts += 1
        if self.fail_first and self.attempts == 1:
            raise RuntimeError("socket busy")
        self.subs.append(list(toks))
        if self.on_sub:
            await self.on_sub()

    async def unsubscribe_scrips(self, toks):
        self.unsubs.append(list(toks))


def test_subscription_sync_converges_after_inflight_watch_change():
    b = _bridge()
    ws = _SyncWs()
    k1, k2, k3 = (("nse_fo", "1"), ("nse_fo", "2"), ("nse_fo", "3"))
    b._ws = ws
    b._wanted = {k1, k2}
    b._subscribed = set()
    b._sub_running = True

    async def change_while_subscribing():
        if len(ws.subs) != 1:
            return
        with b._lock:
            b._wanted.discard(k2)
            b._wanted.add(k3)
            b._sub_dirty = True

    ws.on_sub = change_while_subscribing
    asyncio.run(b._sync_subscriptions(ws))

    assert len(ws.subs) == 2
    assert len(ws.unsubs) == 1
    assert b._subscribed == {k1, k3}
    assert b._sub_running is False
    b.stop()


def test_subscription_sync_retries_without_dropping_state():
    b = _bridge()
    ws = _SyncWs()
    ws.fail_first = True
    k1 = ("nse_fo", "1")
    b._ws = ws
    b._wanted = {k1}
    b._subscribed = set()
    b._sub_running = True
    b._sub_retry_base_sec = 0.001
    b._sub_retry_max_sec = 0.002

    asyncio.run(b._sync_subscriptions(ws))

    assert ws.attempts == 2
    assert b._subscribed == {k1}
    assert b._sub_running is False
    b.stop()


# ---------------- WP10: health + stale alert ----------------
class FakeEngine:
    def __init__(self, legs):
        self._legs = legs

    def all(self):
        return [dict(o) for o in self._legs]

    def entry_intent_health(self):
        return {"counts": {}, "pending": [
            {"tag": "tag-1", "entry_order_id": "E9", "superseded_by": "E10"}]}


def test_health_endpoint_exposes_liveness_and_leg_counts(monkeypatch):
    from app import main

    legs = [
        {"entry_order_id": "E1", "status": "ACTIVE", "naked_position": False},
        {"entry_order_id": "E2", "status": "ACTIVE", "naked_position": True,
         "exit_pending": False},
        {"entry_order_id": "E3", "status": "ERRORED", "naked_position": False,
         "blocked_reason": "EXIT_REJECTED"},
        {"entry_order_id": "E4", "status": "CLOSED", "naked_position": False},
    ]
    monkeypatch.setattr(main, "engine", FakeEngine(legs))
    now = _now_ms()
    monkeypatch.setattr(main.bridge, "_last_order_msg_ms", now - 1500)
    monkeypatch.setattr(main.bridge, "_last_tick_ms", now - 700)
    monkeypatch.setattr(main.bridge, "_last_reconcile_ms", now - 2500)
    monkeypatch.setattr(main.bridge, "_last_reconcile_summary",
                        {"checked": 2, "naked": ["E2"]})

    with TestClient(main.app) as c:
        r = c.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["open_legs"] == 3
    assert body["naked_legs"] == 1
    assert body["errored_legs"] == 1
    assert "naked_leg_ids" not in body
    assert "errored_leg_ids" not in body
    assert body["feed_order_age_ms"] >= 1000
    assert body["feed_tick_age_ms"] >= 500
    assert body["last_reconcile_ms"] == now - 2500
    assert body["last_reconcile"]["naked"] == 1
    assert "E2" not in str(body)
    assert "E9" not in str(body) and "E10" not in str(body)


def test_feed_hang_alerts_once_and_clears_on_recovery(caplog):
    b = _bridge()
    b.feed_stale_sec = 1.0
    b._last_order_msg_ms = _now_ms() - 1500

    class Connected:
        is_connected = True

    feed = Connected()
    with caplog.at_level(logging.WARNING, logger="sfeed"):
        assert b._watchdog_tick(feed) is False
    hits = [r for r in caplog.records if "FEED STALE" in r.getMessage()]
    assert len(hits) == 1
    h = b.health()
    assert h["feed_stale"] is True
    assert h["feed_stale_since_ms"] is not None
    assert h["feed_stale_age_ms"] >= 0

    # Still hung: no duplicate alert spam.
    with caplog.at_level(logging.WARNING, logger="sfeed"):
        b._watchdog_tick(feed)
    assert len([r for r in caplog.records
                if "FEED STALE" in r.getMessage()]) == 1

    b._last_order_msg_ms = _now_ms()
    b._watchdog_tick(feed)
    assert b.health()["feed_stale"] is False
    b.stop()


def test_sl_placement_is_logged(caplog):
    broker = MagicMock()
    broker.place_stop_loss.return_value = {"stCode": 200, "nOrdNo": "SL9"}
    broker.average_fill_price.return_value = None
    engine = SyntheticEngine(broker)
    engine.add({"entry_order_id": "E1", "sl_order_id": None,
                "trading_symbol": "NIFTY25N1825000CE",
                "status": "PENDING_ENTRY", "strategy": "market_exit",
                "target_price": "120.00", "stop_loss_price": "90.00",
                "trail_distance": 5, "high_water_mark": 0.0,
                "low_water_mark": 0.0, "quantity": "75",
                "exchange_segment": "nse_fo", "instrument_token": "tok",
                "side": "B", "product": "MIS", "target_points": "20",
                "stop_points": "10"})
    with caplog.at_level(logging.INFO, logger="synthetic"):
        engine.on_order_update({"nOrdNo": "E1", "ordSt": "complete",
                                "prc": "100.00", "fldQty": 75})
    assert any("SL PLACED" in r.getMessage() for r in caplog.records)


# ---------------- WP11: stacked entries + passive warning ----------------
class PlaceBroker:
    master_ok = True

    def __init__(self):
        self.calls = 0

    def token_for(self, *_a, **_kw):
        return "tok"

    def place_order(self, **_kw):
        self.calls += 1
        return {"stCode": 200, "nOrdNo": f"E{self.calls}"}


def _wire(monkeypatch, broker):
    engine = SyntheticEngine(broker)
    monkeypatch.setattr(orders_routes, "kotak", broker)
    monkeypatch.setattr(orders_routes, "engine", engine)
    monkeypatch.setattr(orders_routes, "hub", None)
    monkeypatch.setattr(orders_routes, "bridge", None)
    monkeypatch.setattr(orders_routes, "worker", None)
    monkeypatch.setattr(orders_routes, "read_worker", None)
    return engine


def _single(tag, **over):
    fields = {"instrument": "NIFTY", "expiry": "25N18", "option_type": "CE",
              "strike": "25000", "lots": 1, "entry_mode": "MKT", "side": "B",
              "client_order_tag": tag}
    fields.update(over)
    return SingleOrderBody(**fields)


def test_stacked_entry_is_blocked_by_default(monkeypatch):
    broker = PlaceBroker()
    engine = _wire(monkeypatch, broker)
    monkeypatch.setattr(settings, "allow_stacked_entries", False)

    orders_routes.place_single(_single("stack-a"), ref_ltp=100.0)
    with pytest.raises(HTTPException) as err:
        orders_routes.place_single(_single("stack-b"), ref_ltp=100.0)
    assert err.value.status_code == 409
    assert "already has an open bracket" in err.value.detail
    assert broker.calls == 1
    assert list(engine.orders) == ["E1"]


def test_stacked_entry_allowed_when_configured(monkeypatch):
    broker = PlaceBroker()
    _wire(monkeypatch, broker)
    monkeypatch.setattr(settings, "allow_stacked_entries", True)

    orders_routes.place_single(_single("stack-c"), ref_ltp=100.0)
    out = orders_routes.place_single(_single("stack-d"), ref_ltp=100.0)
    assert out["entry_order_id"] == "E2"
    assert broker.calls == 2


def test_errored_leg_without_live_orders_does_not_block_entry(monkeypatch):
    broker = PlaceBroker()
    engine = _wire(monkeypatch, broker)
    monkeypatch.setattr(settings, "allow_stacked_entries", False)
    engine.add({"entry_order_id": "OLD", "status": "ERRORED",
                "trading_symbol": "NIFTY25N1825000CE"})

    out = orders_routes.place_single(_single("stack-e"), ref_ltp=100.0)
    assert out["entry_order_id"] == "E1"


def test_errored_leg_with_live_order_still_blocks_entry(monkeypatch):
    broker = PlaceBroker()
    engine = _wire(monkeypatch, broker)
    monkeypatch.setattr(settings, "allow_stacked_entries", False)
    engine.add({"entry_order_id": "OLD", "status": "ERRORED",
                "trading_symbol": "NIFTY25N1825000CE",
                "naked_position": True})

    with pytest.raises(HTTPException) as err:
        orders_routes.place_single(_single("stack-f"), ref_ltp=100.0)
    assert err.value.status_code == 409
    assert broker.calls == 0


def test_passive_limit_far_from_ltp_warns_but_places(monkeypatch):
    broker = PlaceBroker()
    _wire(monkeypatch, broker)
    monkeypatch.setattr(settings, "allow_stacked_entries", False)
    monkeypatch.setattr(settings, "max_passive_offset", 5.0)

    out = orders_routes.place_single(
        _single("passive-1", entry_mode="L", limit_price=80.0), ref_ltp=100.0)
    assert broker.calls == 1
    assert out["warnings"] and "away from LTP" in out["warnings"][0]


def test_passive_warning_absent_inside_threshold(monkeypatch):
    _wire(monkeypatch, PlaceBroker())
    monkeypatch.setattr(settings, "max_passive_offset", 5.0)
    out = orders_routes.place_single(
        _single("passive-2", entry_mode="L", limit_price=98.0), ref_ltp=100.0)
    assert not out.get("warnings")


# ---------------- WP11: CLOSED pruning ----------------
def test_closed_trackers_are_pruned_by_cap_and_open_legs_kept(monkeypatch):
    monkeypatch.setattr(settings, "closed_trackers_max", 500)
    monkeypatch.setattr(settings, "closed_retention_sec", 86400.0)
    engine = SyntheticEngine(MagicMock())
    now = _now_ms()
    for i in range(5):
        engine.add({"entry_order_id": f"C{i}", "status": "CLOSED",
                    "trading_symbol": "X", "closed_at_ms": now - (i + 1) * 1000})
    engine.add({"entry_order_id": "OPEN", "status": "ACTIVE",
                "trading_symbol": "X"})

    monkeypatch.setattr(settings, "closed_trackers_max", 2)
    removed = engine.prune_closed()
    assert removed == 3
    assert set(engine.orders) == {"C0", "C1", "OPEN"}


def test_closed_trackers_are_pruned_by_age(monkeypatch):
    monkeypatch.setattr(settings, "closed_trackers_max", 500)
    monkeypatch.setattr(settings, "closed_retention_sec", 60.0)
    engine = SyntheticEngine(MagicMock())
    old = _now_ms() - 120_000
    engine.add({"entry_order_id": "OLD", "status": "CLOSED",
                "trading_symbol": "X", "closed_at_ms": old})
    engine.add({"entry_order_id": "ERRORED", "status": "ERRORED",
                "trading_symbol": "X", "closed_at_ms": old})
    engine.add({"entry_order_id": "NEW", "status": "CLOSED",
                "trading_symbol": "X", "closed_at_ms": _now_ms()})

    engine.prune_closed()
    assert set(engine.orders) == {"ERRORED", "NEW"}


def test_closed_prune_keeps_late_exit_and_cancel_work(monkeypatch):
    monkeypatch.setattr(settings, "closed_trackers_max", 500)
    monkeypatch.setattr(settings, "closed_retention_sec", 1.0)
    engine = SyntheticEngine(MagicMock())
    old = _now_ms() - 120_000
    engine.add({"entry_order_id": "EXIT_PENDING", "status": "CLOSED",
                "trading_symbol": "X", "closed_at_ms": old,
                "exit_pending": True})
    engine.add({"entry_order_id": "LATE_EXIT", "status": "CLOSED",
                "trading_symbol": "X", "closed_at_ms": old,
                "exit_order_id": "X1", "cancel_requested_ids": {"X1": 1}})
    engine.add({"entry_order_id": "ENTRY_CANCEL", "status": "CLOSED",
                "trading_symbol": "X", "closed_at_ms": old,
                "entry_cancel_pending": True})
    engine.add({"entry_order_id": "PLAIN", "status": "CLOSED",
                "trading_symbol": "X", "closed_at_ms": old})

    monkeypatch.setattr(settings, "closed_trackers_max", 1)
    engine.prune_closed()
    assert set(engine.orders) == {
        "EXIT_PENDING", "LATE_EXIT", "ENTRY_CANCEL"}


def test_closed_tracker_cap_zero_disables_only_count_pruning(monkeypatch):
    monkeypatch.setattr(settings, "closed_trackers_max", 0)
    monkeypatch.setattr(settings, "closed_retention_sec", 86400.0)
    engine = SyntheticEngine(MagicMock())
    now = _now_ms()
    engine.add({"entry_order_id": "C1", "status": "CLOSED",
                "trading_symbol": "X", "closed_at_ms": now})
    engine.add({"entry_order_id": "C2", "status": "CLOSED",
                "trading_symbol": "X", "closed_at_ms": now})

    assert engine.prune_closed() == 0
    assert set(engine.orders) == {"C1", "C2"}


def test_favicon_does_not_404():
    from app import main

    with TestClient(main.app) as c:
        r = c.get("/favicon.ico")
    assert r.status_code == 204
