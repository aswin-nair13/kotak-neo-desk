"""Plan 06 WP5: reconcile manual/external broker actions.

Covers the broker-app workflow: a manual exit leaves an unknown order id and a
flat position, a manual SL trigger modify must land in the tracker/UI, and an
external flat must be confirmed by two fresh snapshots (one empty/lagging
response must never cancel a live SL). Also covers the CLOSING leg, which used
to be driven straight into an exit even when the position was already flat.
"""
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings
from app.sfeed_bridge import map_book_entry, map_order_data
from app.synthetic_engine import SyntheticEngine


def _leg(eid="E1", status="ACTIVE", sl="SL1", **over):
    d = {"entry_order_id": eid, "sl_order_id": sl,
         "trading_symbol": "X", "status": status, "strategy": "market_exit",
         "target_price": "120.00", "stop_loss_price": "90.00",
         "trail_distance": 5, "high_water_mark": 0.0, "low_water_mark": 0.0,
         "quantity": "20", "exchange_segment": "nse_fo",
         "instrument_token": "tok", "side": "B", "product": "MIS",
         "target_points": "20", "stop_points": "10", "fill_price": "100.00",
         "sl_placed": True, "sl_quantity": 20,
         "sl_last_seen_ms": int(time.time() * 1000)}
    d.update(over)
    return d


def _book(oid, st="open", **over):
    d = {"nOrdNo": oid, "ordSt": st, "trdSym": "X", "trnsTp": "S",
         "qty": 20, "prc": "100.00", "fldQty": 0, "rejRsn": "--"}
    d.update(over)
    return d


class SyncBroker:
    def __init__(self):
        self.placed = []
        self.cancels = []
        self.exits = []
        self.cancel_fail_ids: set[str] = set()

    def place_stop_loss(self, *_a, **_kw):
        oid = f"SLX{len(self.placed) + 1}"
        self.placed.append(oid)
        return {"stCode": 200, "nOrdNo": oid}

    def place_market_exit(self, *_a, **_kw):
        oid = f"X{len(self.exits) + 1}"
        self.exits.append(oid)
        return {"stCode": 200, "nOrdNo": oid}

    def average_fill_price(self, _oid):
        return None

    def cancel(self, oid):
        self.cancels.append(oid)
        if oid in self.cancel_fail_ids:
            return {"stCode": 200, "stat": "Not_Ok", "errMsg": "rejected"}
        return {"stCode": 200}


class OrderData:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def test_unknown_manual_exit_and_flat_position_closes_and_cancels_sl_once():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())
    book = [_book("SL1", "open"), _book("MANUAL-EXIT", "complete")]

    # One fresh flat snapshot must not close/cancel a live leg.
    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "ACTIVE"
    assert leg["blocked_reason"] == "POSITION_FLAT_UNCONFIRMED"
    assert broker.cancels == []

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_reason"] == "EXTERNAL_FLAT"
    assert leg["source"] == "external"
    assert leg["last_synced_ms"]
    assert broker.cancels == ["SL1"]          # working SL cancelled exactly once
    assert broker.exits == []                  # never re-open/flip the position

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    assert broker.cancels == ["SL1"]           # CLOSED legs are not re-cancelled


def test_single_empty_positions_success_does_not_close_then_two_do():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())
    book = [_book("SL1", "open")]

    engine.reconcile(book, {}, positions_ok=True)
    assert engine.orders["E1"]["status"] == "ACTIVE"
    assert engine.orders["E1"]["position_flat_streak"] == 1
    assert broker.cancels == []

    engine.reconcile(book, {}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_reason"] == "EXTERNAL_FLAT"
    assert broker.cancels == ["SL1"]


def test_closing_flat_requires_two_snapshots_and_never_fires_exit():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="CLOSING"))
    book = [_book("SL1", "open")]

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSING"
    assert leg["blocked_reason"] == "POSITION_FLAT_UNCONFIRMED"
    assert broker.exits == []
    assert broker.cancels == []

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_reason"] == "EXTERNAL_FLAT"
    assert broker.exits == []
    assert broker.cancels == ["SL1"]


def test_manual_trigger_modify_from_feed_updates_tracker_and_is_not_fought():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())

    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "open",
                            "trigger_price": "95.50", "qty": 20})
    leg = engine.orders["E1"]
    assert leg["stop_loss_price"] == "95.50"
    assert leg["sl_quantity"] == 20
    assert leg["source"] == "external"
    assert leg["last_synced_ms"]

    # A later reconcile sees the same working SL: no re-place at the old stop.
    engine.reconcile([_book("SL1", "open", trigger_price="95.50")],
                     {"X": {"qty": 20}}, positions_ok=True)
    assert engine.orders["E1"]["stop_loss_price"] == "95.50"
    assert broker.placed == []


def test_manual_trigger_modify_from_book_updates_tracker():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())

    engine.reconcile([_book("SL1", "open", trigger_price="96.25")],
                     {"X": {"qty": 20}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["stop_loss_price"] == "96.25"
    assert leg["source"] == "external"
    assert leg["last_synced_ms"]
    assert broker.placed == []


def test_sl_modify_pending_blocks_automatic_replacement():
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "sl_visibility_grace_sec", 0.0)
    try:
        broker = SyncBroker()
        engine = SyntheticEngine(broker)
        engine.add(_leg())
        assert engine.begin_sl_modify("SL1", "95.00")

        # SL temporarily absent from the book while the manual modify is in
        # flight: do not re-place the old stop.
        engine.reconcile([], {"X": {"qty": 20}}, positions_ok=True)
        leg = engine.orders["E1"]
        assert leg["status"] == "ACTIVE"
        assert leg["sl_modify_pending"] is True
        assert broker.placed == []

        # Feed echoes the new trigger: pending settles, book state wins.
        engine.on_order_update({"nOrdNo": "SL1", "ordSt": "open",
                                "trigger_price": "95.00", "qty": 20})
        leg = engine.orders["E1"]
        assert leg["sl_modify_pending"] is False
        assert leg["stop_loss_price"] == "95.00"
    finally:
        monkeypatch.undo()


def test_entry_expired_at_broker_becomes_terminal():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="PENDING_ENTRY", sl=None))

    engine.reconcile([_book("E1", "expired")], {}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_reason"] == "ENTRY_EXPIRED"
    assert broker.placed == []


def test_closing_external_flat_cancels_working_exit_and_sl():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="CLOSING", exit_order_id="X1",
                    exit_client_tag="TAGX"))
    book = [_book("SL1", "open"),
            _book("X1", "open", GuiOrdId="TAGX")]

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    assert engine.orders["E1"]["status"] == "CLOSING"
    assert broker.cancels == []

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["exit_reason"] == "EXTERNAL_FLAT"
    assert sorted(broker.cancels) == ["SL1", "X1"]
    assert broker.exits == []

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    assert sorted(broker.cancels) == ["SL1", "X1"]   # exit cancelled exactly once


def test_closed_leg_live_stale_sl_retries_bounded(monkeypatch):
    monkeypatch.setattr(settings, "closed_sl_cancel_max_attempts", 2)
    # Every reconcile re-confirms the still-visible stale SL immediately.
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="CLOSED"))
    book = [_book("SL1", "open")]

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert broker.cancels == ["SL1"]
    assert leg["closed_sl_cancel_attempts"] == 1

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    assert broker.cancels == ["SL1", "SL1"]
    assert leg["late_sl_risk"] is True

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    assert broker.cancels == ["SL1", "SL1"]   # retries exhausted, no loop


def test_manual_sl_cancel_failure_still_marks_naked(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "sl_visibility_grace_sec", 0.0)
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())

    engine.note_manual_cancel("SL1", False, "broker timeout")
    leg = engine.orders["E1"]
    assert leg["manual_sl_cancel"] is True
    assert leg["manual_sl_cancel_pending"] is True
    assert engine.manual_cancel_requested("SL1") is True
    leg["manual_sl_cancel_deadline_ms"] = int(time.time() * 1000) - 1
    assert engine.manual_cancel_requested("SL1") is False
    leg["manual_sl_cancel_deadline_ms"] = int(time.time() * 1000) + 60000

    engine.reconcile([], {"X": {"qty": 20}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "SL_CANCELLED_MANUAL"
    assert leg["naked_position"] is True
    assert leg["manual_sl_cancel_pending"] is False
    assert leg["manual_sl_cancel_confirmed"] is True
    assert broker.placed == []


def test_manual_sl_cancel_missing_sl_without_positions_is_still_naked(
        monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 1)
    monkeypatch.setattr(settings, "sl_visibility_grace_sec", 0.0)
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())
    engine.note_manual_cancel("SL1", False, "broker timeout")

    engine.reconcile([], {}, positions_ok=False)
    leg = engine.orders["E1"]
    assert leg["status"] == "ERRORED"
    assert leg["blocked_reason"] == "SL_CANCELLED_MANUAL"
    assert leg["naked_position"] is True
    assert leg["manual_sl_cancel_pending"] is False
    assert broker.placed == []


def test_manual_sl_cancel_still_working_after_deadline_alerts_no_replace():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())
    engine.note_manual_cancel("SL1", False, "broker timeout")
    engine.orders["E1"]["manual_sl_cancel_deadline_ms"] = \
        int(time.time() * 1000) - 1

    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "open",
                            "trigger_price": "90.00", "qty": 20})
    leg = engine.orders["E1"]
    assert leg["manual_sl_cancel"] is True
    assert leg["manual_sl_cancel_pending"] is False
    assert leg["manual_sl_cancel_still_working_ms"]
    assert leg["status"] == "ACTIVE"
    assert broker.placed == []


def test_app_initiated_manual_cancel_is_labelled_app():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())

    engine.note_manual_cancel("SL1", True)
    assert engine.orders["E1"]["source"] == "app"


def test_manual_cancel_does_not_mark_unrelated_or_terminal_ids():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg(eid="E1", status="CLOSING", exit_order_id="X1"))
    engine.add(_leg(eid="E2", status="CLOSED", sl="SL2"))

    engine.note_manual_cancel("X1", True)
    assert not engine.orders["E1"].get("manual_sl_cancel")
    engine.note_manual_cancel("SL2", True)
    assert not engine.orders["E2"].get("manual_sl_cancel")


def test_stale_book_echo_does_not_revert_newer_sl_and_source_stays_latest():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())

    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "open",
                            "trigger_price": "95.00", "qty": 20})
    assert engine.orders["E1"]["source"] == "external"

    # A later feed frame with the same trigger is attributed to the feed again
    # (source must reflect the latest read, not stay latched on external).
    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "open",
                            "trigger_price": "95.00", "qty": 20})
    assert engine.orders["E1"]["source"] == "feed"

    old_ts = int(time.time() * 1000) - 60000
    # Versioned by the broker *update* time (not the order creation time).
    engine.reconcile([_book("SL1", "open", trigger_price="90.00",
                            update_ts_ms=old_ts, update_ts_source="update")],
                     {"X": {"qty": 20}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["stop_loss_price"] == "95.00"
    assert leg["source"] == "feed"


def test_manual_modify_adopted_with_old_creation_but_new_update_time():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())
    assert engine.begin_sl_modify("SL1", 95.0) is True

    # An IST/UTC host parses the creation time as arbitrarily old; only the
    # update timestamp may gate versioning, so the modify must still land.
    old_created = int(time.time() * 1000) - 3600_000
    new_update = int(time.time() * 1000) + 1000
    engine.reconcile([_book("SL1", "open", trigger_price="95.00",
                            ord_time=str(old_created),
                            update_ts_ms=new_update,
                            update_ts_source="update")],
                     {"X": {"qty": 20}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["stop_loss_price"] == "95.00"
    assert leg["sl_modify_pending"] is False
    assert leg["stop_loss_source"] == "app"


def test_creation_only_timestamp_never_rejects_a_legitimate_row():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())
    # No update timestamp: the mapper falls back to the creation time. That
    # must not be treated as a version, or the row is silently dropped.
    old_created = int(time.time() * 1000) - 3600_000
    engine.reconcile([_book("SL1", "open", trigger_price="95.00",
                            ord_time=str(old_created),
                            update_ts_ms=old_created,
                            update_ts_source="created")],
                     {"X": {"qty": 20}}, positions_ok=True)
    assert engine.orders["E1"]["stop_loss_price"] == "95.00"


def test_book_entry_prefers_update_time_over_creation():
    old_created = 1_700_000_000_000
    new_update_ns = 1_750_000_000_000_000_000
    row = map_book_entry({"nOrdNo": "S1", "ordSt": "open", "trgPrc": "95.00",
                          "ordDtTm": str(old_created),
                          "updRecvTm": new_update_ns})
    assert row["ord_time"] == str(old_created)
    assert row["update_ts_source"] == "update"
    assert row["update_ts_ms"] == 1_750_000_000_000


def test_creation_only_book_row_falls_back_to_creation_source():
    row = map_book_entry({"nOrdNo": "S1", "ordSt": "open",
                          "ordDtTm": "1700000000000"})
    assert row["update_ts_source"] == "created"
    assert row["update_ts_ms"] == 1_700_000_000_000


def test_reconcile_terminal_sl_is_attributed_to_reconcile():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg())

    engine.reconcile([_book("SL1", "complete")], {"X": {"qty": 20}},
                     positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["source"] == "reconcile"


def test_own_trailing_echo_is_not_external_and_ambiguous_exit_is_driven():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="TRAILING", high_water_mark=100.0))
    leg = engine.orders["E1"]
    leg["sl_app_modify_target"] = 95.0

    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "open",
                            "trigger_price": "95.00", "qty": 20})
    leg = engine.orders["E1"]
    assert leg["stop_loss_price"] == "95.00"
    assert leg["source"] == "feed"
    assert "sl_app_modify_target" not in leg


def test_errored_ambiguous_exit_resolves_before_naked_branch():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="ERRORED", exit_order_id="X1",
                    exit_client_tag="TAGX", naked_position=True,
                    blocked_reason="EXIT_AMBIGUOUS", exit_ambiguous=True))
    book = [_book("SL1", "open", GuiOrdId="TAGSL"),
            _book("X1", "open", GuiOrdId="TAGX")]

    engine.reconcile(book, {"X": {"qty": 20}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSING"
    assert leg["exit_ambiguous"] is False


def test_own_trailing_echo_after_stop_already_updated_clears_marker():
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="TRAILING", high_water_mark=100.0))
    leg = engine.orders["E1"]
    # `_trail` already moved the tracker trigger; the feed echo confirms our
    # own modify and must not clear/attribute anything as external.
    leg["stop_loss_price"] = "95.00"
    leg["sl_app_modify_target"] = 95.0

    engine.on_order_update({"nOrdNo": "SL1", "ordSt": "open",
                            "trigger_price": "95.00", "qty": 20})
    leg = engine.orders["E1"]
    assert leg["stop_loss_price"] == "95.00"
    assert leg["source"] == "feed"
    assert "sl_app_modify_target" not in leg


def test_safe_cancel_treats_stat_not_ok_as_failure():
    broker = SyncBroker()
    broker.cancel_fail_ids = {"SL1"}
    engine = SyntheticEngine(broker)

    assert engine._safe_cancel("SL1", "unit") is False
    assert engine._safe_cancel("SL2", "unit") is True


def test_external_flat_sl_cancel_failure_alerts_critical(caplog):
    import logging

    broker = SyncBroker()
    broker.cancel_fail_ids = {"SL1"}
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="CLOSING"))
    book = [_book("SL1", "open")]

    with caplog.at_level(logging.CRITICAL, logger="synthetic"):
        engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
        engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)

    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["late_sl_risk"] is True
    assert any("could not cancel working SL" in r.message
               for r in caplog.records)


def test_successful_external_flat_sl_cancel_reconfirms_stale_sl(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 2)
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="CLOSING"))
    book = [_book("SL1", "open")]

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["closed_sl_cancel_ok_ms"]
    assert broker.cancels == ["SL1"]

    # First fresh echo is only a confirmation counter, no duplicate cancel.
    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    assert broker.cancels == ["SL1"]
    # Second confirmation proves the SL is still working -> retry once.
    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    assert broker.cancels == ["SL1", "SL1"]
    assert engine.orders["E1"]["closed_sl_cancel_attempts"] == 2


def test_external_flat_entry_cancel_failure_is_retried_and_alerted(
        monkeypatch, caplog):
    import logging

    monkeypatch.setattr(settings, "entry_cancel_max_attempts", 1)
    broker = SyncBroker()
    broker.cancel_fail_ids = {"E1"}
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="CLOSING"))
    book = [_book("SL1", "open"), _book("E1", "open")]

    with caplog.at_level(logging.CRITICAL, logger="synthetic"):
        engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
        engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["external_flat_entry_cancel_error_ms"]
    assert broker.cancels.count("E1") == 1

    with caplog.at_level(logging.CRITICAL, logger="synthetic"):
        engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    assert broker.cancels.count("E1") == 1     # budget exhausted, no loop
    assert engine.orders["E1"]["external_flat_entry_cancel_alerted"] is True
    assert any("ENTRY CANCEL RETRIES EXHAUSTED" in r.message
               for r in caplog.records)


def test_successful_external_flat_entry_cancel_reconfirms_stale_entry(monkeypatch):
    monkeypatch.setattr(settings, "sl_missing_confirmations", 2)
    broker = SyncBroker()
    engine = SyntheticEngine(broker)
    engine.add(_leg(status="CLOSING"))
    book = [_book("SL1", "open"), _book("E1", "open")]

    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    leg = engine.orders["E1"]
    assert leg["status"] == "CLOSED"
    assert leg["external_flat_entry_cancel_ok_ms"]
    assert broker.cancels.count("E1") == 1

    # A bad ack is not trusted forever: first fresh echo only counts.
    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    assert broker.cancels.count("E1") == 1
    # Second confirmation proves the entry is still working -> retry once.
    engine.reconcile(book, {"X": {"qty": 0}}, positions_ok=True)
    assert broker.cancels.count("E1") == 2
    assert engine.orders["E1"]["external_flat_entry_cancel_attempts"] == 2


def test_mappers_carry_trigger_price_and_filled_qty():
    od = map_order_data(OrderData(
        order_id="O1", order_status="open", trading_symbol="X",
        transaction_type="S", quantity=20, average_price="0",
        filled_quantity=5, trigger_price="95.50"))
    assert od["trigger_price"] == "95.50"
    assert od["fldQty"] == 5

    row = map_book_entry({"nOrdNo": "O1", "ordSt": "open", "trdSym": "X",
                          "trnsTp": "S", "qty": "20", "trgPrc": "96.25",
                          "fldQty": "5", "ordDtTm": "1700000000000"})
    assert row["trigger_price"] == "96.25"
    assert row["filled_qty"] == "5"
    assert row["ord_time"] == "1700000000000"
