"""Tests for the SQLite state store scaffold and Phase 1 advert tables."""

import json
import sqlite3
import tempfile
import time
from pathlib import Path

from meshbot.bot.state_store import (
    DB_FILENAME,
    LEGACY_DB_FILENAME,
    StateStore,
    import_adverts_from_json,
    import_routes_from_json,
)


def _tmp() -> Path:
    return Path(tempfile.mkdtemp())


def test_creates_schema_version_table_on_first_open():
    s = StateStore(_tmp() / DB_FILENAME)
    cur = s.conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
    )
    assert cur.fetchone() is not None
    s.close()


def test_wal_journal_mode_active():
    s = StateStore(_tmp() / DB_FILENAME)
    cur = s.conn.cursor()
    cur.execute("PRAGMA journal_mode")
    assert cur.fetchone()[0].lower() == "wal"
    s.close()


def test_renames_legacy_db_when_only_legacy_exists():
    d = _tmp()
    legacy = d / LEGACY_DB_FILENAME
    conn = sqlite3.connect(str(legacy))
    conn.execute("CREATE TABLE marker (id INTEGER)")
    conn.execute("INSERT INTO marker VALUES (42)")
    conn.commit()
    conn.close()

    s = StateStore(d / DB_FILENAME)
    assert (d / DB_FILENAME).exists()
    assert not legacy.exists()
    cur = s.conn.cursor()
    cur.execute("SELECT id FROM marker")
    assert cur.fetchone()[0] == 42
    s.close()


def test_does_not_rename_when_canonical_already_exists():
    d = _tmp()
    legacy = d / LEGACY_DB_FILENAME
    canonical = d / DB_FILENAME
    sqlite3.connect(str(legacy)).close()
    sqlite3.connect(str(canonical)).close()

    s = StateStore(canonical)
    # legacy must remain untouched if canonical was already there
    assert legacy.exists()
    s.close()


def test_does_not_rename_when_path_is_custom():
    d = _tmp()
    legacy = d / LEGACY_DB_FILENAME
    sqlite3.connect(str(legacy)).close()

    # Custom path != canonical: legacy must not move
    s = StateStore(d / "custom.db")
    assert legacy.exists()
    s.close()


def test_migrations_apply_once_and_persist_version():
    d = _tmp()
    calls: list[int] = []

    def mig_v1(conn: sqlite3.Connection) -> None:
        calls.append(1)
        conn.execute("CREATE TABLE foo (id INTEGER)")

    migrations = [(1, mig_v1)]
    s1 = StateStore(d / DB_FILENAME, migrations=migrations)
    s1.close()
    s2 = StateStore(d / DB_FILENAME, migrations=migrations)
    s2.close()
    assert calls == [1]


def test_record_advert_upserts_and_caps_history():
    s = StateStore(_tmp() / DB_FILENAME)
    pubkey = "abc123"
    now = time.time()
    for i in range(25):
        s.record_advert(
            pubkey=pubkey, name="Test", recv_at=now + i,
            adv_ts=int(now + i) - 5, drift=-5, snr=10.0, rssi=-20,
            path_len=2, adv_type=2, lat=40.0, lon=-3.0, path="d2",
            history_max=20,
        )
    cur = s.conn.cursor()
    cur.execute("SELECT count(*) FROM adverts WHERE pubkey=?", (pubkey,))
    assert cur.fetchone()[0] == 1, "single 'latest' row per pubkey"
    cur.execute("SELECT count(*) FROM adverts_history WHERE pubkey=?", (pubkey,))
    assert cur.fetchone()[0] == 20, "history capped at history_max"
    s.close()


def test_get_recent_adverts_filter_and_limit():
    s = StateStore(_tmp() / DB_FILENAME)
    base = time.time()
    for i, name in enumerate(["Madrid", "MadMesh001", "Loranca"]):
        s.record_advert(
            pubkey=f"pk{i:02d}", name=name, recv_at=base + i,
            adv_ts=int(base + i), drift=0, snr=None, rssi=None,
            path_len=1, adv_type=2, lat=None, lon=None,
        )
    # Filter by substring (case-insensitive)
    matches = s.get_recent_adverts(name_filter="mad", limit=10)
    assert {m["name"] for m in matches} == {"Madrid", "MadMesh001"}
    # Newest first
    matches = s.get_recent_adverts(limit=10)
    assert matches[0]["name"] == "Loranca"
    s.close()


def test_compute_clock_drift_stats():
    s = StateStore(_tmp() / DB_FILENAME)
    now = time.time()
    for i, drift in enumerate([0, -10, -200, -100000, -86400 * 50]):
        s.record_advert(
            pubkey=f"pk{i}", name=f"Node{i}", recv_at=now,
            adv_ts=int(now + drift), drift=drift,
            snr=None, rssi=None, path_len=1, adv_type=2,
            lat=None, lon=None,
        )
    stats = s.compute_clock_drift_stats(window_hours=48)
    assert stats["count"] == 5
    assert stats["within_30s_pct"] == 40   # 2 of 5 (0, -10)
    assert stats["over_1d_pct"] == 40       # 2 of 5 (-100000, -50d)
    assert stats["over_30d_pct"] == 20      # 1 of 5 (-50d)
    assert abs(stats["worst_drift_seconds"]) >= 86400 * 50
    s.close()


def test_import_adverts_from_json_renames_legacy():
    d = _tmp()
    legacy_json = d / "adverts_seen.json"
    legacy_json.write_text(json.dumps({
        "abc123": {
            "name": "TestNode",
            "first_seen": 1000.0, "last_seen": 2000.0,
            "last_adv_ts": 1995, "last_drift": -5,
            "last_snr": 12.0, "last_rssi": -20,
            "last_path_len": 3, "adv_type": 2,
            "lat": 40.0, "lon": -3.0,
        }
    }))
    s = StateStore(d / DB_FILENAME)
    assert import_adverts_from_json(s, d) == 1
    cur = s.conn.cursor()
    cur.execute("SELECT name, last_drift FROM adverts WHERE pubkey='abc123'")
    name, drift = cur.fetchone()
    assert name == "TestNode"
    assert drift == -5
    # JSON renamed
    assert not legacy_json.exists()
    assert (d / "adverts_seen.json.imported").exists()
    # Re-importing is a no-op once the table is populated
    assert import_adverts_from_json(s, d) == 0
    s.close()


def test_import_adverts_skips_when_table_already_populated():
    d = _tmp()
    s = StateStore(d / DB_FILENAME)
    s.record_advert(
        pubkey="existing", name="Already", recv_at=1.0, adv_ts=1,
        drift=0, snr=None, rssi=None, path_len=0, adv_type=2,
        lat=None, lon=None,
    )
    (d / "adverts_seen.json").write_text(json.dumps({"x": {"name": "ShouldNotImport"}}))
    assert import_adverts_from_json(s, d) == 0
    cur = s.conn.cursor()
    cur.execute("SELECT name FROM adverts")
    assert {r[0] for r in cur} == {"Already"}
    # JSON not renamed since we skipped
    assert (d / "adverts_seen.json").exists()
    s.close()


def test_record_route_dedupes_consecutive_same_route():
    s = StateStore(_tmp() / DB_FILENAME)
    base = time.time()
    s.record_route(contact_name="X", route="aa->bb", hops=2, seen_at=base)
    s.record_route(contact_name="X", route="aa->bb", hops=2, seen_at=base + 60)
    cur = s.conn.cursor()
    cur.execute("SELECT count(*) FROM routes_seen WHERE contact_name='X'")
    assert cur.fetchone()[0] == 1, "consecutive same route should not duplicate"
    cur.execute("SELECT seen_at FROM routes_seen WHERE contact_name='X'")
    assert abs(cur.fetchone()[0] - (base + 60)) < 1e-6
    s.close()


def test_record_route_caps_history():
    s = StateStore(_tmp() / DB_FILENAME)
    base = time.time()
    for i in range(25):
        s.record_route(
            contact_name="N", route=f"r{i}", hops=i + 1,
            seen_at=base + i, history_max=20,
        )
    cur = s.conn.cursor()
    cur.execute("SELECT count(*) FROM routes_seen WHERE contact_name='N'")
    assert cur.fetchone()[0] == 20
    cur.execute(
        "SELECT route FROM routes_seen WHERE contact_name='N' "
        "ORDER BY seen_at DESC LIMIT 1"
    )
    assert cur.fetchone()[0] == "r24"
    s.close()


def test_get_recent_routes_returns_newest_first():
    s = StateStore(_tmp() / DB_FILENAME)
    base = time.time()
    for i, r in enumerate(["alpha", "beta", "gamma"]):
        s.record_route(contact_name="X", route=r, hops=1, seen_at=base + i)
    assert s.get_recent_routes("X", limit=2) == ["gamma", "beta"]
    s.close()


def test_routes_by_name_pattern_filters_and_groups():
    s = StateStore(_tmp() / DB_FILENAME)
    now = time.time()
    s.record_route(contact_name="MadMesh", route="aa", hops=1, seen_at=now)
    s.record_route(contact_name="OtherNode", route="bb", hops=1, seen_at=now)
    s.record_route(contact_name="MadMesh", route="cc", hops=1, seen_at=now + 1)
    grouped = s.routes_by_name_pattern("madmesh", now - 60)
    assert set(grouped.keys()) == {"MadMesh"}
    assert [r["route"] for r in grouped["MadMesh"]] == ["cc", "aa"]
    s.close()


def test_import_routes_from_json_renames_legacy():
    d = _tmp()
    legacy_json = d / "routes_seen.json"
    legacy_json.write_text(json.dumps({
        "Alice": [
            {"route": "aa->bb", "hops": 2, "time": 1000.0},
            {"route": "cc->dd", "hops": 2, "time": 1100.0},
        ]
    }))
    s = StateStore(d / DB_FILENAME)
    assert import_routes_from_json(s, d) == 2
    cur = s.conn.cursor()
    cur.execute("SELECT count(*) FROM routes_seen WHERE contact_name='Alice'")
    assert cur.fetchone()[0] == 2
    assert not legacy_json.exists()
    assert (d / "routes_seen.json.imported").exists()
    # Subsequent calls are no-ops
    assert import_routes_from_json(s, d) == 0
    s.close()


def test_migration_failure_rolls_back():
    d = _tmp()

    def mig_bad(conn: sqlite3.Connection) -> None:
        conn.execute("CREATE TABLE bar (id INTEGER)")
        raise RuntimeError("boom")

    try:
        StateStore(d / DB_FILENAME, migrations=[(1, mig_bad)])
        assert False, "should have raised"
    except RuntimeError:
        pass

    # The new connection should not have the partial table or the version row
    s = StateStore(d / DB_FILENAME, migrations=[])
    cur = s.conn.cursor()
    cur.execute("SELECT count(*) FROM sqlite_master WHERE name='bar'")
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT count(*) FROM schema_version WHERE version=1")
    assert cur.fetchone()[0] == 0
    s.close()
