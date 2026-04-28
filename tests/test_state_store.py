"""Tests for the SQLite state store."""

import sqlite3
import tempfile
import time
from pathlib import Path

from meshbot.bot.message_store import MessageStore
from meshbot.bot.state_store import DB_FILENAME, StateStore


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
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT count(*) FROM adverts_history WHERE pubkey=?", (pubkey,))
    assert cur.fetchone()[0] == 20
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
    matches = s.get_recent_adverts(name_filter="mad", limit=10)
    assert {m["name"] for m in matches} == {"Madrid", "MadMesh001"}
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
    assert stats["within_30s_pct"] == 40
    assert stats["over_1d_pct"] == 40
    assert stats["over_30d_pct"] == 20
    assert abs(stats["worst_drift_seconds"]) >= 86400 * 50
    s.close()


def test_record_route_dedupes_consecutive_same_route():
    s = StateStore(_tmp() / DB_FILENAME)
    base = time.time()
    s.record_route(contact_name="X", route="aa->bb", hops=2, seen_at=base)
    s.record_route(contact_name="X", route="aa->bb", hops=2, seen_at=base + 60)
    cur = s.conn.cursor()
    cur.execute("SELECT count(*) FROM routes_seen WHERE contact_name='X'")
    assert cur.fetchone()[0] == 1
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


def test_record_seen_upserts():
    s = StateStore(_tmp() / DB_FILENAME)
    s.record_seen("Miguel", "#b0b0t", 1000.0)
    s.record_seen("Miguel", "DM", 2000.0)
    seen = s.get_last_seen("Miguel")
    assert seen == {"time": 2000.0, "channel": "DM"}
    s.close()


def test_get_last_seen_returns_none_for_unknown():
    s = StateStore(_tmp() / DB_FILENAME)
    assert s.get_last_seen("Nobody") is None
    s.close()


def test_messages_table_has_phase5_columns_on_fresh_install():
    """Fresh install: MessageStore creates messages with the new columns
    directly. The Phase 5 migration is a no-op for that path."""
    d = _tmp()
    s = StateStore(d / DB_FILENAME)
    ms = MessageStore(db_path=str(d / DB_FILENAME))
    cur = ms._conn.execute("PRAGMA table_info(messages)")
    cols = {row[1] for row in cur.fetchall()}
    assert "pubkey_prefix" in cols
    assert "direction" in cols
    ms.close()
    s.close()


def test_phase5_migration_alters_existing_messages_table():
    """Existing install: the v5 migration adds the missing columns."""
    d = _tmp()
    db = d / DB_FILENAME
    pre = sqlite3.connect(str(db))
    pre.executescript("""
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT NOT NULL,
            text TEXT NOT NULL,
            channel_name TEXT NOT NULL DEFAULT '',
            timestamp REAL NOT NULL,
            sender_timestamp INTEGER NOT NULL,
            is_private INTEGER NOT NULL DEFAULT 0,
            path_len INTEGER NOT NULL DEFAULT 0
        );
    """)
    pre.execute(
        "INSERT INTO messages (sender, text, timestamp, sender_timestamp) "
        "VALUES (?, ?, ?, ?)",
        ("Alice", "preexisting", 1000.0, 1000),
    )
    pre.commit()
    pre.close()

    s = StateStore(db)
    cur = s.conn.execute("PRAGMA table_info(messages)")
    cols = {row[1] for row in cur.fetchall()}
    assert "pubkey_prefix" in cols
    assert "direction" in cols
    cur = s.conn.execute("SELECT direction FROM messages WHERE sender='Alice'")
    assert cur.fetchone()[0] == "in"
    s.close()


def test_message_store_get_dm_history_returns_chronological():
    d = _tmp()
    s = StateStore(d / DB_FILENAME)
    ms = MessageStore(db_path=str(d / DB_FILENAME))
    s.conn.execute(
        "INSERT INTO messages "
        "(sender, text, channel_name, timestamp, sender_timestamp, "
        " is_private, path_len, pubkey_prefix, direction) "
        "VALUES "
        "('Miguel', 'first', 'DM', 1.0, 0, 1, 0, 'abc', 'in'),"
        "('b0b0t', 'second', 'DM', 2.0, 0, 1, 0, 'abc', 'out'),"
        "('Miguel', 'third', 'DM', 3.0, 0, 1, 0, 'abc', 'in')"
    )
    s.conn.commit()
    h = ms.get_dm_history("abc", limit=10)
    assert h == [("Miguel", "first"), ("b0b0t", "second"), ("Miguel", "third")]
    ms.close()
    s.close()


def test_migrations_apply_once_and_persist_version():
    d = _tmp()
    calls: list[int] = []

    def mig(conn: sqlite3.Connection) -> None:
        calls.append(1)
        conn.execute("CREATE TABLE injected (id INTEGER)")

    migrations = [(1, mig)]
    StateStore(d / DB_FILENAME, migrations=migrations).close()
    StateStore(d / DB_FILENAME, migrations=migrations).close()
    assert calls == [1]


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

    s = StateStore(d / DB_FILENAME, migrations=[])
    cur = s.conn.cursor()
    cur.execute("SELECT count(*) FROM sqlite_master WHERE name='bar'")
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT count(*) FROM schema_version WHERE version=1")
    assert cur.fetchone()[0] == 0
    s.close()
