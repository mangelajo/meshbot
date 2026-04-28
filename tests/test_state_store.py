"""Tests for the SQLite state store scaffold."""

import sqlite3
import tempfile
from pathlib import Path

from meshbot.bot.state_store import (
    DB_FILENAME,
    LEGACY_DB_FILENAME,
    StateStore,
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
