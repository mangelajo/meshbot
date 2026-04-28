"""Unified SQLite storage for meshbot state.

Phase 0 of the JSON->SQLite migration. Provides only the scaffolding:
the connection, WAL pragma, a schema_version table, and a migration
runner. No domain tables yet — phases 1-5 register their migrations on
the module-level MIGRATIONS list.

Also auto-renames the legacy `messages.db` (and its WAL sidecars) to
`meshbot.db` on first instantiation so existing deployments upgrade
transparently. The rename only fires when the canonical path doesn't
exist yet, so it's safe to call repeatedly.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Callable

logger = logging.getLogger("meshbot.state")

DB_FILENAME = "meshbot.db"
LEGACY_DB_FILENAME = "messages.db"
_DB_SUFFIXES = ("", "-wal", "-shm")

Migration = Callable[[sqlite3.Connection], None]

# (version, callable) tuples applied in order. Each callable receives an
# open sqlite3.Connection inside an active transaction. Empty for Phase
# 0; subsequent phases prepend their schema work here.
MIGRATIONS: list[tuple[int, Migration]] = []


class StateStore:
    """Owns the meshbot SQLite connection and applies pending migrations."""

    def __init__(
        self,
        db_path: str | Path,
        migrations: list[tuple[int, Migration]] | None = None,
    ) -> None:
        self._path = Path(db_path)
        self._maybe_rename_legacy()
        self._conn = sqlite3.connect(str(self._path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._migrations = migrations if migrations is not None else MIGRATIONS
        self._init_schema()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    def _maybe_rename_legacy(self) -> None:
        if self._path.name != DB_FILENAME:
            return
        legacy = self._path.parent / LEGACY_DB_FILENAME
        if not legacy.exists() or self._path.exists():
            return
        for suffix in _DB_SUFFIXES:
            src = legacy.parent / f"{LEGACY_DB_FILENAME}{suffix}"
            dst = self._path.parent / f"{DB_FILENAME}{suffix}"
            if src.exists():
                src.rename(dst)
        logger.info("Renamed %s -> %s", legacy, self._path)

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)"
        )
        cur.execute("SELECT max(version) FROM schema_version")
        row = cur.fetchone()
        current = row[0] if row and row[0] is not None else 0
        for version, fn in self._migrations:
            if version <= current:
                continue
            logger.info("Applying schema migration v%d", version)
            # Explicit transaction control: Python 3.12+'s implicit
            # transaction handling in legacy mode is fuzzy around DDL,
            # so we begin/commit/rollback by hand to guarantee the
            # whole migration (CREATE TABLEs + version row) is atomic.
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                fn(self._conn)
                self._conn.execute(
                    "INSERT INTO schema_version(version) VALUES (?)", (version,)
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
