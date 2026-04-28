"""Unified SQLite storage for meshbot state.

JSON->SQLite migration. Provides the connection, WAL pragma,
schema_version table, transactional migration runner, and the
domain-specific record/query helpers each phase adds (currently:
adverts).

Also auto-renames the legacy `messages.db` (and its WAL sidecars) to
`meshbot.db` on first instantiation so existing deployments upgrade
transparently. The rename only fires when the canonical path doesn't
exist yet, so it's safe to call repeatedly.
"""

import json
import logging
import sqlite3
import time
import unicodedata
from pathlib import Path
from typing import Any, Callable, Iterator

logger = logging.getLogger("meshbot.state")

DB_FILENAME = "meshbot.db"
LEGACY_DB_FILENAME = "messages.db"
_DB_SUFFIXES = ("", "-wal", "-shm")

Migration = Callable[[sqlite3.Connection], None]


def _normalize(text: str) -> str:
    """Lower-case + strip diacritics for accent-insensitive matching."""
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _migration_v2_routes(conn: sqlite3.Connection) -> None:
    """Phase 2: route history per contact, capped per contact at the
    application layer (DELETE-and-keep-N pattern)."""
    conn.execute(
        """
        CREATE TABLE routes_seen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_name TEXT NOT NULL,
            route TEXT NOT NULL,
            hops INTEGER NOT NULL,
            seen_at REAL NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX idx_routes_seen_contact_time "
        "ON routes_seen(contact_name, seen_at DESC)"
    )


def _migration_v1_adverts(conn: sqlite3.Connection) -> None:
    """Phase 1: tables for inbound advert history."""
    conn.execute(
        """
        CREATE TABLE adverts (
            pubkey TEXT PRIMARY KEY,
            name TEXT,
            first_seen REAL NOT NULL,
            last_seen REAL NOT NULL,
            last_adv_ts INTEGER,
            last_drift INTEGER,
            last_snr REAL,
            last_rssi INTEGER,
            last_path_len INTEGER,
            adv_type INTEGER,
            lat REAL,
            lon REAL
        )
        """
    )
    conn.execute("CREATE INDEX idx_adverts_last_seen ON adverts(last_seen DESC)")
    conn.execute(
        """
        CREATE TABLE adverts_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pubkey TEXT NOT NULL,
            seen_at REAL NOT NULL,
            adv_ts INTEGER,
            drift INTEGER,
            snr REAL,
            rssi INTEGER,
            path_len INTEGER,
            path TEXT,
            FOREIGN KEY (pubkey) REFERENCES adverts(pubkey) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX idx_adverts_history_pubkey_time "
        "ON adverts_history(pubkey, seen_at DESC)"
    )


# (version, callable) tuples applied in order. Each callable receives
# an open sqlite3.Connection inside an active transaction.
MIGRATIONS: list[tuple[int, Migration]] = [
    (1, _migration_v1_adverts),
    (2, _migration_v2_routes),
]


def import_adverts_from_json(state: "StateStore", data_dir: Path) -> int:
    """One-shot importer: read adverts_seen.json into the SQLite store
    if the table is empty and the legacy file is present. Renames the
    JSON to *.imported on success. Returns rows imported (0 if skipped)."""
    json_path = data_dir / "adverts_seen.json"
    if not json_path.exists():
        return 0
    cur = state.conn.cursor()
    cur.execute("SELECT count(*) FROM adverts")
    if cur.fetchone()[0] > 0:
        return 0
    try:
        data = json.loads(json_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to read %s: %s", json_path, e)
        return 0
    if not isinstance(data, dict) or not data:
        return 0

    cur.execute("BEGIN IMMEDIATE")
    try:
        n = 0
        for pubkey, info in data.items():
            if not isinstance(info, dict):
                continue
            last_seen = float(info.get("last_seen") or 0)
            first_seen = float(info.get("first_seen") or last_seen)
            cur.execute(
                """
                INSERT OR REPLACE INTO adverts (
                    pubkey, name, first_seen, last_seen, last_adv_ts,
                    last_drift, last_snr, last_rssi, last_path_len,
                    adv_type, lat, lon
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (pubkey, info.get("name"), first_seen, last_seen,
                 info.get("last_adv_ts"), info.get("last_drift"),
                 info.get("last_snr"), info.get("last_rssi"),
                 info.get("last_path_len"), info.get("adv_type"),
                 info.get("lat"), info.get("lon")),
            )
            cur.execute(
                """
                INSERT INTO adverts_history
                  (pubkey, seen_at, adv_ts, drift, snr, rssi, path_len, path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (pubkey, last_seen, info.get("last_adv_ts"),
                 info.get("last_drift"), info.get("last_snr"),
                 info.get("last_rssi"), info.get("last_path_len"), None),
            )
            n += 1
        state.conn.commit()
    except BaseException:
        state.conn.rollback()
        raise
    json_path.rename(json_path.with_suffix(json_path.suffix + ".imported"))
    logger.info("Imported %d advert records from %s", n, json_path.name)
    return n


def import_routes_from_json(state: "StateStore", data_dir: Path) -> int:
    """One-shot importer: read routes_seen.json into the SQLite store
    if the table is empty and the legacy file is present. Renames the
    JSON to *.imported on success. Returns rows imported (0 if skipped)."""
    json_path = data_dir / "routes_seen.json"
    if not json_path.exists():
        return 0
    cur = state.conn.cursor()
    cur.execute("SELECT count(*) FROM routes_seen")
    if cur.fetchone()[0] > 0:
        return 0
    try:
        data = json.loads(json_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to read %s: %s", json_path, e)
        return 0
    if not isinstance(data, dict) or not data:
        return 0

    cur.execute("BEGIN IMMEDIATE")
    try:
        n = 0
        for contact_name, routes in data.items():
            if not isinstance(routes, list):
                continue
            for r in routes:
                if not isinstance(r, dict):
                    continue
                route = r.get("route")
                hops = r.get("hops")
                seen_at = r.get("time")
                if route is None or hops is None or seen_at is None:
                    continue
                cur.execute(
                    "INSERT INTO routes_seen "
                    "(contact_name, route, hops, seen_at) VALUES (?, ?, ?, ?)",
                    (contact_name, route, int(hops), float(seen_at)),
                )
                n += 1
        state.conn.commit()
    except BaseException:
        state.conn.rollback()
        raise
    json_path.rename(json_path.with_suffix(json_path.suffix + ".imported"))
    logger.info("Imported %d route records from %s", n, json_path.name)
    return n


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

    # ---------------- adverts ----------------

    def record_advert(
        self,
        *,
        pubkey: str,
        name: str | None,
        recv_at: float,
        adv_ts: int | None,
        drift: int | None,
        snr: float | None,
        rssi: int | None,
        path_len: int | None,
        adv_type: int | None,
        lat: float | None,
        lon: float | None,
        path: str | None = None,
        history_max: int = 20,
    ) -> None:
        """UPSERT the latest advert row for pubkey, append to history,
        and trim history to the most recent ``history_max`` rows for
        that pubkey. All in one transaction."""
        cur = self._conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            cur.execute(
                """
                INSERT INTO adverts (
                    pubkey, name, first_seen, last_seen, last_adv_ts,
                    last_drift, last_snr, last_rssi, last_path_len,
                    adv_type, lat, lon
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pubkey) DO UPDATE SET
                    name = excluded.name,
                    last_seen = excluded.last_seen,
                    last_adv_ts = excluded.last_adv_ts,
                    last_drift = excluded.last_drift,
                    last_snr = excluded.last_snr,
                    last_rssi = excluded.last_rssi,
                    last_path_len = excluded.last_path_len,
                    adv_type = excluded.adv_type,
                    lat = excluded.lat,
                    lon = excluded.lon
                """,
                (pubkey, name, recv_at, recv_at, adv_ts, drift, snr, rssi,
                 path_len, adv_type, lat, lon),
            )
            cur.execute(
                """
                INSERT INTO adverts_history
                  (pubkey, seen_at, adv_ts, drift, snr, rssi, path_len, path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (pubkey, recv_at, adv_ts, drift, snr, rssi, path_len, path),
            )
            cur.execute(
                """
                DELETE FROM adverts_history
                WHERE pubkey = ?
                  AND id NOT IN (
                    SELECT id FROM adverts_history
                    WHERE pubkey = ?
                    ORDER BY seen_at DESC, id DESC
                    LIMIT ?
                  )
                """,
                (pubkey, pubkey, history_max),
            )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

    def get_advert_record(self, pubkey: str) -> dict[str, Any] | None:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM adverts WHERE pubkey = ?", (pubkey,))
        row = cur.fetchone()
        return dict(zip([d[0] for d in cur.description], row)) if row else None

    def iter_adverts(
        self, *, since: float = 0, repeater_only: bool = False
    ) -> Iterator[dict[str, Any]]:
        """Stream advert rows newer than ``since`` (last_seen >= since).
        Optionally restrict to repeater nodes (adv_type == 2)."""
        cur = self._conn.cursor()
        if repeater_only:
            cur.execute(
                "SELECT * FROM adverts WHERE last_seen >= ? AND adv_type = 2",
                (since,),
            )
        else:
            cur.execute("SELECT * FROM adverts WHERE last_seen >= ?", (since,))
        cols = [d[0] for d in cur.description]
        for row in cur:
            yield dict(zip(cols, row))

    def get_recent_adverts(
        self, name_filter: str = "", limit: int = 10
    ) -> list[dict[str, Any]]:
        """Latest adverts overall, newest first; optional case- and
        accent-insensitive name substring filter."""
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM adverts ORDER BY last_seen DESC")
        cols = [d[0] for d in cur.description]
        results: list[dict[str, Any]] = []
        norm = _normalize(name_filter) if name_filter else ""
        for row in cur:
            d = dict(zip(cols, row))
            if norm and norm not in _normalize(d.get("name") or ""):
                continue
            results.append(d)
            if len(results) >= limit:
                break
        return results

    def compute_clock_drift_stats(
        self, window_hours: float = 48
    ) -> dict[str, Any]:
        """Aggregate drift across nodes heard in the last ``window_hours``.

        Returns a dict with sample size, signed median, share within
        ±30s/±5m/±1h, share over 1d/30d/1y, and the worst offender.
        """
        cutoff = time.time() - window_hours * 3600
        cur = self._conn.cursor()
        cur.execute(
            "SELECT name, last_drift FROM adverts "
            "WHERE last_seen >= ? AND last_drift IS NOT NULL",
            (cutoff,),
        )
        drifts = [(int(d), name or "?") for name, d in cur.fetchall()]
        if not drifts:
            return {"window_hours": int(window_hours), "count": 0}

        n = len(drifts)
        abs_d = [abs(d) for d, _ in drifts]
        signed = sorted(d for d, _ in drifts)
        median = (
            signed[n // 2] if n % 2
            else (signed[n // 2 - 1] + signed[n // 2]) // 2
        )
        worst = max(drifts, key=lambda x: abs(x[0]))

        def pct(threshold: int, op: str = "le") -> int:
            if op == "le":
                k = sum(1 for a in abs_d if a <= threshold)
            else:
                k = sum(1 for a in abs_d if a > threshold)
            return round(100 * k / n)

        return {
            "window_hours": int(window_hours),
            "count": n,
            "median_seconds": median,
            "within_30s_pct": pct(30),
            "within_5m_pct": pct(300),
            "within_1h_pct": pct(3600),
            "over_1d_pct": pct(86400, "gt"),
            "over_30d_pct": pct(30 * 86400, "gt"),
            "over_1y_pct": pct(365 * 86400, "gt"),
            "worst_drift_seconds": worst[0],
            "worst_name": worst[1],
        }

    # ---------------- routes_seen ----------------

    def record_route(
        self,
        *,
        contact_name: str,
        route: str,
        hops: int,
        seen_at: float,
        history_max: int = 20,
    ) -> None:
        """Insert a route observation for a contact, capped at the most
        recent ``history_max`` rows. Consecutive duplicates of the same
        route just bump the timestamp on the existing row instead of
        adding a new one (keeps the table tidy when a node keeps using
        the same path)."""
        cur = self._conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            cur.execute(
                "SELECT id, route FROM routes_seen "
                "WHERE contact_name = ? "
                "ORDER BY seen_at DESC, id DESC LIMIT 1",
                (contact_name,),
            )
            row = cur.fetchone()
            if row and row[1] == route:
                cur.execute(
                    "UPDATE routes_seen SET seen_at = ? WHERE id = ?",
                    (seen_at, row[0]),
                )
            else:
                cur.execute(
                    "INSERT INTO routes_seen (contact_name, route, hops, seen_at) "
                    "VALUES (?, ?, ?, ?)",
                    (contact_name, route, hops, seen_at),
                )
                cur.execute(
                    """
                    DELETE FROM routes_seen
                    WHERE contact_name = ?
                      AND id NOT IN (
                        SELECT id FROM routes_seen
                        WHERE contact_name = ?
                        ORDER BY seen_at DESC, id DESC
                        LIMIT ?
                      )
                    """,
                    (contact_name, contact_name, history_max),
                )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

    def get_recent_routes(
        self, contact_name: str, limit: int = 3
    ) -> list[str]:
        """Return up to ``limit`` most-recent route strings for a contact,
        newest first."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT route FROM routes_seen WHERE contact_name = ? "
            "ORDER BY seen_at DESC, id DESC LIMIT ?",
            (contact_name, limit),
        )
        return [r[0] for r in cur.fetchall()]

    def routes_by_name_pattern(
        self, name_pattern: str, since: float
    ) -> dict[str, list[dict[str, Any]]]:
        """Group recent (>= since) routes by contact_name for every
        contact whose name matches ``name_pattern`` as an accent- and
        case-insensitive substring."""
        norm = _normalize(name_pattern)
        cur = self._conn.cursor()
        cur.execute(
            "SELECT contact_name, route, hops, seen_at FROM routes_seen "
            "WHERE seen_at >= ? "
            "ORDER BY contact_name, seen_at DESC, id DESC",
            (since,),
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for cname, route, hops, seen_at in cur:
            if norm and norm not in _normalize(cname):
                continue
            grouped.setdefault(cname, []).append(
                {"route": route, "hops": hops, "time": seen_at}
            )
        return grouped

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
