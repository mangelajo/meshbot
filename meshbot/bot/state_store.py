"""Unified SQLite storage for meshbot state.

All persistent state lives in `meshbot.db` next to the bot. The
schema is created and evolved by numbered migrations so future
changes can be added incrementally — see `MIGRATIONS`. The
MessageStore in `message_store.py` shares the same DB file via its
own connection; SQLite WAL mode keeps the two writers consistent.
"""

import logging
import sqlite3
import time
import unicodedata
from pathlib import Path
from typing import Any, Callable, Iterator

from meshbot.models import split_path_prefixes

logger = logging.getLogger("meshbot.state")

DB_FILENAME = "meshbot.db"

Migration = Callable[[sqlite3.Connection], None]


def _normalize(text: str) -> str:
    """Lower-case + strip diacritics for accent-insensitive matching."""
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _apply_txdelay_compensation(
    advert: dict[str, Any], txdelay_per_hop: float
) -> None:
    """In-place: bump ``last_drift`` by ``last_path_len × txdelay_per_hop``
    to remove the relay-propagation bias from the displayed drift.

    The drift stored in the DB is `adv_ts - now_when_received`. By the
    time we receive the advert it has spent roughly path_len × txdelay
    seconds traversing relays, so the observed drift is biased low by
    that amount. Adding the bias back yields the source's real clock
    offset relative to ours. No-op when txdelay_per_hop <= 0 or
    last_drift is None.
    """
    if txdelay_per_hop <= 0:
        return
    drift = advert.get("last_drift")
    if drift is None:
        return
    path_len = advert.get("last_path_len") or 0
    if path_len <= 0:
        return
    advert["last_drift"] = int(drift) + int(round(path_len * txdelay_per_hop))


# ---------------- migrations ----------------


def _migration_v1_adverts(conn: sqlite3.Connection) -> None:
    """Per-pubkey advert state: latest values + bounded history."""
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


def _migration_v2_routes(conn: sqlite3.Connection) -> None:
    """Per-contact route history, capped per contact at the application
    layer (DELETE-and-keep-N pattern)."""
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


def _migration_v3_route_stats(conn: sqlite3.Connection) -> None:
    """Aggregate counters that drive !stats and the get_top_repeaters
    tool — repeater frequency, route-type histogram, total-routes."""
    conn.execute(
        "CREATE TABLE repeater_counts ("
        "prefix TEXT PRIMARY KEY, count INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE route_type_counts ("
        "label TEXT PRIMARY KEY, count INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE route_total ("
        "id INTEGER PRIMARY KEY CHECK (id=1), total INTEGER NOT NULL)"
    )
    conn.execute("INSERT INTO route_total (id, total) VALUES (1, 0)")


def _migration_v4_last_seen(conn: sqlite3.Connection) -> None:
    """Per-name last-seen row (sender of channel/DM messages)."""
    conn.execute(
        """
        CREATE TABLE last_seen (
            name TEXT PRIMARY KEY,
            seen_at REAL NOT NULL,
            channel TEXT
        )
        """
    )


def _migration_v5_messages_columns(conn: sqlite3.Connection) -> None:
    """Extend the existing messages table with pubkey_prefix (so we can
    identify DM peers) and direction ('in'/'out'). On a fresh DB the
    messages table doesn't exist yet — MessageStore creates it with
    the new columns directly, so this migration is a no-op there."""
    cur = conn.execute("PRAGMA table_info(messages)")
    cols = {row[1] for row in cur.fetchall()}
    if not cols:
        return
    if "pubkey_prefix" not in cols:
        conn.execute("ALTER TABLE messages ADD COLUMN pubkey_prefix TEXT")
    if "direction" not in cols:
        conn.execute(
            "ALTER TABLE messages ADD COLUMN direction TEXT NOT NULL DEFAULT 'in'"
        )


# (version, callable) tuples applied in order. Each callable receives
# an open sqlite3.Connection inside an active transaction.
MIGRATIONS: list[tuple[int, Migration]] = [
    (1, _migration_v1_adverts),
    (2, _migration_v2_routes),
    (3, _migration_v3_route_stats),
    (4, _migration_v4_last_seen),
    (5, _migration_v5_messages_columns),
]


class StateStore:
    """Owns the meshbot SQLite connection and applies pending migrations."""

    def __init__(
        self,
        db_path: str | Path,
        migrations: list[tuple[int, Migration]] | None = None,
    ) -> None:
        self._path = Path(db_path)
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
        self, *, since: float = 0, repeater_only: bool = False,
        txdelay_per_hop: float = 0.0,
    ) -> Iterator[dict[str, Any]]:
        """Stream advert rows newer than ``since`` (last_seen >= since).
        Optionally restrict to repeater nodes (adv_type == 2).

        ``txdelay_per_hop`` (seconds) is added to the returned
        ``last_drift`` of each row scaled by ``last_path_len`` to back
        out the propagation lag accumulated through relays. Set to 0
        to disable the compensation.
        """
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
            d = dict(zip(cols, row))
            _apply_txdelay_compensation(d, txdelay_per_hop)
            yield d

    def get_recent_adverts(
        self, name_filter: str = "", limit: int = 10,
        txdelay_per_hop: float = 0.0,
    ) -> list[dict[str, Any]]:
        """Latest adverts overall, newest first; optional case- and
        accent-insensitive name substring filter. ``txdelay_per_hop``
        applies the per-hop drift compensation, see iter_adverts."""
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM adverts ORDER BY last_seen DESC")
        cols = [d[0] for d in cur.description]
        results: list[dict[str, Any]] = []
        norm = _normalize(name_filter) if name_filter else ""
        for row in cur:
            d = dict(zip(cols, row))
            if norm and norm not in _normalize(d.get("name") or ""):
                continue
            _apply_txdelay_compensation(d, txdelay_per_hop)
            results.append(d)
            if len(results) >= limit:
                break
        return results

    def compute_clock_drift_stats(
        self, window_hours: float = 48, txdelay_per_hop: float = 0.0,
    ) -> dict[str, Any]:
        """Aggregate drift across nodes heard in the last ``window_hours``.

        ``txdelay_per_hop`` (seconds) compensates for relay propagation:
        each hop a flooded advert traverses adds roughly the relay's
        configured txdelay to its arrival time, so the drift we observe
        is actually ``real_drift - path_len × txdelay``. Adding back
        ``path_len × txdelay_per_hop`` cancels that bias when the
        constant is set close to the real average txdelay of the mesh.
        Set 0 to disable.
        """
        cutoff = time.time() - window_hours * 3600
        cur = self._conn.cursor()
        cur.execute(
            "SELECT name, last_drift, last_path_len FROM adverts "
            "WHERE last_seen >= ? AND last_drift IS NOT NULL",
            (cutoff,),
        )
        drifts = [
            (
                int(d) + int(round((p or 0) * txdelay_per_hop)),
                name or "?",
            )
            for name, d, p in cur.fetchall()
        ]
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
        adding a new one."""
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

    # ---------------- route stats ----------------

    def record_path(self, path: str, path_len: int, hash_size: int) -> None:
        """Bump the histograms for a packet that traversed `path`. The
        first prefix is attributed (since later hops are biased toward
        repeaters near our antenna)."""
        if path_len == 0 or not path:
            return
        label = f"{hash_size}-byte"
        prefixes = split_path_prefixes(path, hash_size)
        prefix = prefixes[0] if prefixes else None
        cur = self._conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            cur.execute("UPDATE route_total SET total = total + 1 WHERE id = 1")
            cur.execute(
                """
                INSERT INTO route_type_counts (label, count) VALUES (?, 1)
                ON CONFLICT(label) DO UPDATE SET count = count + 1
                """,
                (label,),
            )
            if prefix:
                cur.execute(
                    """
                    INSERT INTO repeater_counts (prefix, count) VALUES (?, 1)
                    ON CONFLICT(prefix) DO UPDATE SET count = count + 1
                    """,
                    (prefix,),
                )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

    def get_total_routes(self) -> int:
        cur = self._conn.cursor()
        cur.execute("SELECT total FROM route_total WHERE id = 1")
        row = cur.fetchone()
        return row[0] if row else 0

    def get_route_types(self) -> dict[str, Any]:
        cur = self._conn.cursor()
        cur.execute("SELECT label, count FROM route_type_counts")
        return {
            "total_routes": self.get_total_routes(),
            "types": {label: count for label, count in cur.fetchall()},
        }

    def iter_repeater_counts(self) -> Iterator[tuple[str, int]]:
        """Yield (prefix, count) pairs sorted by count desc."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT prefix, count FROM repeater_counts ORDER BY count DESC"
        )
        for row in cur:
            yield (row[0], row[1])

    def get_top_repeaters_raw(self, limit: int) -> list[dict[str, Any]]:
        """Top repeaters by raw prefix count, no grouping/filtering."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT prefix, count FROM repeater_counts "
            "ORDER BY count DESC LIMIT ?",
            (limit,),
        )
        return [{"prefix": p, "count": c} for p, c in cur.fetchall()]

    # ---------------- last_seen ----------------

    def record_seen(self, name: str, channel: str, seen_at: float) -> None:
        """UPSERT a last-seen row for a sender name."""
        if not name:
            return
        cur = self._conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            cur.execute(
                """
                INSERT INTO last_seen (name, seen_at, channel) VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    seen_at = excluded.seen_at,
                    channel = excluded.channel
                """,
                (name, seen_at, channel),
            )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

    def get_last_seen(self, name: str) -> dict[str, Any] | None:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT seen_at, channel FROM last_seen WHERE name = ?",
            (name,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"time": row[0], "channel": row[1]}

    # ---------------- migrations runner ----------------

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
