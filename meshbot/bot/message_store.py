"""Persistent message storage backed by SQLite with FTS5 full-text search."""

import logging
import sqlite3
import time
from typing import Any

from meshbot.models import MeshMessage

logger = logging.getLogger("meshbot.message_store")

DEFAULT_DB_PATH = "messages.db"


def _format_ago(ts: float) -> str:
    """Format a timestamp as 'X ago' relative to now."""
    delta = time.time() - ts
    if delta < 60:
        return "just now"
    elif delta < 3600:
        return f"{int(delta / 60)}m ago"
    elif delta < 86400:
        return f"{int(delta / 3600)}h ago"
    else:
        return f"{int(delta / 86400)}d ago"


class MessageStore:
    """SQLite-backed message store with FTS5 full-text search."""

    def __init__(
        self, db_path: str = DEFAULT_DB_PATH, max_age_days: int = 30
    ) -> None:
        self.db_path = db_path
        self.max_age_days = max_age_days
        self._insert_count = 0
        self._conn = self._connect()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_schema(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender TEXT NOT NULL,
                text TEXT NOT NULL,
                channel_name TEXT NOT NULL DEFAULT '',
                timestamp REAL NOT NULL,
                sender_timestamp INTEGER NOT NULL,
                is_private INTEGER NOT NULL DEFAULT 0,
                path_len INTEGER NOT NULL DEFAULT 0,
                pubkey_prefix TEXT,
                direction TEXT NOT NULL DEFAULT 'in'
            );

            CREATE INDEX IF NOT EXISTS idx_messages_timestamp
                ON messages(timestamp);
            CREATE INDEX IF NOT EXISTS idx_messages_sender
                ON messages(sender);
            CREATE INDEX IF NOT EXISTS idx_messages_channel
                ON messages(channel_name);
            CREATE INDEX IF NOT EXISTS idx_messages_pubkey
                ON messages(pubkey_prefix);
        """)
        # FTS5 virtual table — may not be available on all builds
        try:
            self._conn.executescript("""
                CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                    sender, text, channel_name,
                    content='messages', content_rowid='id'
                );

                CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
                    INSERT INTO messages_fts(rowid, sender, text, channel_name)
                    VALUES (new.id, new.sender, new.text, new.channel_name);
                END;

                CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
                    INSERT INTO messages_fts(messages_fts, rowid, sender, text, channel_name)
                    VALUES ('delete', old.id, old.sender, old.text, old.channel_name);
                END;
            """)
            self._has_fts = True
        except sqlite3.OperationalError:
            logger.warning("FTS5 not available, falling back to LIKE search")
            self._has_fts = False

        self._conn.commit()
        count = self._conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        if count:
            logger.info("Message store: %d messages in %s", count, self.db_path)

    def store(self, msg: MeshMessage, channel_name: str = "") -> None:
        """Store an inbound message in the database."""
        if not msg.text.strip():
            return
        self._conn.execute(
            """INSERT INTO messages
               (sender, text, channel_name, timestamp, sender_timestamp,
                is_private, path_len, pubkey_prefix, direction)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'in')""",
            (
                msg.sender,
                msg.text,
                channel_name,
                time.time(),
                msg.sender_timestamp,
                1 if msg.is_private else 0,
                msg.path_len,
                msg.pubkey_prefix or None,
            ),
        )
        self._conn.commit()
        self._insert_count += 1
        if self._insert_count % 100 == 0:
            self._prune()

    def record_outgoing(
        self,
        *,
        sender: str,
        text: str,
        channel_name: str,
        target_pubkey_prefix: str | None,
        is_private: bool,
    ) -> None:
        """Persist a message the bot just sent so DM history and
        full-text search both see both sides of the conversation."""
        if not text.strip():
            return
        self._conn.execute(
            """INSERT INTO messages
               (sender, text, channel_name, timestamp, sender_timestamp,
                is_private, path_len, pubkey_prefix, direction)
               VALUES (?, ?, ?, ?, 0, ?, 0, ?, 'out')""",
            (
                sender, text, channel_name, time.time(),
                1 if is_private else 0,
                target_pubkey_prefix or None,
            ),
        )
        self._conn.commit()

    def get_dm_history(
        self, pubkey_prefix: str, limit: int
    ) -> list[tuple[str, str]]:
        """Return up to ``limit`` most-recent (sender, text) entries from
        the DM thread with ``pubkey_prefix``, oldest first so the agent
        can read them as a chronological transcript."""
        rows = self._conn.execute(
            """SELECT sender, text FROM messages
               WHERE is_private = 1 AND pubkey_prefix = ?
               ORDER BY timestamp DESC LIMIT ?""",
            (pubkey_prefix, limit),
        ).fetchall()
        return [(r["sender"], r["text"]) for r in reversed(rows)]

    def search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Full-text search across all stored messages."""
        if not query.strip():
            return []

        if self._has_fts:
            # Sanitize for FTS5: wrap each word in quotes
            words = query.strip().split()
            fts_query = " ".join(f'"{w}"' for w in words)
            try:
                rows = self._conn.execute(
                    """SELECT m.sender, m.text, m.channel_name, m.timestamp
                       FROM messages m
                       JOIN messages_fts f ON m.id = f.rowid
                       WHERE messages_fts MATCH ?
                       ORDER BY m.timestamp DESC
                       LIMIT ?""",
                    (fts_query, limit),
                ).fetchall()
            except sqlite3.OperationalError:
                rows = self._fallback_search(query, limit)
        else:
            rows = self._fallback_search(query, limit)

        return [self._row_to_dict(r) for r in rows]

    def _fallback_search(
        self, query: str, limit: int
    ) -> list[sqlite3.Row]:
        """LIKE-based fallback when FTS5 is unavailable."""
        pattern = f"%{query}%"
        return self._conn.execute(
            """SELECT sender, text, channel_name, timestamp
               FROM messages
               WHERE text LIKE ? OR sender LIKE ?
               ORDER BY timestamp DESC
               LIMIT ?""",
            (pattern, pattern, limit),
        ).fetchall()

    def search_by_sender(self, sender: str, limit: int = 10) -> list[dict[str, Any]]:
        """Find messages from a specific sender (substring match)."""
        pattern = f"%{sender}%"
        rows = self._conn.execute(
            """SELECT sender, text, channel_name, timestamp
               FROM messages
               WHERE sender LIKE ?
               ORDER BY timestamp DESC
               LIMIT ?""",
            (pattern, limit),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_recent(
        self,
        channel: str | None = None,
        limit: int = 10,
        since: float | None = None,
        until: float | None = None,
    ) -> list[dict[str, Any]]:
        """Get the most recent messages, optionally filtered by channel
        and timestamp window.

        Args:
            channel: substring match against channel_name (None = all).
            limit: max rows to return.
            since: lower bound on timestamp (epoch seconds, inclusive).
            until: upper bound on timestamp (epoch seconds, exclusive).
        """
        clauses: list[str] = []
        params: list[Any] = []
        if channel:
            clauses.append("channel_name LIKE ?")
            params.append(f"%{channel}%")
        if since is not None:
            clauses.append("timestamp >= ?")
            params.append(since)
        if until is not None:
            clauses.append("timestamp < ?")
            params.append(until)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        rows = self._conn.execute(
            f"""SELECT sender, text, channel_name, timestamp
                FROM messages {where}
                ORDER BY timestamp DESC LIMIT ?""",
            params,
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_stats(self) -> dict[str, Any]:
        """Return message count per channel, total count, date range."""
        total = self._conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        if total == 0:
            return {"total": 0, "channels": {}, "oldest": None, "newest": None}

        channels: dict[str, int] = {}
        for row in self._conn.execute(
            "SELECT channel_name, COUNT(*) as cnt FROM messages GROUP BY channel_name"
        ):
            channels[row["channel_name"] or "unknown"] = row["cnt"]

        oldest = self._conn.execute(
            "SELECT MIN(timestamp) FROM messages"
        ).fetchone()[0]
        newest = self._conn.execute(
            "SELECT MAX(timestamp) FROM messages"
        ).fetchone()[0]

        return {
            "total": total,
            "channels": channels,
            "oldest": _format_ago(oldest) if oldest else None,
            "newest": _format_ago(newest) if newest else None,
        }

    def _row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "sender": row["sender"],
            "text": row["text"],
            "channel": row["channel_name"],
            "when": _format_ago(row["timestamp"]),
        }

    def _prune(self) -> None:
        """Delete messages older than max_age_days."""
        cutoff = time.time() - (self.max_age_days * 86400)
        deleted = self._conn.execute(
            "DELETE FROM messages WHERE timestamp < ?", (cutoff,)
        ).rowcount
        self._conn.commit()
        if deleted:
            logger.info("Pruned %d old messages", deleted)

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
