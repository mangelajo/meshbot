"""Tests for message storage with SQLite + FTS5."""

import time

from meshbot.bot.message_store import MessageStore
from meshbot.models import MeshMessage


def _make_store() -> MessageStore:
    """Create an in-memory MessageStore for testing."""
    return MessageStore(db_path=":memory:", max_age_days=30)


def _make_msg(
    sender: str = "Alice", text: str = "hello", channel_idx: int = 0, ts: int = 0
) -> MeshMessage:
    return MeshMessage(
        text=text, sender=sender, channel_idx=channel_idx,
        path_len=0, sender_timestamp=ts or int(time.time()),
    )


def test_store_and_search():
    """Store a message and find it by keyword."""
    store = _make_store()
    store.store(_make_msg(text="the antenna is broken"), "#test")

    results = store.search("antenna")
    assert len(results) == 1
    assert "antenna" in results[0]["text"]
    assert results[0]["channel"] == "#test"


def test_search_no_results():
    """Search for nonexistent keyword returns empty."""
    store = _make_store()
    store.store(_make_msg(text="hello world"), "#test")

    assert store.search("nonexistent") == []


def test_search_by_sender():
    """Find messages from a specific sender."""
    store = _make_store()
    store.store(_make_msg(sender="Santiago", text="msg 1"), "#ch")
    store.store(_make_msg(sender="Miguel", text="msg 2"), "#ch")
    store.store(_make_msg(sender="Santiago", text="msg 3"), "#ch")

    results = store.search_by_sender("Santiago")
    assert len(results) == 2
    assert all(r["sender"] == "Santiago" for r in results)


def test_search_by_sender_partial():
    """Sender search matches partial names."""
    store = _make_store()
    store.store(_make_msg(sender="Miguel EA4IPW 🧄", text="test"), "#ch")

    results = store.search_by_sender("Miguel")
    assert len(results) == 1


def test_search_empty_query():
    """Empty query returns empty."""
    store = _make_store()
    store.store(_make_msg(text="hello"), "#ch")

    assert store.search("") == []
    assert store.search("   ") == []


def test_fts_special_chars():
    """Special characters in query don't cause errors."""
    store = _make_store()
    store.store(_make_msg(text="hello world"), "#ch")

    # These should not raise
    store.search('test "quoted"')
    store.search("test*")
    store.search("AND OR NOT")


def test_get_stats():
    """Stats return correct counts."""
    store = _make_store()
    store.store(_make_msg(text="a"), "#public")
    store.store(_make_msg(text="b"), "#public")
    store.store(_make_msg(text="c"), "#bot")

    stats = store.get_stats()
    assert stats["total"] == 3
    assert stats["channels"]["#public"] == 2
    assert stats["channels"]["#bot"] == 1


def test_get_stats_empty():
    """Empty store returns zero stats."""
    store = _make_store()
    stats = store.get_stats()
    assert stats["total"] == 0


def test_store_skips_empty():
    """Empty messages are not stored."""
    store = _make_store()
    store.store(_make_msg(text=""), "#ch")
    store.store(_make_msg(text="   "), "#ch")

    assert store.get_stats()["total"] == 0


def test_pruning():
    """Old messages are pruned after enough inserts."""
    store = MessageStore(db_path=":memory:", max_age_days=1)

    # Insert an old message directly
    store._conn.execute(
        """INSERT INTO messages
           (sender, text, channel_name, timestamp, sender_timestamp, is_private, path_len)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("Old", "old msg", "#ch", time.time() - 86400 * 5, 0, 0, 0),
    )
    store._conn.commit()

    # Force prune by setting counter
    store._insert_count = 99
    store.store(_make_msg(text="new msg"), "#ch")

    stats = store.get_stats()
    assert stats["total"] == 1  # only the new one


def test_store_private_message():
    """Private messages are stored with DM channel."""
    store = _make_store()
    msg = MeshMessage(
        text="private hello", sender="Bob", channel_idx=-1,
        path_len=0, sender_timestamp=int(time.time()),
        is_private=True, pubkey_prefix="ab12cd",
    )
    store.store(msg, "DM")

    results = store.search("private")
    assert len(results) == 1
    assert results[0]["channel"] == "DM"


def test_multiple_channels():
    """Messages from different channels are stored and searchable."""
    store = _make_store()
    store.store(_make_msg(text="public chat"), "Public")
    store.store(_make_msg(text="bot chat"), "#b0b0t")

    all_results = store.search("chat")
    assert len(all_results) == 2

    stats = store.get_stats()
    assert "Public" in stats["channels"]
    assert "#b0b0t" in stats["channels"]
