"""Tests for route statistics (StateStore-backed since Phase 3)."""

import tempfile
from pathlib import Path

from meshbot.bot.state_store import DB_FILENAME, StateStore


def _make_state() -> StateStore:
    """Create a StateStore against an isolated tempdir."""
    return StateStore(Path(tempfile.mkdtemp()) / DB_FILENAME)


def test_record_counts_first_hop_only():
    """Only the first hop (entry repeater) in each path is counted."""
    s = _make_state()
    s.record_path("edd2", 2, 1)
    s.record_path("ed", 1, 1)

    cur = s.conn.cursor()
    cur.execute("SELECT count FROM repeater_counts WHERE prefix='ed'")
    assert cur.fetchone()[0] == 2
    cur.execute("SELECT count(*) FROM repeater_counts WHERE prefix='d2'")
    assert cur.fetchone()[0] == 0
    assert s.get_total_routes() == 2


def test_record_route_types():
    """Route types are counted by hash size."""
    s = _make_state()
    s.record_path("ed", 1, 1)
    s.record_path("d259ed97", 2, 2)
    s.record_path("d259ed97", 2, 2)

    types = s.get_route_types()["types"]
    assert types["1-byte"] == 1
    assert types["2-byte"] == 2


def test_record_skips_direct():
    """path_len=0 is not recorded."""
    s = _make_state()
    s.record_path("", 0, 1)
    assert s.get_total_routes() == 0


def test_record_skips_no_path():
    """Empty path is not recorded even when path_len > 0."""
    s = _make_state()
    s.record_path("", 2, 1)
    assert s.get_total_routes() == 0


def test_record_path_first_hop_only():
    """record_path attributes only the first prefix and tallies totals."""
    s = _make_state()
    s.record_path("edd2ab", 3, 1)
    s.record_path("ed", 1, 1)
    s.record_path("d259ed97", 2, 2)

    cur = s.conn.cursor()
    cur.execute("SELECT prefix, count FROM repeater_counts ORDER BY prefix")
    counts = dict(cur.fetchall())
    assert counts == {"ed": 2, "d259": 1}
    assert s.get_total_routes() == 3


def test_record_path_skips_empty():
    """record_path ignores packets with no route info."""
    s = _make_state()
    s.record_path("", 0, 1)
    s.record_path("", 3, 1)
    assert s.get_total_routes() == 0


def test_get_top_repeaters_orders_by_count():
    """Top repeaters come back ordered by frequency, capped to limit."""
    s = _make_state()
    for _ in range(5):
        s.record_path("ed", 1, 1)
    for _ in range(3):
        s.record_path("d2", 1, 1)
    s.record_path("ab", 1, 1)

    top = s.get_top_repeaters_raw(2)
    assert len(top) == 2
    assert top[0] == {"prefix": "ed", "count": 5}
    assert top[1] == {"prefix": "d2", "count": 3}


def test_get_route_types():
    """Route type distribution is returned correctly."""
    s = _make_state()
    s.record_path("ed", 1, 1)
    s.record_path("d259", 1, 2)

    result = s.get_route_types()
    assert result["total_routes"] == 2
    assert result["types"]["1-byte"] == 1
    assert result["types"]["2-byte"] == 1


def test_get_route_types_empty():
    """Empty stats return zero totals."""
    s = _make_state()
    result = s.get_route_types()
    assert result["total_routes"] == 0
    assert result["types"] == {}


def test_2byte_prefixes_counted():
    """First-hop 2-byte prefix is counted; later hops are not."""
    s = _make_state()
    s.record_path("d259ed97", 2, 2)

    cur = s.conn.cursor()
    cur.execute("SELECT prefix FROM repeater_counts")
    prefixes = {r[0] for r in cur.fetchall()}
    assert prefixes == {"d259"}
