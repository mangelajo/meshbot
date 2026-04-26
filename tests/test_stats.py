"""Tests for route statistics."""

from meshbot.bot.stats import RouteStats
from meshbot.models import MeshMessage


def _make_stats() -> RouteStats:
    """Create a RouteStats with clean state."""
    stats = RouteStats()
    stats.repeater_counts.clear()
    stats.route_type_counts.clear()
    stats.total_routes = 0
    return stats


def _make_msg(path: str, path_len: int, hash_size: int = 1) -> MeshMessage:
    return MeshMessage(
        text="hi", sender="Test", channel_idx=0,
        path_len=path_len, sender_timestamp=1000,
        path=path, path_hash_size=hash_size,
    )


def test_record_counts_first_hop_only():
    """Only the first hop (entry repeater) in each path is counted."""
    stats = _make_stats()
    stats.record(_make_msg("edd2", 2))
    stats.record(_make_msg("ed", 1))

    assert stats.repeater_counts["ed"] == 2
    assert "d2" not in stats.repeater_counts
    assert stats.total_routes == 2


def test_record_route_types():
    """Route types are counted by hash size."""
    stats = _make_stats()
    stats.record(_make_msg("ed", 1, hash_size=1))
    stats.record(_make_msg("d259ed97", 2, hash_size=2))
    stats.record(_make_msg("d259ed97", 2, hash_size=2))

    assert stats.route_type_counts["1-byte"] == 1
    assert stats.route_type_counts["2-byte"] == 2


def test_record_skips_direct():
    """Direct messages (path_len=0) are not recorded."""
    stats = _make_stats()
    stats.record(_make_msg("", 0))

    assert stats.total_routes == 0


def test_record_skips_no_path():
    """Messages with path_len>0 but no path string are skipped."""
    stats = _make_stats()
    stats.record(_make_msg("", 2))

    assert stats.total_routes == 0


def test_get_top_repeaters():
    """Top repeaters are returned in order of frequency."""
    stats = _make_stats()
    for _ in range(5):
        stats.record(_make_msg("ed", 1))
    for _ in range(3):
        stats.record(_make_msg("d2", 1))
    stats.record(_make_msg("ab", 1))

    top = stats.get_top_repeaters(2)
    assert len(top) == 2
    assert top[0]["prefix"] == "ed"
    assert top[0]["count"] == 5
    assert top[1]["prefix"] == "d2"
    assert top[1]["count"] == 3


def test_get_route_types():
    """Route type distribution is returned correctly."""
    stats = _make_stats()
    stats.record(_make_msg("ed", 1, hash_size=1))
    stats.record(_make_msg("d259", 1, hash_size=2))

    result = stats.get_route_types()
    assert result["total_routes"] == 2
    assert result["types"]["1-byte"] == 1
    assert result["types"]["2-byte"] == 1


def test_get_route_types_empty():
    """Empty stats return zero totals."""
    stats = _make_stats()
    result = stats.get_route_types()
    assert result["total_routes"] == 0
    assert result["types"] == {}


def test_2byte_prefixes_counted():
    """First-hop 2-byte prefix is counted; later hops are not."""
    stats = _make_stats()
    stats.record(_make_msg("d259ed97", 2, hash_size=2))

    assert stats.repeater_counts["d259"] == 1
    assert "ed97" not in stats.repeater_counts
