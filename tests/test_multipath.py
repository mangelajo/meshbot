"""Tests for multipath route collection and formatting."""

import tempfile

from meshbot.bot.commands import format_multipath
from meshbot.bot.mesh import MeshConnection
from meshbot.models import BotConfig, MeshMessage


def _make_msg(text: str = "multipath", sender_ts: int = 1000) -> MeshMessage:
    return MeshMessage(
        text=text, sender="TestUser", channel_idx=0,
        path_len=0, sender_timestamp=sender_ts,
    )


def _make_msg_with_path(
    path: str, path_len: int, hash_size: int = 1, sender_ts: int = 1000
) -> MeshMessage:
    return MeshMessage(
        text="multipath", sender="TestUser", channel_idx=0,
        path_len=path_len, sender_timestamp=sender_ts,
        path=path, path_hash_size=hash_size,
    )


def test_multipath_add_entry_collects_and_dedups():
    """_multipath_add_entry stores routes and skips duplicates."""
    config = BotConfig()
    conn = MeshConnection(config, data_dir=tempfile.mkdtemp())

    msg = _make_msg_with_path("", 0)
    msg_id = conn._msg_id(msg)

    conn._multipath_add_entry(msg_id, "", 0, 1, None)
    conn._multipath_add_entry(msg_id, "ed", 1, 1, -3.0)
    conn._multipath_add_entry(msg_id, "d2ed", 2, 1, -7.0)
    conn._multipath_add_entry(msg_id, "ed", 1, 1, -3.0)  # duplicate

    routes = conn.get_multipath(msg)
    assert len(routes) == 3
    assert routes[0]["is_direct"]
    assert routes[1]["path"] == "ed"
    assert routes[2]["path"] == "d2ed"


def test_is_duplicate_no_longer_records_paths():
    """_is_duplicate detects repeats but does not touch the multipath
    bucket: paths are recorded exclusively from RX_LOG_DATA, never from
    the unreliable CONTACT_MSG_RECV/CHANNEL_MSG_RECV header."""
    config = BotConfig()
    conn = MeshConnection(config, data_dir=tempfile.mkdtemp())

    msg = _make_msg_with_path("ed", 1)

    assert not conn._is_duplicate(msg)
    assert conn._is_duplicate(msg)
    assert conn.get_multipath(msg) == []


def test_format_multipath_single_direct():
    """Format with only direct route."""
    routes = [{"path": "", "path_len": 0, "path_hash_size": 1, "is_direct": True, "snr": None}]
    result = format_multipath("Alice", routes, 127)
    assert "1 routes" in result
    assert "direct" in result


def test_format_multipath_multiple_routes():
    """Format with multiple different routes."""
    routes = [
        {"path": "", "path_len": 0, "path_hash_size": 1, "is_direct": True, "snr": None},
        {"path": "ed", "path_len": 1, "path_hash_size": 1, "is_direct": False, "snr": None},
        {"path": "d2ed", "path_len": 2, "path_hash_size": 1, "is_direct": False, "snr": None},
    ]
    result = format_multipath("Alice", routes, 200)
    assert "3 routes" in result
    assert "direct" in result
    assert "ed" in result
    assert "d2->ed" in result


def test_format_multipath_deduplicates():
    """Duplicate routes are shown only once."""
    routes = [
        {"path": "ed", "path_len": 1, "path_hash_size": 1, "is_direct": False, "snr": None},
        {"path": "ed", "path_len": 1, "path_hash_size": 1, "is_direct": False, "snr": None},
        {"path": "d2", "path_len": 1, "path_hash_size": 1, "is_direct": False, "snr": None},
    ]
    result = format_multipath("Alice", routes, 200)
    assert "2 routes" in result
    assert result.count("ed") == 1  # not duplicated


def test_format_multipath_splits_when_long():
    """Falls back to multiline when result exceeds max_length."""
    routes = [
        {"path": "aabbccdd", "path_len": 2, "path_hash_size": 2, "is_direct": False, "snr": None, "time": 0},
        {"path": "eeff0011", "path_len": 2, "path_hash_size": 2, "is_direct": False, "snr": None, "time": 0},
        {"path": "22334455", "path_len": 2, "path_hash_size": 2, "is_direct": False, "snr": None, "time": 0},
    ]
    result = format_multipath("VeryLongSenderName 🎉🎊", routes, 40)
    lines = result.split("\n")
    assert len(lines) == 4  # header + 3 routes
    assert "3 routes" in lines[0]
    assert "aabb->ccdd" in lines[1]
    assert "eeff->0011" in lines[2]


def test_format_multipath_2byte_prefixes():
    """Correctly splits 2-byte hash size prefixes."""
    routes = [
        {"path": "d259ed97", "path_len": 2, "path_hash_size": 2, "is_direct": False, "snr": None},
        {"path": "cebaed97", "path_len": 2, "path_hash_size": 2, "is_direct": False, "snr": None},
    ]
    result = format_multipath("Alice", routes, 200)
    assert "d259->ed97" in result
    assert "ceba->ed97" in result


def test_get_multipath_empty():
    """get_multipath returns empty list for unknown message."""
    config = BotConfig()
    conn = MeshConnection(config, data_dir=tempfile.mkdtemp())
    msg = _make_msg(sender_ts=9999)
    assert conn.get_multipath(msg) == []
