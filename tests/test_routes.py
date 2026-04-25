"""Tests for route history tracking."""

import tempfile
import time

from meshbot.bot.mesh import MeshConnection
from meshbot.models import BotConfig, MeshMessage


def _make_conn() -> MeshConnection:
    """Create a MeshConnection with isolated temp directory."""
    tmpdir = tempfile.mkdtemp()
    return MeshConnection(BotConfig(), data_dir=tmpdir)


def _make_msg(
    sender: str, path: str = "", path_len: int = 0, hash_size: int = 1
) -> MeshMessage:
    return MeshMessage(
        text="hello", sender=sender, channel_idx=0,
        path_len=path_len, sender_timestamp=int(time.time()),
        path=path, path_hash_size=hash_size,
    )


def test_record_route_stores_path():
    """Routes are recorded with sender name."""
    conn = _make_conn()
    msg = _make_msg("Alice", path="ed", path_len=1)
    conn._record_route("Alice", msg)

    assert "Alice" in conn.routes_seen
    assert len(conn.routes_seen["Alice"]) == 1
    assert conn.routes_seen["Alice"][0]["route"] == "ed"
    assert conn.routes_seen["Alice"][0]["hops"] == 1


def test_record_route_skips_direct():
    """Direct messages (path_len=0) are not recorded as routes."""
    conn = _make_conn()
    msg = _make_msg("Alice", path="", path_len=0)
    conn._record_route("Alice", msg)

    assert "Alice" not in conn.routes_seen


def test_record_route_dedup_consecutive():
    """Same route consecutively only updates the time."""
    conn = _make_conn()
    msg1 = _make_msg("Alice", path="ed", path_len=1)
    msg2 = _make_msg("Alice", path="ed", path_len=1)
    conn._record_route("Alice", msg1)
    conn._record_route("Alice", msg2)

    assert len(conn.routes_seen["Alice"]) == 1


def test_record_route_different_routes():
    """Different routes are stored separately."""
    conn = _make_conn()
    conn._record_route("Alice", _make_msg("Alice", path="ed", path_len=1))
    conn._record_route("Alice", _make_msg("Alice", path="d2", path_len=1))
    conn._record_route("Alice", _make_msg("Alice", path="edd2", path_len=2, hash_size=1))

    assert len(conn.routes_seen["Alice"]) == 3
    assert conn.routes_seen["Alice"][0]["route"] == "ed"
    assert conn.routes_seen["Alice"][1]["route"] == "d2"
    assert conn.routes_seen["Alice"][2]["route"] == "ed->d2"


def test_record_route_multibyte_prefix():
    """2-byte hash prefixes are split correctly in routes."""
    conn = _make_conn()
    conn._record_route("Alice", _make_msg("Alice", path="d259ed97", path_len=2, hash_size=2))

    assert conn.routes_seen["Alice"][0]["route"] == "d259->ed97"


async def test_get_contact_routes_by_name():
    """get_contact_routes searches by partial name."""
    conn = _make_conn()
    conn._record_route("Santiago 🍅", _make_msg("Santiago 🍅", path="ed", path_len=1))
    conn._record_route("Santiago 🍅", _make_msg("Santiago 🍅", path="d2", path_len=1))
    conn._record_route("Miguel", _make_msg("Miguel", path="ab", path_len=1))

    results = await conn.get_contact_routes("Santiago")
    assert len(results) == 1
    assert results[0]["name"] == "Santiago 🍅"
    assert len(results[0]["routes"]) == 2


async def test_get_contact_routes_filters_old():
    """Routes older than max_age_days are filtered out."""
    conn = _make_conn()
    conn._record_route("Alice", _make_msg("Alice", path="ed", path_len=1))
    # Manually age the entry
    conn.routes_seen["Alice"][0]["time"] = time.time() - 86400 * 10

    results = await conn.get_contact_routes("Alice", max_age_days=7)
    assert len(results) == 0  # too old


async def test_get_contact_routes_empty():
    """Returns empty for unknown contacts."""
    conn = _make_conn()
    assert await conn.get_contact_routes("Nobody") == []


def test_max_routes_per_contact():
    """Routes are bounded per contact."""
    conn = _make_conn()
    for i in range(25):
        conn._record_route(
            "Alice", _make_msg("Alice", path=f"{i:02x}", path_len=1)
        )

    assert len(conn.routes_seen["Alice"]) <= 20
