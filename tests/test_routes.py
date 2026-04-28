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


def _routes_for(conn: MeshConnection, name: str) -> list[dict]:
    """Read raw rows from the routes_seen table for a contact, oldest first."""
    cur = conn.state.conn.cursor()
    cur.execute(
        "SELECT route, hops, seen_at FROM routes_seen "
        "WHERE contact_name = ? ORDER BY seen_at, id",
        (name,),
    )
    return [{"route": r[0], "hops": r[1], "time": r[2]} for r in cur.fetchall()]


def test_record_route_stores_path():
    """Routes are recorded with sender name."""
    conn = _make_conn()
    msg = _make_msg("Alice", path="ed", path_len=1)
    conn._record_route("Alice", msg)

    rows = _routes_for(conn, "Alice")
    assert len(rows) == 1
    assert rows[0]["route"] == "ed"
    assert rows[0]["hops"] == 1


def test_record_route_skips_direct():
    """Direct messages (path_len=0) are not recorded as routes."""
    conn = _make_conn()
    msg = _make_msg("Alice", path="", path_len=0)
    conn._record_route("Alice", msg)

    assert _routes_for(conn, "Alice") == []


def test_record_route_dedup_consecutive():
    """Same route consecutively only updates the time."""
    conn = _make_conn()
    conn._record_route("Alice", _make_msg("Alice", path="ed", path_len=1))
    conn._record_route("Alice", _make_msg("Alice", path="ed", path_len=1))

    assert len(_routes_for(conn, "Alice")) == 1


def test_record_route_different_routes():
    """Different routes are stored separately, newest last."""
    conn = _make_conn()
    conn._record_route("Alice", _make_msg("Alice", path="ed", path_len=1))
    conn._record_route("Alice", _make_msg("Alice", path="d2", path_len=1))
    conn._record_route(
        "Alice", _make_msg("Alice", path="edd2", path_len=2, hash_size=1)
    )

    rows = _routes_for(conn, "Alice")
    assert [r["route"] for r in rows] == ["ed", "d2", "ed->d2"]


def test_record_route_multibyte_prefix():
    """2-byte hash prefixes are split correctly in routes."""
    conn = _make_conn()
    conn._record_route(
        "Alice", _make_msg("Alice", path="d259ed97", path_len=2, hash_size=2)
    )

    rows = _routes_for(conn, "Alice")
    assert rows[0]["route"] == "d259->ed97"


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
    # Age the row directly via the SQL connection
    conn.state.conn.execute(
        "UPDATE routes_seen SET seen_at = ? WHERE contact_name = ?",
        (time.time() - 86400 * 10, "Alice"),
    )
    conn.state.conn.commit()

    results = await conn.get_contact_routes("Alice", max_age_days=7)
    assert len(results) == 0


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

    assert len(_routes_for(conn, "Alice")) <= 20
