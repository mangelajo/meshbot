"""FastMCP server wrapping meshcore + bot features for Claude Code / MCP clients."""

import sys
from collections import deque
from collections.abc import AsyncIterator
from typing import Any

import meshcore  # type: ignore[import-untyped]
from fastmcp import Context, FastMCP
from fastmcp.server.lifespan import lifespan
from meshcore.events import EventType  # type: ignore[import-untyped]

from meshbot.bot.message_store import MessageStore
from meshbot.bot.pollen import fetch_pollen_data
from meshbot.bot.state_store import DB_FILENAME, StateStore
from meshbot.models import MeshMessage

# Parse serial port and baudrate from sys.argv
_serial_port: str | None = None
_baudrate: int = 115200
_debug: bool = False

for i, arg in enumerate(sys.argv):
    if arg in ("--serial-port", "-p") and i + 1 < len(sys.argv):
        _serial_port = sys.argv[i + 1]
    elif arg in ("--baudrate", "-b") and i + 1 < len(sys.argv):
        _baudrate = int(sys.argv[i + 1])
    elif arg in ("--debug", "-d"):
        _debug = True

_message_buffer: deque[dict[str, Any]] = deque(maxlen=1000)
_message_store = MessageStore(db_path=DB_FILENAME)
_state = StateStore(DB_FILENAME)


@lifespan
async def mesh_lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:  # type: ignore[type-arg]
    """Connect to mesh device and start auto message fetching."""
    if _serial_port is None:
        raise RuntimeError("Serial port not specified. Use -p/--serial-port.")

    mc = await meshcore.MeshCore.create_serial(_serial_port, _baudrate, _debug)
    await mc.start_auto_message_fetching()

    async def on_channel_message(event: Any) -> None:
        payload = event.payload
        _message_buffer.append(payload)
        msg = MeshMessage.from_channel_payload(payload)
        _message_store.store(msg, f"ch{msg.channel_idx}")
        _state.record_path(msg.path, msg.path_len, msg.path_hash_size)

    subscription = mc.subscribe(EventType.CHANNEL_MSG_RECV, on_channel_message)

    yield {"mc": mc, "subscription": subscription}

    subscription.unsubscribe()
    await mc.stop_auto_message_fetching()
    await mc.disconnect()
    _message_store.close()


mcp = FastMCP("meshbot", lifespan=mesh_lifespan)


def _get_mc(ctx: Context) -> Any:
    """Get the MeshCore instance from the request context."""
    rc = ctx.request_context
    assert rc is not None
    return rc.lifespan_context["mc"]


# --- Meshcore tools ---


@mcp.tool
async def poll_messages(channel_idx: int | None = None) -> list[dict[str, Any]]:
    """Drain the message buffer and return all pending messages.

    Args:
        channel_idx: If set, only return messages from this channel index.
    """
    messages: list[dict[str, Any]] = []
    while _message_buffer:
        msg = _message_buffer.popleft()
        if channel_idx is not None and msg.get("channel_idx") != channel_idx:
            continue
        messages.append(msg)
    return messages


@mcp.tool
async def send_channel_message(ctx: Context, channel_idx: int, text: str) -> str:
    """Send a text message to a mesh channel.

    Args:
        channel_idx: Channel index to send to.
        text: Message text to send.
    """
    mc = _get_mc(ctx)
    result = await mc.commands.send_chan_msg(channel_idx, text)
    if result.type == EventType.ERROR:
        return f"Error: {result.payload}"
    return "ok"


@mcp.tool
async def get_contacts(ctx: Context) -> list[dict[str, Any]]:
    """List all known contacts from the mesh device."""
    mc = _get_mc(ctx)
    await mc.ensure_contacts()
    return list(mc.contacts.values())


@mcp.tool
async def get_repeaters(ctx: Context) -> list[dict[str, Any]]:
    """List all repeater nodes from the contact list."""
    mc = _get_mc(ctx)
    await mc.ensure_contacts()
    return [c for c in mc.contacts.values() if c.get("type") == 2]


@mcp.tool
async def get_status(ctx: Context) -> dict[str, Any]:
    """Get mesh device connection status, node count, and self info."""
    mc = _get_mc(ctx)
    await mc.ensure_contacts()
    return {
        "connected": mc.is_connected,
        "self_info": mc.self_info,
        "contact_count": len(mc.contacts),
        "buffered_messages": len(_message_buffer),
    }


# --- Node lookup tools ---


@mcp.tool
async def get_node_by_prefix(ctx: Context, prefix: str) -> dict[str, Any] | None:
    """Look up a node by its public key prefix.

    Args:
        prefix: Hex string prefix of the node's public key.
    """
    mc = _get_mc(ctx)
    await mc.ensure_contacts()
    result: dict[str, Any] | None = mc.get_contact_by_key_prefix(prefix)
    return result


@mcp.tool
async def resolve_prefixes(ctx: Context, prefixes: str) -> list[dict[str, Any]]:
    """Resolve one or more hex prefixes to node names and info.

    Args:
        prefixes: Comma-separated hex prefixes (e.g. "d2,ed97,ceba").
    """
    mc = _get_mc(ctx)
    await mc.ensure_contacts()
    results = []
    for prefix in (p.strip() for p in prefixes.split(",") if p.strip()):
        node = mc.get_contact_by_key_prefix(prefix)
        if node:
            results.append({
                "prefix": prefix,
                "name": node.get("adv_name", ""),
                "type": node.get("type"),
                "hops": node.get("out_path_len"),
            })
        else:
            results.append({"prefix": prefix, "name": None})
    return results


@mcp.tool
async def search_contacts(ctx: Context, name: str) -> list[dict[str, Any]]:
    """Search for mesh contacts by name (case-insensitive substring match).

    Args:
        name: Name or partial name to search for.
    """
    mc = _get_mc(ctx)
    await mc.ensure_contacts()
    pattern = name.lower()
    results = []
    for contact in mc.contacts.values():
        cname = contact.get("adv_name", "")
        if cname and pattern in cname.lower():
            results.append({
                "name": cname,
                "public_key": contact.get("public_key", "")[:12],
                "type": contact.get("type"),
                "hops": contact.get("out_path_len"),
                "last_advert": contact.get("last_advert", 0),
            })
    return results


# --- Statistics tools ---


@mcp.tool
async def get_top_repeaters(ctx: Context, limit: int = 10) -> list[dict[str, Any]]:
    """Get the most frequently seen repeater prefixes with resolved names.

    Args:
        limit: Max number of repeaters to return (default 10).
    """
    mc = _get_mc(ctx)
    await mc.ensure_contacts()
    top = _state.get_top_repeaters_raw(limit)
    for entry in top:
        node = mc.get_contact_by_key_prefix(entry["prefix"])
        entry["name"] = node.get("adv_name", entry["prefix"]) if node else entry["prefix"]
    return top


@mcp.tool
async def get_route_type_stats() -> dict[str, Any]:
    """Get route type distribution statistics (1-byte, 2-byte, etc)."""
    return _state.get_route_types()


# --- Message search tools ---


@mcp.tool
async def search_messages(query: str, limit: int = 10) -> list[dict[str, Any]]:
    """Search stored channel messages by keyword using full-text search.

    Args:
        query: Keywords to search for.
        limit: Max results to return (default 10).
    """
    return _message_store.search(query, limit=limit)


@mcp.tool
async def search_messages_by_sender(sender: str, limit: int = 10) -> list[dict[str, Any]]:
    """Search stored messages from a specific sender.

    Args:
        sender: Name or partial name of the sender.
        limit: Max results to return (default 10).
    """
    return _message_store.search_by_sender(sender, limit=limit)


@mcp.tool
async def get_message_stats() -> dict[str, Any]:
    """Get message storage statistics: total messages, per-channel counts, date range."""
    return _message_store.get_stats()


# --- Pollen tool ---


@mcp.tool
async def get_pollen_levels() -> str:
    """Fetch current pollen levels for Madrid from Clinica Subiza."""
    return await fetch_pollen_data()


# --- Traceroute tool ---


@mcp.tool
async def traceroute(ctx: Context, path: str, timeout: float = 30) -> dict[str, Any]:
    """Trace a route through the mesh and measure SNR at each hop (round-trip).

    Accepts outbound path (closest to farthest). Return path is auto-calculated.
    e.g. path "ed,d2,df" traces round-trip ed->d2->df->d2->ed.

    Args:
        path: Outbound route, comma-separated hex prefixes (e.g. "ed,d2,df").
        timeout: Max seconds to wait for trace response (default 30).
    """
    mc = _get_mc(ctx)
    # Normalize and reverse (route history stores farthest→closest)
    normalized = path.replace("->", ",").replace(" ", "")
    hops_raw = [h.strip() for h in normalized.split(",") if h.strip()]
    if not hops_raw:
        return {"outbound": [], "return": [], "error": "empty path"}

    hops = list(reversed(hops_raw))
    roundtrip = hops + list(reversed(hops))[1:]
    roundtrip_str = ",".join(roundtrip)

    import random

    tag = random.randint(1, 0xFFFFFFFF)
    result = await mc.commands.send_trace(path=roundtrip_str, tag=tag)
    if result.type == EventType.ERROR:
        return {"outbound": [], "return": [], "error": str(result.payload)}
    trace_event = await mc.wait_for_event(
        EventType.TRACE_DATA,
        attribute_filters={"tag": tag},
        timeout=timeout,
    )
    if trace_event is None:
        return {"outbound": [], "return": [], "error": "timeout"}

    trace_path = trace_event.payload.get("path", [])
    n_out = len(hops)

    await mc.ensure_contacts()
    outbound: list[dict[str, Any]] = []
    return_leg: list[dict[str, Any]] = []
    for i, hop in enumerate(trace_path):
        prefix = hop.get("hash", "")
        name = prefix
        if prefix:
            node = mc.get_contact_by_key_prefix(prefix)
            if node:
                name = node.get("adv_name", prefix)
        entry = {"prefix": prefix or "local", "name": name or "local", "snr": hop.get("snr", 0)}
        if i < n_out:
            outbound.append(entry)
        else:
            return_leg.append(entry)

    return {"outbound": outbound, "return": return_leg, "error": None}
