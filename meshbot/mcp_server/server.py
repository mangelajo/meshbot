"""FastMCP server wrapping meshcore for mesh radio interaction."""

import sys
from collections import deque
from collections.abc import AsyncIterator
from typing import Any

import meshcore  # type: ignore[import-untyped]
from fastmcp import Context, FastMCP
from fastmcp.server.lifespan import lifespan
from meshcore.events import EventType  # type: ignore[import-untyped]

# Parse serial port and baudrate from sys.argv so the MCP server can be
# launched as a subprocess with these args.
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

# Message buffer: incoming channel messages are stored here and drained by poll_messages
_message_buffer: deque[dict[str, Any]] = deque(maxlen=1000)


@lifespan
async def mesh_lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:  # type: ignore[type-arg]
    """Connect to mesh device and start auto message fetching."""
    if _serial_port is None:
        raise RuntimeError("Serial port not specified. Use -p/--serial-port.")

    mc = await meshcore.MeshCore.create_serial(_serial_port, _baudrate, _debug)
    await mc.start_auto_message_fetching()

    async def on_channel_message(event: Any) -> None:
        _message_buffer.append(event.payload)

    subscription = mc.subscribe(EventType.CHANNEL_MSG_RECV, on_channel_message)

    yield {"mc": mc, "subscription": subscription}

    subscription.unsubscribe()
    await mc.stop_auto_message_fetching()
    await mc.disconnect()


mcp = FastMCP("meshbot", lifespan=mesh_lifespan)


def _get_mc(ctx: Context) -> Any:
    """Get the MeshCore instance from the request context."""
    rc = ctx.request_context
    assert rc is not None
    return rc.lifespan_context["mc"]


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
async def get_repeaters(ctx: Context) -> list[dict[str, Any]]:
    """List all repeater nodes from the contact list."""
    mc = _get_mc(ctx)
    await mc.ensure_contacts()
    return [
        c for c in mc.contacts.values()
        if c.get("type") == 2  # REP type
    ]


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
async def get_contacts(ctx: Context) -> list[dict[str, Any]]:
    """List all known contacts from the mesh device."""
    mc = _get_mc(ctx)
    await mc.ensure_contacts()
    return list(mc.contacts.values())


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
