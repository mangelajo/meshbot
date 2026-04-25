"""Predefined command handlers for the mesh bot."""

import asyncio
import logging

from meshbot.bot.mesh import MeshConnection
from meshbot.models import BotConfig, MeshMessage, split_path_prefixes

logger = logging.getLogger("meshbot.commands")

# Command prefix character
CMD_PREFIX = "!"

# Known command names (used for matching without ! prefix after mention)
COMMAND_NAMES = {
    "ping", "help", "prefix", "path", "multipath", "stats", "estadisticas", "trace",
}


def is_command(text: str) -> bool:
    """Check if a message is a bot command."""
    return text.strip().startswith(CMD_PREFIX)


def parse_command(text: str) -> tuple[str, str]:
    """Parse a command message into (command_name, args).

    Returns lowercase command name and the remaining args string.
    """
    stripped = text.strip()
    if not stripped.startswith(CMD_PREFIX):
        return "", stripped

    parts = stripped[len(CMD_PREFIX) :].split(None, 1)
    cmd = parts[0].lower() if parts else ""
    args = parts[1] if len(parts) > 1 else ""
    return cmd, args


async def handle_command(
    cmd: str,
    args: str,
    message: MeshMessage,
    config: BotConfig,
    mesh: MeshConnection,
) -> str | None:
    """Dispatch a command and return the response text, or None if unknown."""
    handlers = {
        "ping": _cmd_ping,
        "help": _cmd_help,
        "prefix": _cmd_prefix,
        "path": _cmd_path,
        "multipath": _cmd_multipath,
        "stats": _cmd_stats,
        "estadisticas": _cmd_stats,
        "trace": _cmd_trace,
    }
    handler = handlers.get(cmd)
    if handler is None:
        return None
    logger.debug("Handling command: %s %s", cmd, args)
    return await handler(args, message, config, mesh)


async def _cmd_ping(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """Respond with pong."""
    return "pong"


async def _cmd_help(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """List available commands."""
    return (
        f"{CMD_PREFIX}ping {CMD_PREFIX}help "
        f"{CMD_PREFIX}prefix <XX> {CMD_PREFIX}path "
        f"{CMD_PREFIX}multipath {CMD_PREFIX}stats "
        f"{CMD_PREFIX}pollen. Or ask me anything!"
    )


async def _cmd_prefix(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """Look up a node by public key prefix."""
    prefix = args.strip()
    if not prefix:
        return f"Usage: {CMD_PREFIX}prefix <hex_prefix>"

    result = await mesh.get_node_by_prefix(prefix)
    if result is None:
        return f"No node found for prefix {prefix}"

    name = result.get("adv_name", "unknown")
    key = result.get("public_key", "")[:12]
    hops = result.get("out_path_len", "?")
    return f"{name} ({key}...) hops={hops}"


async def _cmd_path(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """Report the path of the received message.

    Tries detailed format (with node names) first. Falls back to short
    format (prefix->prefix) if detail doesn't fit in max_length.
    """
    hops = message.path_len
    sender = message.sender or "unknown"
    if hops == 0:
        return f"{sender}: direct (0 hops)"

    if not message.path:
        return f"{sender}: {hops} hop{'s' if hops != 1 else ''} (path unknown)"

    prefixes = split_path_prefixes(message.path, message.path_hash_size)
    max_len = config.message.max_length

    # Try detailed format first: resolve each prefix to a name
    names: list[str] = []
    for prefix in prefixes:
        node = await mesh.get_node_by_prefix(prefix)
        name = node.get("adv_name", "?") if node else "?"
        names.append(f"{prefix}({name})")

    detail = f"{sender}: " + "->".join(names) + f" ({hops} hops)"

    if len(detail) <= max_len:
        return detail

    # Fall back to short format
    chain = "->".join(prefixes)
    return f"{sender}: {chain} ({hops} hops)"


MULTIPATH_WAIT = 10  # seconds to wait for duplicate paths


async def _cmd_multipath(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """Wait for message copies via different routes and report all paths."""
    sender = message.sender or "unknown"
    wait = MULTIPATH_WAIT
    # Optional wait override: "multipath 5"
    if args.strip().isdigit():
        wait = min(int(args.strip()), 30)

    logger.info("Multipath: waiting %ds for routes...", wait)
    await asyncio.sleep(wait)

    routes = mesh.get_multipath(message)
    if not routes:
        return f"{sender}: no routes collected"

    return format_multipath(sender, routes, config.message.max_length, mesh)


def format_multipath(
    sender: str,
    routes: list[dict],
    max_length: int,
    mesh: MeshConnection | None = None,
) -> str:
    """Format collected multipath routes into a concise response."""
    if not routes:
        return f"{sender}: no routes"

    parts: list[str] = []
    for r in routes:
        if r["is_direct"]:
            parts.append("direct")
        elif r["path"]:
            prefixes = split_path_prefixes(r["path"], r["path_hash_size"])
            parts.append("->".join(prefixes))
        else:
            parts.append(f"{r['path_len']}h")

    # Deduplicate routes preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            unique.append(p)

    header = f"{sender} ({len(unique)} routes): "
    result = header + " | ".join(unique)

    if len(result) <= max_length:
        return result

    # Won't fit in one message — split into multiple lines (sent as separate messages)
    # First line: header with short routes, then overflow lines
    lines: list[str] = [header.rstrip()]
    for route in unique:
        lines.append(route)
    return "\n".join(lines)


async def _cmd_stats(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """Show route statistics with resolved repeater names."""
    stats = mesh.stats
    if stats.total_routes == 0:
        return "Sin rutas registradas"

    top = stats.get_top_repeaters(5)
    types = stats.get_route_types()
    max_len = config.message.max_length

    # Resolve names for top repeaters
    named: list[str] = []
    for r in top:
        node = await mesh.get_node_by_prefix(r["prefix"])
        name = node.get("adv_name", r["prefix"]) if node else r["prefix"]
        named.append(f"{name} x{r['count']}")

    type_str = ", ".join(f"{k}: {v}" for k, v in types["types"].items())
    header = f"{stats.total_routes} rutas ({type_str})"

    # Try single-line top
    top_line = "Top: " + ", ".join(named)
    if len(top_line) <= max_len:
        return f"{header}\n{top_line}"

    # Split repeaters into lines that fit
    lines = [header]
    current = "Top: "
    for n in named:
        candidate = f"{current}{n}, " if current != "Top: " else f"Top: {n}, "
        if len(candidate.rstrip(", ")) > max_len:
            lines.append(current.rstrip(", "))
            current = f"{n}, "
        else:
            current = candidate
    if current.rstrip(", "):
        lines.append(current.rstrip(", "))

    return "\n".join(lines)


async def _cmd_trace(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """Trace a route and report SNR at each hop (round-trip)."""
    path = args.strip()
    if not path:
        return f"Usage: {CMD_PREFIX}trace <prefixes> (e.g. trace ed,d2,df)"

    result = await mesh.traceroute(path)
    if result.get("error"):
        return f"Trace error: {result['error']}"

    return format_trace(result, config.message.max_length)


def format_trace(result: dict, max_length: int) -> str:
    """Format trace results into concise output."""
    outbound = result.get("outbound", [])
    return_leg = result.get("return", [])

    def _fmt_leg(hops: list[dict], label: str) -> str:
        parts = [f"{h['prefix']}:{h['snr']}" for h in hops]
        return f"{label}: " + "->".join(parts)

    ida = _fmt_leg(outbound, "Ida") if outbound else ""
    vuelta = _fmt_leg(return_leg, "Vuelta") if return_leg else ""

    single = f"{ida} | {vuelta}" if ida and vuelta else ida or vuelta
    if len(single) <= max_length:
        return single

    return f"{ida}\n{vuelta}" if ida and vuelta else ida or vuelta
