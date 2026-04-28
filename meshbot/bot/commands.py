"""Predefined command handlers for the mesh bot."""

import asyncio
import logging
import time
import unicodedata

from meshbot.bot.mesh import MeshConnection
from meshbot.bot.propagation import fetch_propagation
from meshbot.bot.weather import fetch_weather
from meshbot.models import BotConfig, MeshMessage, split_path_prefixes

logger = logging.getLogger("meshbot.commands")


def visual_width(s: str) -> int:
    """Return the visual width of a string, treating East Asian Wide and
    Fullwidth characters (including most emoji) as 2 columns and combining
    marks as 0."""
    w = 0
    for c in s:
        if unicodedata.combining(c):
            continue
        if unicodedata.east_asian_width(c) in ("W", "F"):
            w += 2
        else:
            w += 1
    return w


def truncate_visual(s: str, max_w: int) -> str:
    """Truncate s to a visual width <= max_w, suffixing ".." if truncated.

    If the string ends with a wide character (typically an emoji), the
    suffix is preserved so that names that differ only in their trailing
    emoji stay distinguishable after truncation.
    """
    if visual_width(s) <= max_w:
        return s

    # Preserve a single trailing wide char (emoji) when there's room.
    if s and visual_width(s[-1]) >= 2:
        suffix = s[-1]
        head = s[:-1].rstrip()
        head_target = max_w - visual_width(suffix) - 1  # space + suffix
        if head_target >= 4:
            return _truncate_plain(head, head_target) + " " + suffix
    return _truncate_plain(s, max_w)


def _truncate_plain(s: str, max_w: int) -> str:
    if visual_width(s) <= max_w:
        return s
    target = max_w - 2
    out = ""
    w = 0
    for c in s:
        cw = visual_width(c)
        if w + cw > target:
            break
        out += c
        w += cw
    return out.rstrip() + ".."


def pad_visual(s: str, width: int) -> str:
    """Pad s on the right with spaces until it has the given visual width."""
    pad = width - visual_width(s)
    return s + " " * pad if pad > 0 else s

# Command prefix character
CMD_PREFIX = "!"

# Known command names (used for matching without ! prefix after mention)
COMMAND_NAMES = {
    "ping", "help", "prefix", "path", "multipath", "stats", "estadisticas", "trace",
    "clocks", "clock", "wx", "health", "prop", "sendq",
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
        "clocks": _cmd_clocks,
        "clock": _cmd_clocks,
        "wx": _cmd_wx,
        "health": _cmd_health,
        "prop": _cmd_prop,
        "sendq": _cmd_sendq,
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
        f"{CMD_PREFIX}ping {CMD_PREFIX}help {CMD_PREFIX}prefix <XX> "
        f"{CMD_PREFIX}path {CMD_PREFIX}multipath {CMD_PREFIX}stats "
        f"{CMD_PREFIX}clocks [stats] [Nh] {CMD_PREFIX}health [Nh] "
        f"{CMD_PREFIX}wx [city] {CMD_PREFIX}prop [city] "
        f"{CMD_PREFIX}sendq {CMD_PREFIX}pollen. Or ask me anything!"
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
    total = mesh.state.get_total_routes()
    if total == 0:
        return "Sin rutas registradas"

    types = mesh.state.get_route_types()
    pct_2byte = round(100 * types["types"].get("2-byte", 0) / total)

    top = await mesh.get_top_repeaters_grouped(
        exclude_prefixes=config.stats.exclude_prefixes,
        limit=config.stats.repeaters_max,
    )

    # Shrink the name budget if needed so the whole response rides in one
    # mesh packet instead of falling back to one packet per line.
    header = f"{total} rutas, {pct_2byte}% 2B"
    max_bytes = config.message.max_length
    response = ""
    for name_max in (20, 18, 16, 14, 12, 10):
        lines = [header]
        for name, count in top:
            pct = round(100 * count / total)
            lines.append(f"{pct}% {truncate_visual(name, name_max)}")
        response = "\n".join(lines)
        if len(response.encode("utf-8")) <= max_bytes:
            break

    return response


async def _cmd_trace(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """Trace a route and report SNR at each hop (round-trip).

    The path is used as-is (closest to farthest from bot).
    """
    path = args.strip()
    if not path:
        return f"Usage: {CMD_PREFIX}trace <prefixes> (e.g. trace ed97,ceba)"

    result = await mesh.traceroute(path, reverse=False)
    if result.get("error"):
        return f"Trace error: {result['error']}"

    return format_trace(result, config.message.max_length)


def _fmt_drift(seconds: int) -> str:
    """Render a clock drift in human-friendly units, signed."""
    sign = "+" if seconds >= 0 else "-"
    s = abs(seconds)
    if s < 60:
        return f"{sign}{s}s"
    if s < 3600:
        return f"{sign}{s // 60}m"
    if s < 86400:
        return f"{sign}{s // 3600}h"
    return f"{sign}{s // 86400}d"


CLOCK_DRIFT_THRESHOLD_S = 30


async def _cmd_clocks(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """List clock drift, or a network-wide summary with `stats`.

    Forms accepted:
      !clocks               -> nodes with |drift| > 30s in last 48h
      !clocks 24            -> same, 24h window
      !clocks stats         -> summary (median, %within thresholds, worst)
      !clocks stats 24      -> same, 24h window
    """
    parts = args.strip().lower().split()
    mode = "list"
    if parts and parts[0] == "stats":
        mode = "stats"
        parts = parts[1:]
    hours = 48
    if parts:
        h = parts[0].rstrip("h").strip()
        try:
            hours = max(1, int(h))
        except ValueError:
            pass

    if mode == "stats":
        return _format_clock_stats(mesh, config, hours)
    return _format_clock_list(mesh, config, hours)


def _format_clock_list(mesh: MeshConnection, config: BotConfig, hours: int) -> str:
    cutoff = time.time() - hours * 3600
    candidates: list[tuple[str, int]] = []
    for info in mesh.iter_adverts(since=cutoff):
        drift = info.get("last_drift")
        if drift is None or abs(drift) < CLOCK_DRIFT_THRESHOLD_S:
            continue
        name = info.get("name") or "?"
        candidates.append((name, int(drift)))

    if not candidates:
        return f"Sin nodos drift>{CLOCK_DRIFT_THRESHOLD_S}s en últ. {hours}h"

    candidates.sort(key=lambda x: abs(x[1]), reverse=True)

    header = f"{len(candidates)} nodos drift>{CLOCK_DRIFT_THRESHOLD_S}s, últ. {hours}h"
    max_bytes = config.message.max_length

    response = ""
    for name_max in (20, 18, 16, 14, 12, 10):
        lines = [header]
        for name, drift in candidates[: config.stats.repeaters_max]:
            lines.append(f"{_fmt_drift(drift)} {truncate_visual(name, name_max)}")
        response = "\n".join(lines)
        if len(response.encode("utf-8")) <= max_bytes:
            break
    return response


def _format_clock_stats(mesh: MeshConnection, config: BotConfig, hours: int) -> str:
    s = mesh.compute_clock_drift_stats(window_hours=hours)
    if s["count"] == 0:
        return f"Sin datos en últ. {hours}h"
    lines = [
        f"Drift red ({hours}h, N={s['count']})",
        f"mediana {_fmt_drift(s['median_seconds'])}",
        f"≤30s {s['within_30s_pct']}% ≤1h {s['within_1h_pct']}%",
        f">1d {s['over_1d_pct']}% >30d {s['over_30d_pct']}% >1y {s['over_1y_pct']}%",
        f"peor {_fmt_drift(s['worst_drift_seconds'])} "
        f"{truncate_visual(s['worst_name'], 16)}",
    ]
    return "\n".join(lines)


def _fmt_ago_short(seconds: float) -> str:
    """Compact relative age like '3d', '12h', '45m'."""
    s = int(max(0, seconds))
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h"
    return f"{s // 86400}d"


async def _cmd_health(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """List repeaters that haven't advertised in N hours (default 48h).

    "Last seen" comes from our own RX clock (the adverts table's
    last_seen column), not from the timestamp embedded in the advert by
    the sender. The embedded value reflects the sender's RTC, which on
    this network is often years off and would make every node look
    long-silent.
    """
    hours = 48
    arg = args.strip().lower().rstrip("h").strip()
    if arg:
        try:
            hours = max(1, int(arg))
        except ValueError:
            pass

    cutoff = time.time() - hours * 3600
    candidates: list[tuple[str, float]] = []
    for info in mesh.iter_adverts(repeater_only=True):
        last_seen = info.get("last_seen") or 0
        name = info.get("name") or ""
        if not name or last_seen == 0:
            continue
        if last_seen < cutoff:
            candidates.append((name, time.time() - last_seen))

    if not candidates:
        return f"Repetidores OK (todos vistos en últ. {hours}h) ✅"

    candidates.sort(key=lambda x: x[1], reverse=True)

    header = f"{len(candidates)} rep. mudos >{hours}h"
    max_bytes = config.message.max_length

    response = ""
    for name_max in (20, 18, 16, 14, 12, 10):
        lines = [header]
        for name, age in candidates[: config.stats.repeaters_max]:
            lines.append(f"{_fmt_ago_short(age)} {truncate_visual(name, name_max)}")
        response = "\n".join(lines)
        if len(response.encode("utf-8")) <= max_bytes:
            break
    return response


async def _cmd_sendq(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """Show recent send failures (DM no-ACK or channel send error)."""
    failures = list(mesh._send_failure_log)
    if not failures:
        return "Sin envíos fallidos recientes ✅"

    failures.sort(key=lambda f: f["time"], reverse=True)
    header = f"{len(failures)} fallos de envío recientes"
    max_bytes = config.message.max_length

    response = ""
    for name_max in (20, 18, 16, 14, 12, 10):
        lines = [header]
        for f in failures[: config.stats.repeaters_max]:
            ago = _fmt_ago_short(time.time() - f["time"])
            kind = f.get("kind", "?")
            short_name = truncate_visual(f.get("name") or "?", name_max)
            lines.append(f"{ago} {kind} {short_name}: {f.get('reason')}")
        response = "\n".join(lines)
        if len(response.encode("utf-8")) <= max_bytes:
            break
    return response


async def _cmd_wx(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """Show current weather for a city (default = config.weather_default_location)."""
    location = args.strip() or config.weather_default_location
    return await fetch_weather(location)


async def _cmd_prop(
    args: str, message: MeshMessage, config: BotConfig, mesh: MeshConnection
) -> str:
    """Show current HF propagation summary; location picks day/night slice."""
    location = args.strip() or config.weather_default_location
    return await fetch_propagation(location)


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
