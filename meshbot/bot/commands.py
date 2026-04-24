"""Predefined command handlers for the mesh bot."""

import logging

from meshbot.bot.mesh import MeshConnection
from meshbot.models import BotConfig, MeshMessage, split_path_prefixes

logger = logging.getLogger("meshbot.commands")

# Command prefix character
CMD_PREFIX = "!"

# Known command names (used for matching without ! prefix after mention)
COMMAND_NAMES = {"ping", "help", "prefix", "path"}


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
        f"Commands: {CMD_PREFIX}ping, {CMD_PREFIX}help, "
        f"{CMD_PREFIX}prefix <XX>, {CMD_PREFIX}path, "
        f"{CMD_PREFIX}pollen/{CMD_PREFIX}polen. "
        f"Or ask me anything!"
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
