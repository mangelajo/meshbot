"""Message routing and response length handling."""

import logging
import re

from pydantic_ai import Agent

from meshbot.bot.commands import (
    CMD_PREFIX,
    COMMAND_NAMES,
    handle_command,
    is_command,
    parse_command,
)
from meshbot.bot.mesh import MeshConnection
from meshbot.models import BotConfig, MeshMessage

logger = logging.getLogger("meshbot.router")


def should_process(message: MeshMessage, config: BotConfig) -> bool:
    """Decide whether the bot should process this message."""
    text = message.text.strip()

    # Always process commands
    if is_command(text):
        return True

    # If the message mentions someone with @ and it's NOT the bot, skip it
    bot_lower = config.bot_name.lower()
    mentions = re.findall(r"@\[([^\]]+)\]|@([\w\d_-]+)", text)
    for bracket_name, plain_name in mentions:
        name = bracket_name or plain_name
        if name.lower() != bot_lower:
            return False

    if config.trigger_mode == "all":
        return True

    # Mention mode: check if @[bot_name], @bot_name, or bot_name appears
    bot = re.escape(config.bot_name)
    pattern = rf"(?:@\[{bot}\]|@{bot}|{bot})"
    return bool(re.search(pattern, text, re.IGNORECASE))


def strip_mention(text: str, bot_name: str) -> str:
    """Remove @[bot_name], @bot_name, or bot_name mention from the message text."""
    bot = re.escape(bot_name)
    # Try @[name] first (mesh radio format), then @name, then bare name
    stripped = re.sub(
        rf"(?:@\[{bot}\]|@{bot}|{bot})[,:;]?\s*",
        "", text, count=1, flags=re.IGNORECASE,
    )
    return stripped.strip()


def _looks_like_command(text: str) -> tuple[str, str] | None:
    """Check if text starts with a known command name (without ! prefix).

    Returns (cmd, args) if matched, None otherwise.
    """
    parts = text.split(None, 1)
    if not parts:
        return None
    word = parts[0].lower()
    if word in COMMAND_NAMES:
        return word, parts[1] if len(parts) > 1 else ""
    return None


async def route_message(
    message: MeshMessage,
    config: BotConfig,
    agent: Agent[MeshConnection, str],
    mesh: MeshConnection,
    history: list[tuple[str, str]] | None = None,
) -> str | None:
    """Route a message to the appropriate handler and return the response.

    Args:
        history: List of (sender, text) tuples for recent channel messages.

    Returns None if the message should not be processed.
    """
    if not should_process(message, config):
        return None

    text = message.text.strip()

    # Try !command handling first
    if is_command(text):
        cmd, args = parse_command(text)
        response = await handle_command(cmd, args, message, config, mesh)
        if response is not None:
            return response
        # Unknown command — fall through to agent
        text = text[len(CMD_PREFIX) :]

    # Strip the bot mention
    text = strip_mention(text, config.bot_name)

    # Try matching a command name without ! prefix (e.g. "@b0b0t prefix d2")
    cmd_match = _looks_like_command(text)
    if cmd_match is not None:
        cmd, args = cmd_match
        response = await handle_command(cmd, args, message, config, mesh)
        if response is not None:
            return response

    # Route to AI agent with message context and history
    return await _run_agent(text, message, config, agent, mesh, history)


def _format_history(history: list[tuple[str, str]] | None) -> str:
    """Format recent messages as background context for the agent."""
    if not history:
        return ""
    lines = [f"  {sender}: {text}" for sender, text in history]
    return (
        "[Channel log for context only, do NOT respond to these, "
        "only respond to the message below]\n"
        + "\n".join(lines)
        + "\n\n"
    )


async def _run_agent(
    text: str,
    message: MeshMessage,
    config: BotConfig,
    agent: Agent[MeshConnection, str],
    mesh: MeshConnection,
    history: list[tuple[str, str]] | None = None,
) -> str:
    """Run the AI agent and handle response length."""
    # Build prompt: prefix first, then history as context, then the actual message
    parts: list[str] = []
    if config.prompt_prefix:
        parts.append(config.prompt_prefix)

    history_ctx = _format_history(history)
    if history_ctx:
        parts.append(history_ctx)

    parts.append(f"[From {message.sender}, {message.path_len} hops] {text}")
    prompt = "\n".join(parts)
    logger.debug("Agent prompt: %s", prompt)

    result = await agent.run(prompt, deps=mesh)
    response = str(result.output).strip()
    logger.debug("Agent response (%d chars): %s", len(response), response)

    # Agent signals no response needed
    if response == "NO_RESPONSE":
        logger.debug("Agent decided no response needed")
        return None

    max_len = config.message.max_length

    if len(response) <= max_len:
        return response

    # Retry with explicit brevity instruction
    sender_ctx = f"[From {message.sender}, {message.path_len} hops] "
    retry_prompt = f"{sender_ctx}Answer in under {max_len} chars: {text}"
    if config.prompt_prefix:
        retry_prompt = f"{config.prompt_prefix} {retry_prompt}"
    logger.debug("Agent retry (too long): %s", retry_prompt)

    result = await agent.run(retry_prompt, deps=mesh)
    response = str(result.output)
    logger.debug("Agent retry response (%d chars): %s", len(response), response)

    if len(response) <= max_len:
        return response

    # Split into multipart messages
    return _split_response(response, max_len, config.message.max_parts)


def _split_response(text: str, max_length: int, max_parts: int) -> str:
    """Split a long response into multipart format [1/N]...[N/N].

    Returns all parts joined by newlines (the caller sends each line separately).
    """
    # Reserve space for part label like " [1/3]"
    label_len = len(f" [{max_parts}/{max_parts}]")
    chunk_size = max_length - label_len

    if chunk_size <= 0:
        return text[:max_length]

    # Split on word boundaries
    chunks: list[str] = []
    remaining = text
    while remaining and len(chunks) < max_parts:
        if len(remaining) <= chunk_size:
            chunks.append(remaining)
            remaining = ""
        else:
            # Find last space within chunk_size
            split_at = remaining.rfind(" ", 0, chunk_size)
            if split_at <= 0:
                split_at = chunk_size
            chunks.append(remaining[:split_at].rstrip())
            remaining = remaining[split_at:].lstrip()

    total = len(chunks)
    if total == 1:
        return chunks[0]

    return "\n".join(f"{chunk} [{i + 1}/{total}]" for i, chunk in enumerate(chunks))
