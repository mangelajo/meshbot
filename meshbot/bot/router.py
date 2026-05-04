"""Message routing and response length handling."""

import asyncio
import logging
import re

from pydantic_ai import Agent
from pydantic_ai.usage import UsageLimits

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
    # Private messages are always processed
    if message.is_private:
        return True

    text = message.text.strip()

    # Always process commands
    if is_command(text):
        return True

    # If the message mentions someone with @ and it's NOT the bot, skip it
    bot_lower = config.bot_name.lower()
    mentions = re.findall(r"@\[([^\]]+)\]|(?:^|(?<=\s))@([\w\d_-]+)", text)
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
    logger.info("Agent prompt: %s", prompt)

    # Budget per response = max_length × max_parts. The packer in
    # loop.py splits at \n\n boundaries; within each section it greedy-
    # packs into max_length-byte packets. The model is told the rules
    # in the system prompt; this is just the safety net.
    max_len = config.message.max_length
    max_parts = max(1, config.message.max_parts)
    budget = max_len * max_parts
    max_retries = 2
    agent_timeout = 120  # seconds

    try:
        result = await asyncio.wait_for(
            agent.run(prompt, deps=mesh, usage_limits=UsageLimits(request_limit=8)),
            agent_timeout,
        )
    except TimeoutError:
        logger.error("Agent timed out after %ds", agent_timeout)
        return None

    response = str(result.output).strip()
    logger.info("Agent response (%d chars): %s", len(response), response)

    # Agent signals no response needed
    if response == "NO_RESPONSE":
        logger.info("Agent decided no response needed")
        return None

    # If response fits in the multi-packet budget, return it.
    if len(response) <= budget:
        return response

    # Feed the error back to the agent so it can self-correct
    msg_history = result.all_messages()
    for attempt in range(max_retries):
        over = len(response) - budget
        error_msg = (
            f"ERROR: response is {len(response)} chars, max is {budget} "
            f"({over} over). Shorten so the whole reply fits in {budget} "
            f"chars total ({max_parts} packets of {max_len})."
        )
        logger.info("Agent retry #%d: %s", attempt + 1, error_msg)

        try:
            result = await asyncio.wait_for(
                agent.run(
                    error_msg, message_history=msg_history, deps=mesh,
                    usage_limits=UsageLimits(request_limit=2),
                ),
                agent_timeout,
            )
        except TimeoutError:
            logger.error("Agent retry timed out after %ds", agent_timeout)
            return response[:budget]

        response = str(result.output).strip()
        logger.info("Agent retry response (%d chars): %s", len(response), response)

        if response == "NO_RESPONSE":
            return None
        if len(response) <= budget:
            return response
        msg_history = result.all_messages()

    # Last resort: truncate to the full budget. The packer will still
    # split it sensibly into packets.
    logger.warning("Agent failed to shorten after %d retries, truncating", max_retries)
    return response[:budget]
