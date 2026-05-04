"""Main async event loop for the bot."""

import asyncio
import logging
import random
import re
import signal
import time
from collections import deque

from rich.console import Console
from rich.logging import RichHandler

from meshbot.bot.agent import create_agent
from meshbot.bot.mesh import MeshConnection
from meshbot.bot.router import route_message
from meshbot.models import BotConfig

console = Console(stderr=True)

# Random pause between packets when a response has more than one. Keeps
# the bot from hogging the channel on multi-packet replies; tuned so the
# typical SF8 BW62.5 airtime (~1s per ~50-byte packet) does not stack up.
PACKET_GAP_RANGE = (3.0, 4.0)


def _split_oversize_line(line: str, max_bytes: int) -> tuple[str, str]:
    """Split a single line that is too big into (head, rest), trying
    sentence boundaries first, then word boundaries, then a hard cut.
    `head` is guaranteed to fit; `rest` may still be oversize and gets
    fed back through the loop."""
    encoded = line.encode("utf-8")
    if len(encoded) <= max_bytes:
        return line, ""

    # Walk backwards from the byte cap to find the best break point.
    # Decoding may chop a multi-byte char, so we trim to a valid string
    # first and then look for a separator in the result.
    cap = encoded[:max_bytes]
    try:
        head_text = cap.decode("utf-8")
    except UnicodeDecodeError:
        # Trim to last valid utf-8 boundary
        for n in range(max_bytes, 0, -1):
            try:
                head_text = encoded[:n].decode("utf-8")
                break
            except UnicodeDecodeError:
                continue
        else:
            head_text = ""

    # Prefer sentence enders, then commas / em dashes, then any space.
    for pat in (r".*[.!?…][ \t]", r".*[,;:][ \t]", r".*\s"):
        m = re.match(pat, head_text, re.DOTALL)
        if m:
            head = m.group(0).rstrip()
            rest = line[len(m.group(0)):]
            return head, rest

    # No sensible break: hard cut at byte boundary.
    return head_text.rstrip(), line[len(head_text):]


def pack_response(response: str, max_bytes: int, max_parts: int) -> list[str]:
    """Split a response into per-packet chunks.

    Hard packet boundary: `\\n\\n` (blank line) — never combined across.
    Within each section, lines (`\\n`) are greedy-packed into packets so
    short multi-line content (lists, etc.) shares a packet when it fits.
    Lines that exceed `max_bytes` are fragmented at sentence/word
    boundaries by `_split_oversize_line` so we never silently truncate.
    Caps the result at `max_parts` packets, suffixing "[…]" on the last
    one when content was dropped.
    """
    parts: list[str] = []
    sections = response.split("\n\n")
    for section in sections:
        section = section.strip()
        if not section:
            continue
        current = ""
        for raw_line in section.split("\n"):
            line = raw_line.strip()
            if not line:
                continue
            # Fragment overlong single lines first so the packer below
            # only deals with safely-sized pieces.
            while len(line.encode("utf-8")) > max_bytes:
                head, line = _split_oversize_line(line, max_bytes)
                if not head:
                    break
                # Push current section content first if any, then the
                # fragment as its own packet.
                if current:
                    parts.append(current)
                    current = ""
                parts.append(head)
            if not line:
                continue
            candidate = f"{current}\n{line}" if current else line
            if len(candidate.encode("utf-8")) <= max_bytes:
                current = candidate
            else:
                if current:
                    parts.append(current)
                current = line
        if current:
            parts.append(current)

    if len(parts) > max_parts:
        dropped = len(parts) - max_parts
        parts = parts[:max_parts]
        # Append ellipsis indicator without busting max_bytes.
        marker = " […]"
        last = parts[-1]
        if len((last + marker).encode("utf-8")) <= max_bytes:
            parts[-1] = last + marker
        else:
            parts[-1] = last[: max_bytes - len(marker.encode("utf-8"))] + marker
        logger = logging.getLogger("meshbot.loop")
        logger.warning(
            "Response truncated: %d packets dropped (max_parts=%d)",
            dropped, max_parts,
        )
    return parts


def _setup_logging(config: BotConfig) -> None:
    """Configure logging with Rich handler."""
    level = logging.DEBUG if config.debug else logging.INFO
    if not config.verbose and not config.debug:
        level = logging.WARNING

    handler = RichHandler(
        console=console,
        show_path=False,
        rich_tracebacks=True,
        tracebacks_show_locals=config.debug,
    )
    handler.setFormatter(logging.Formatter("%(message)s", datefmt="[%X]"))

    root = logging.getLogger("meshbot")
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)

    # Quieten noisy libraries unless debug
    if not config.debug:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("meshcore").setLevel(logging.WARNING)
        logging.getLogger("mcp").setLevel(logging.WARNING)
    # Always show openai requests at DEBUG to diagnose model interactions
    logging.getLogger("openai").setLevel(logging.DEBUG if config.verbose else logging.WARNING)


async def run_bot(config: BotConfig) -> None:
    """Main bot loop: connect to mesh, listen for messages, route and respond."""
    _setup_logging(config)
    logger = logging.getLogger("meshbot.loop")

    console.print(f"[bold green]meshbot[/] starting as [bold]{config.bot_name}[/]")
    console.print(
        f"  port={config.serial_port} channel={config.channel} "
        f"provider={config.provider} model={config.model} trigger={config.trigger_mode}"
    )

    mesh = MeshConnection(config)

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        console.print("\n[yellow]Shutting down...[/]")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    async with mesh:
        agent = create_agent(config, mesh)
        # Ring buffer for recent channel messages (sender, text) for agent
        # context. DMs get a separate per-pubkey history persisted on
        # MeshConnection so private conversations survive a restart.
        history: deque[tuple[str, str]] = deque(maxlen=config.history_size)

        # Wait briefly for queued messages from before startup, then drain them
        await asyncio.sleep(2)
        drained = 0
        while not mesh._queue.empty():
            msg = mesh._queue.get_nowait()
            mesh._record_seen(msg.sender, config.channel if not msg.is_private else "DM")
            drained += 1
        if drained:
            logger.info("Drained %d queued messages from before startup", drained)

        console.print(
            f"[bold green]meshbot[/] listening on channel "
            f"{config.channel} (index {mesh.channel_idx})."
        )
        # await mesh.send(mesh.channel_idx, f"@{config.bot_name} está listo.")

        last_response_per_user: dict[str, float] = {}

        while not shutdown_event.is_set():
            try:
                # Wait for a message or shutdown, whichever comes first
                recv_task = asyncio.create_task(mesh.recv())
                shutdown_task = asyncio.create_task(shutdown_event.wait())
                done, pending = await asyncio.wait(
                    {recv_task, shutdown_task}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                if shutdown_event.is_set():
                    break

                msg = recv_task.result()

                # Filter: channel messages must match our channel
                if not msg.is_private and msg.channel_idx != mesh.channel_idx:
                    continue

                if msg.is_private:
                    logger.info(
                        "[dim]<< DM [bold]%s[/bold]: %s[/]",
                        msg.sender, msg.text,
                        extra={"markup": True},
                    )
                else:
                    logger.info(
                        "[dim]<< [bold]%s[/bold] (hops=%d): %s[/]",
                        msg.sender, msg.path_len, msg.text,
                        extra={"markup": True},
                    )

                # Record incoming channel text in the in-memory ring
                # buffer for cross-message context. DMs are already
                # persisted by mesh._on_private_message → message_store,
                # which is what get_dm_history reads back later.
                if not msg.is_private:
                    history.append((msg.sender, msg.text))

                # Per-user cooldown: skip if too soon since last response to this user
                sender_key = msg.sender or msg.pubkey_prefix or "unknown"
                last_resp = last_response_per_user.get(sender_key, 0)
                elapsed = time.time() - last_resp
                if elapsed < config.cooldown:
                    logger.info(
                        "Cooldown for %s: %.1fs remaining, skipping",
                        sender_key, config.cooldown - elapsed,
                    )
                    continue

                # Private messages always go to agent (no mention check needed)
                if msg.is_private:
                    pk = msg.pubkey_prefix or "unknown"
                    response = await route_message(
                        msg, config, agent, mesh,
                        history=mesh.get_dm_history(pk, config.history_size),
                    )
                else:
                    response = await route_message(
                        msg, config, agent, mesh, history=list(history)
                    )
                if response is None:
                    continue

                # Split response into mesh packets. The model can use
                # \n\n to mark hard packet boundaries (for summaries /
                # multi-step replies); within each section \n is just
                # in-packet formatting (lists, etc.) that gets greedy-
                # packed together when it fits. See pack_response().
                parts = pack_response(
                    response,
                    max_bytes=config.message.max_length,
                    max_parts=config.message.max_parts,
                )

                send_ok = True
                for i, part in enumerate(parts):
                    if i > 0:
                        # Polite gap so we don't hog the channel on
                        # multi-packet replies. Random within a small
                        # range to avoid a deterministic cadence that
                        # could collide with periodic traffic.
                        await asyncio.sleep(random.uniform(*PACKET_GAP_RANGE))
                    logger.info("[bold]>> %s[/]", part, extra={"markup": True})
                    if msg.is_private:
                        if not await mesh.send_private(msg.pubkey_prefix, part):
                            send_ok = False
                    else:
                        await mesh.send(mesh.channel_idx, part)

                # Only apply cooldown if send succeeded
                if send_ok:
                    last_response_per_user[sender_key] = time.time()

                # Mirror the bot's reply into whichever history bucket
                # the request came from. For DMs we persist to the
                # messages table so the next get_dm_history call sees
                # both sides; for the channel we keep the in-memory
                # ring buffer (channel transcripts are large and only
                # needed as recent context).
                first_line = response.split("\n")[0]
                if msg.is_private:
                    mesh.message_store.record_outgoing(
                        sender=config.bot_name,
                        text=first_line,
                        channel_name="DM",
                        target_pubkey_prefix=msg.pubkey_prefix or None,
                        is_private=True,
                    )
                else:
                    history.append((config.bot_name, first_line))

            except Exception as e:
                logger.error("Error processing message: %s", e, exc_info=config.debug)

        # await mesh.send(mesh.channel_idx, f"@{config.bot_name} has been stopped.")

    console.print("[bold green]meshbot[/] stopped.")
