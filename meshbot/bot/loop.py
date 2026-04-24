"""Main async event loop for the bot."""

import asyncio
import logging
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
        logging.getLogger("openai").setLevel(logging.WARNING)
        logging.getLogger("meshcore").setLevel(logging.WARNING)
        logging.getLogger("mcp").setLevel(logging.WARNING)


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
        # Ring buffer for recent messages (sender, text) for agent context
        history: deque[tuple[str, str]] = deque(maxlen=config.history_size)

        # Wait briefly for queued messages from before startup, then drain them
        await asyncio.sleep(2)
        drained = 0
        while not mesh._queue.empty():
            msg = mesh._queue.get_nowait()
            if msg.sender:
                mesh.last_seen[msg.sender] = time.time()
            drained += 1
        if drained:
            logger.info("Drained %d queued messages from before startup", drained)

        console.print(
            f"[bold green]meshbot[/] listening on channel "
            f"{config.channel} (index {mesh.channel_idx})."
        )
        await mesh.send(mesh.channel_idx, f"@{config.bot_name} está listo.")

        last_response_time = 0.0

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

                # Filter by channel
                if msg.channel_idx != mesh.channel_idx:
                    continue

                logger.info(
                    "[dim]<< [bold]%s[/bold] (hops=%d): %s[/]",
                    msg.sender, msg.path_len, msg.text,
                    extra={"markup": True},
                )

                # Always record in history
                history.append((msg.sender, msg.text))

                # Cooldown: skip if too soon since last response
                elapsed = time.time() - last_response_time
                if elapsed < config.cooldown:
                    logger.info(
                        "Cooldown: %.1fs remaining, skipping",
                        config.cooldown - elapsed,
                    )
                    continue

                response = await route_message(
                    msg, config, agent, mesh, history=list(history)
                )
                if response is None:
                    continue

                # Send each line as a separate message (multipart support)
                for part in response.split("\n"):
                    part = part.strip()
                    if not part:
                        continue
                    logger.info("[bold]>> %s[/]", part, extra={"markup": True})
                    await mesh.send(mesh.channel_idx, part)

                last_response_time = time.time()

                # Add bot's response to history too
                history.append((config.bot_name, response.split("\n")[0]))

            except Exception as e:
                logger.error("Error processing message: %s", e, exc_info=config.debug)

        await mesh.send(mesh.channel_idx, f"@{config.bot_name} has been stopped.")

    console.print("[bold green]meshbot[/] stopped.")
