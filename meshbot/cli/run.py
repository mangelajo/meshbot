"""CLI command to run the meshbot."""

import asyncio

import click


@click.command()
@click.option(
    "--channel",
    type=str,
    default=None,
    help="Channel name to join, e.g. '#bot' (overrides config)",
)
@click.option(
    "--provider",
    type=click.Choice(["anthropic", "deepseek", "minimax", "ollama"]),
    default=None,
    help="LLM provider (overrides config)",
)
@click.option(
    "--model",
    type=str,
    default=None,
    help="LLM model name (overrides config)",
)
@click.option(
    "--trigger-mode",
    type=click.Choice(["all", "mention"]),
    default=None,
    help="Trigger mode (overrides config)",
)
@click.option(
    "--bot-name",
    type=str,
    default=None,
    help="Bot display name (overrides config)",
)
@click.pass_context
def run(
    ctx: click.Context,
    channel: int | None,
    provider: str | None,
    model: str | None,
    trigger_mode: str | None,
    bot_name: str | None,
) -> None:
    """Start the meshbot: listen for messages, route to commands or AI agent."""
    from meshbot.bot.loop import run_bot
    from meshbot.config import load_config

    config = load_config(
        ctx.obj["config"],
        serial_port=ctx.obj["serial_port"],
        baudrate=ctx.obj["baudrate"],
        debug=ctx.obj["debug"],
        verbose=ctx.obj["verbose"],
        channel=channel,
        provider=provider,
        model=model,
        trigger_mode=trigger_mode,
        bot_name=bot_name,
    )

    asyncio.run(run_bot(config))
