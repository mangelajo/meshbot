"""Command-line interface for meshbot using Click."""

import sys

import click

from .mcp_server import mcp_server
from .run import run


@click.group(invoke_without_command=True)
@click.option(
    "--serial-port",
    "-p",
    required=True,
    type=str,
    help="Serial port to connect to (e.g., /dev/ttyUSB0 or COM3)",
)
@click.option(
    "--baudrate",
    "-b",
    default=115200,
    type=int,
    help="Serial port baud rate",
)
@click.option(
    "--debug/--no-debug",
    "-d",
    default=False,
    help="Enable low-level debug logging from meshcore",
)
@click.option(
    "--verbose/--no-verbose",
    "-v",
    default=False,
    help="Print progress messages to stderr",
)
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=False),
    default=None,
    help="Path to config YAML file",
)
@click.pass_context
def cli(
    ctx: click.Context,
    serial_port: str,
    baudrate: int,
    debug: bool,
    verbose: bool,
    config: str | None,
) -> None:
    """MeshCore mesh network chatbot with AI agent support."""
    ctx.ensure_object(dict)
    ctx.obj["serial_port"] = serial_port
    ctx.obj["baudrate"] = baudrate
    ctx.obj["debug"] = debug
    ctx.obj["verbose"] = verbose
    ctx.obj["config"] = config

    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


cli.add_command(run)
cli.add_command(mcp_server, name="mcp-server")


def main() -> int:
    """Main entry point for the meshbot CLI."""
    try:
        cli(obj={})
        return 0
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 0
    except Exception:
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
