"""CLI command to run the MCP server standalone."""

import click


@click.command()
@click.pass_context
def mcp_server(ctx: click.Context) -> None:
    """Run the MCP server standalone (for Claude Code or other MCP clients)."""
    from meshbot.mcp_server.server import mcp

    mcp.run(transport="stdio")
