# meshbot

## Project Overview
meshbot is a MeshCore mesh network chatbot with AI agent support. It listens on a mesh channel,
handles predefined commands (prefix, ping, help, path), and routes free-form queries to an AI
agent via PydanticAI.

## Tech Stack
- Python 3.12+
- uv for dependency and venv management
- PydanticAI for agentic framework (supports Anthropic, DeepSeek, MiniMax, Ollama)
- FastMCP for MCP server wrapping meshcore
- Click for CLI
- Rich for terminal output

## Development Setup
- Use `uv` for all dependency and venv management
- `uv sync` to install dependencies
- `uv run meshbot` to run the bot
- Run commands via the Makefile for consistency

## Project Structure
```
meshbot/
├── meshbot/
│   ├── cli/           # Click CLI entry points (run, mcp-server)
│   ├── bot/           # Bot core (agent, commands, router, event loop)
│   └── mcp_server/    # FastMCP server wrapping meshcore
├── tests/             # Test files
├── pyproject.toml     # Project configuration
├── Makefile           # Development commands
└── config.example.yaml
```

## Architecture
- **MCP Server** (`meshbot mcp-server`): Wraps meshcore via FastMCP. Owns the serial connection.
  Exposes tools: poll_messages, send_channel_message, get_repeaters, get_node_by_prefix, etc.
  Can be used standalone by Claude Code or other MCP clients.
- **Bot** (`meshbot run`): Spawns MCP server as subprocess, polls for messages, routes to
  command handlers or PydanticAI agent, sends responses back via MCP.
- Config: YAML file + CLI overrides + env vars for secrets.

## Key Commands
- `make sync` — sync dependencies with uv
- `make test` — run tests with coverage
- `make lint` — ruff + mypy checks
- `make format` — format code with ruff
- `uv run meshbot -p /dev/ttyUSB0 run` — start bot
- `uv run meshbot -p /dev/ttyUSB0 mcp-server` — start MCP server standalone
