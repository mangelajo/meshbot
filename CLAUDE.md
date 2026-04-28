# meshbot

## Project Overview
meshbot is a MeshCore mesh network chatbot with AI agent support. It listens on mesh channels,
handles predefined commands (prefix, ping, help, path, multipath, stats, pollen), and routes
free-form queries to a PydanticAI agent. It stores messages from multiple channels in SQLite
with FTS5 full-text search for historical queries.

## Tech Stack
- Python 3.12+
- uv for dependency and venv management
- PydanticAI for agentic framework (supports Anthropic, DeepSeek, MiniMax, Ollama)
- FastMCP for MCP server wrapping meshcore
- SQLite + FTS5 for message storage and search
- Click for CLI
- Rich for terminal output

## Development Setup
- Use `uv` for all dependency and venv management
- `uv sync` to install dependencies
- `uv run meshbot` to run the bot
- Run commands via the Makefile for consistency

## Testing rules
- Tests must NEVER read or write the project's live data files. All
  bot state lives in `meshbot.db` (SQLite, alongside the bot) plus
  `config.yaml`. The test runner shares a working directory with the
  production process, so a stray default path will clobber real data.
- Always pass an isolated path. Patterns in the existing suite:
  - `StateStore(Path(tempfile.mkdtemp()) / DB_FILENAME)` for the unified
    store.
  - `MessageStore(db_path=":memory:")` or against a tempdir DB for the
    message-search layer.
  - `MeshConnection(BotConfig(), data_dir=tempfile.mkdtemp())` for
    full-stack tests; both StateStore and MessageStore inherit the
    isolation through `data_dir`.
- A constructor that defaults to a project-root filename (e.g. an old
  `RouteStats("route_stats.json")` style) is a footgun. If you spot
  one, fix the test to pass an explicit path; don't rely on cwd.

## State storage
- All persisted runtime state lives in a single SQLite file
  (`meshbot.db`), evolved via numbered migrations in
  `meshbot/bot/state_store.py::MIGRATIONS`. To add a new schema
  change, append a `(N, callable)` tuple — the runner applies anything
  past the recorded `schema_version` inside one transaction with
  rollback on error.

## Project Structure
```
meshbot/
├── meshbot/
│   ├── cli/              # Click CLI entry points (run, mcp-server)
│   ├── bot/              # Bot core (agent, commands, router, event loop)
│   │   ├── mesh.py       # MeshConnection: serial, events, contacts, routes
│   │   ├── agent.py      # PydanticAI agent with tools
│   │   ├── commands.py   # Bot commands (ping, path, stats, pollen, etc.)
│   │   ├── router.py     # Message routing + length handling
│   │   ├── loop.py       # Main async event loop
│   │   ├── message_store.py  # SQLite + FTS5 message storage
│   │   ├── pollen.py     # Pollen data fetcher (sigueros.es)
│   │   └── stats.py      # Route statistics histograms
│   └── mcp_server/       # FastMCP server (standalone for Claude Code)
├── tests/                # Test files
├── pyproject.toml        # Project configuration
├── Makefile              # Development commands
└── config.example.yaml
```

## Architecture
- **Bot** (`meshbot run`): Connects directly to meshcore via serial. Event-driven message
  reception. Routes to command handlers or PydanticAI agent. Stores all messages in SQLite.
  Can listen on multiple channels but only responds on its own.
- **MCP Server** (`meshbot mcp-server`): Standalone FastMCP server for Claude Code or other
  MCP clients. Exposes all meshcore tools + message search + stats + pollen.
- Config: YAML file + CLI overrides + env vars for secrets.

## Key Commands
- `make sync` — sync dependencies with uv
- `make test` — run tests with coverage
- `make lint` — ruff + mypy checks
- `make format` — format code with ruff
- `uv run meshbot -p /dev/ttyUSB0 run` — start bot
- `uv run meshbot -p /dev/ttyUSB0 mcp-server` — start MCP server standalone

## Using the MCP Server with Claude Code

The MCP server exposes all meshbot tools (meshcore, search, stats, pollen) for use
with Claude Code or any MCP-compatible client.

### Setup

Add to your Claude Code MCP config (`~/.claude/claude_desktop_config.json` or project
`.mcp.json`):

```json
{
  "mcpServers": {
    "meshbot": {
      "command": "uv",
      "args": ["run", "--project", "/path/to/meshbot", "meshbot", "-p", "/dev/ttyUSB0", "mcp-server"],
      "env": {}
    }
  }
}
```

Replace `/path/to/meshbot` with the absolute path to the meshbot project directory,
and `/dev/ttyUSB0` with your serial port (e.g. `/dev/tty.usbmodem2101` on macOS).

### Available MCP Tools

**Meshcore:**
- `poll_messages(channel_idx?)` — drain message buffer
- `send_channel_message(channel_idx, text)` — send to a channel
- `get_contacts()` — list all contacts
- `get_repeaters()` — list repeater nodes
- `get_status()` — connection status and device info

**Node Lookup:**
- `get_node_by_prefix(prefix)` — look up node by hex prefix
- `resolve_prefixes(prefixes)` — resolve comma-separated hex prefixes to names
- `search_contacts(name)` — search contacts by name

**Statistics:**
- `get_top_repeaters(limit?)` — most seen repeaters with names
- `get_route_type_stats()` — route type distribution

**Message Search:**
- `search_messages(query, limit?)` — full-text search across stored messages
- `search_messages_by_sender(sender, limit?)` — search by sender name
- `get_message_stats()` — message counts per channel

**Other:**
- `get_pollen_levels()` — current pollen levels for Madrid
