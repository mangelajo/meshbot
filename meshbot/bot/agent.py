"""PydanticAI agent factory with provider switching and mesh tools."""

import logging
import os
from typing import Any

from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel

from meshbot.bot.mesh import MeshConnection
from meshbot.bot.pollen import fetch_pollen_data
from meshbot.models import BotConfig

# Fix Ollama compatibility: Ollama rejects content=None in assistant messages
# with tool calls. PydanticAI sets content=None when the model responds with
# only tool calls (no text). Patch to use empty string instead.
_original_into_message_param = (
    OpenAIChatModel._MapModelResponseContext._into_message_param  # type: ignore[attr-defined]
)


def _patched_into_message_param(self):  # type: ignore[no-untyped-def]
    result = _original_into_message_param(self)
    if result.get("content") is None and result.get("tool_calls"):
        result["content"] = ""
    return result


OpenAIChatModel._MapModelResponseContext._into_message_param = (  # type: ignore[attr-defined]
    _patched_into_message_param
)

logger = logging.getLogger("meshbot.agent")

SYSTEM_PROMPT_TEMPLATE = """\
You are {bot_name}, a helpful assistant on a mesh radio network.
Always respond in {language}.
Keep responses under {max_length} characters — bandwidth is extremely limited.
Be concise and direct. No markdown formatting. Plain text only. Use emojis to be expressive.
Only respond to the LAST message marked with [From ...]. \
The channel log above it is background context only — do NOT respond to those messages.
If the message does not need a response (greetings between others, reactions, \
emojis, acknowledgements like "ok", "👍", "lol"), reply with exactly: NO_RESPONSE

You can answer general questions using your own knowledge.
When the question is about the mesh network, use your tools:
- Contact/node/person on the mesh -> call get_contact_info(name).
- Route/path history for a contact -> call get_contact_routes(name).
- Top repeaters / network stats -> call get_top_repeaters() or get_route_type_stats().
- Pollen/polen/allergies -> call get_pollen_levels().
- What was discussed / who said X -> call search_messages(query).
- What did person X say -> call search_messages_by_sender(sender).
- Trace route SNR for a contact -> get route with get_contact_routes, then traceroute(path).
- Trace explicit path given by user -> call trace_explicit(path) without reversing.
- Resolve hex prefixes to names -> call resolve_prefixes(prefixes).
If you see up to 4 hex prefixes, resolve them in one call.
Never invent mesh network data — always use tools for that.\
"""


def _log_result(name: str, result: Any) -> Any:
    """Log tool result and return it."""
    text = str(result)
    if len(text) > 200:
        text = text[:200] + "..."
    logger.info("Tool result %s: %s", name, text)
    return result


def build_model_string(config: BotConfig) -> str:
    """Build the PydanticAI model string from config."""
    if config.provider == "ollama":
        os.environ.setdefault("OLLAMA_BASE_URL", config.ollama_base_url)
        return f"ollama:{config.model}"
    elif config.provider == "anthropic":
        return f"anthropic:{config.model}"
    elif config.provider == "deepseek":
        return f"deepseek:{config.model}"
    elif config.provider == "minimax":
        os.environ.setdefault("OPENAI_BASE_URL", config.minimax_base_url)
        os.environ.setdefault("OPENAI_API_KEY", os.environ.get("MINIMAX_API_KEY", ""))
        return f"openai:{config.model}"
    else:
        raise ValueError(f"Unknown provider: {config.provider}")


def create_agent(config: BotConfig, mesh: MeshConnection) -> Agent[MeshConnection, str]:
    """Create a PydanticAI agent with mesh tools."""
    model_string = build_model_string(config)
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        bot_name=config.bot_name,
        max_length=config.message.max_length,
        language=config.language,
    )

    logger.info("Creating agent: provider=%s model=%s", config.provider, config.model)
    logger.debug("System prompt: %s", system_prompt)

    agent: Agent[MeshConnection, str] = Agent(
        model_string,
        system_prompt=system_prompt,
        deps_type=MeshConnection,
    )

    @agent.tool
    async def resolve_prefixes(
        ctx: RunContext[MeshConnection], prefixes: str
    ) -> list[dict[str, Any]]:
        """Resolve one or more hex prefixes to node names and info.

        Use this to look up mesh nodes by their public key hex prefix.
        Pass multiple prefixes separated by commas.

        Args:
            prefixes: Comma-separated hex prefixes (e.g. "d2,ed97,ceba").
        """
        prefix_list = [p.strip() for p in prefixes.split(",") if p.strip()]
        logger.info("Tool call: resolve_prefixes(%s)", prefix_list)
        results = []
        for prefix in prefix_list:
            node = await ctx.deps.get_node_by_prefix(prefix)
            if node:
                results.append({
                    "prefix": prefix,
                    "name": node.get("adv_name", ""),
                    "type": node.get("type"),
                    "hops": node.get("out_path_len"),
                })
            else:
                results.append({"prefix": prefix, "name": None})
        return _log_result("resolve_prefixes", results)

    @agent.tool
    async def get_contact_info(
        ctx: RunContext[MeshConnection], name: str
    ) -> list[dict[str, Any]]:
        """Search for mesh contacts by name and return their info.

        Use this when someone asks about a person, node, or contact on the mesh.
        Returns a list of matching contacts with their name, type, hops,
        last advertisement time, and last time seen on the channel.

        Args:
            name: Name or partial name to search for (case-insensitive).
        """
        logger.info("Tool call: get_contact_info(%s)", name)
        return _log_result("get_contact_info", await ctx.deps.get_contacts_by_name(name))

    @agent.tool
    async def get_contact_routes(
        ctx: RunContext[MeshConnection], name: str
    ) -> list[dict[str, Any]]:
        """Get the route history for a contact/node/repeater by name.

        Shows all routes seen in the last 7 days, including from
        messages and repeater advertisements. Use this when asked
        about how a node reaches us, what path/route it takes, or
        through which repeaters it connects.

        Args:
            name: Name or partial name to search for.
        """
        logger.info("Tool call: get_contact_routes(%s)", name)
        return _log_result("get_contact_routes", await ctx.deps.get_contact_routes(name))

    @agent.tool
    async def get_top_repeaters(
        ctx: RunContext[MeshConnection], limit: int = 10
    ) -> list[dict[str, Any]]:
        """Get the most frequently seen repeater prefixes with names.

        Use when asked about which repeaters are most used, most seen,
        or most popular in the mesh network.

        Args:
            limit: Max number of repeaters to return (default 10).
        """
        logger.info("Tool call: get_top_repeaters(%d)", limit)
        top = ctx.deps.stats.get_top_repeaters(limit)
        for entry in top:
            node = await ctx.deps.get_node_by_prefix(entry["prefix"])
            entry["name"] = node.get("adv_name", entry["prefix"]) if node else entry["prefix"]
        return _log_result("get_top_repeaters", top)

    @agent.tool
    async def get_route_type_stats(ctx: RunContext[MeshConnection]) -> dict[str, Any]:
        """Get route type distribution statistics.

        Returns total routes seen and breakdown by hash size
        (1-byte, 2-byte, etc). Use when asked about route types,
        network statistics, or mesh analytics.
        """
        logger.info("Tool call: get_route_type_stats")
        return _log_result("get_route_type_stats", ctx.deps.stats.get_route_types())

    @agent.tool
    async def get_pollen_levels(ctx: RunContext[MeshConnection]) -> str:
        """Fetch current pollen levels for Madrid from Clinica Subiza.

        Returns structured pollen data with levels and risk classification.
        Use this when asked about pollen, polen, allergies, or air quality.
        """
        logger.info("Tool call: get_pollen_levels")
        return _log_result("get_pollen_levels", await fetch_pollen_data())

    @agent.tool
    async def search_messages(
        ctx: RunContext[MeshConnection], query: str
    ) -> list[dict[str, Any]]:
        """Search stored channel messages by keyword.

        Use when someone asks what was discussed, who said something,
        or when a topic was mentioned. Searches across all channels.

        Args:
            query: Keywords to search for (e.g. "antenna", "noise floor").
        """
        logger.info("Tool call: search_messages(%s)", query)
        return _log_result("search_messages", ctx.deps.message_store.search(query, limit=10))

    @agent.tool
    async def search_messages_by_sender(
        ctx: RunContext[MeshConnection], sender: str
    ) -> list[dict[str, Any]]:
        """Search stored messages from a specific sender/person.

        Use when someone asks what a particular person said or discussed.

        Args:
            sender: Name or partial name of the sender.
        """
        logger.info("Tool call: search_messages_by_sender(%s)", sender)
        return _log_result("search_by_sender", ctx.deps.message_store.search_by_sender(sender, limit=10))

    @agent.tool
    async def get_message_stats(ctx: RunContext[MeshConnection]) -> dict[str, Any]:
        """Get message storage statistics.

        Returns total messages stored, messages per channel, and date range.
        Use when asked about message volume or channel activity.
        """
        logger.info("Tool call: get_message_stats")
        return _log_result("get_message_stats", ctx.deps.message_store.get_stats())

    @agent.tool
    async def traceroute(
        ctx: RunContext[MeshConnection], path: str
    ) -> dict[str, Any]:
        """Trace a route from get_contact_routes and measure SNR per hop.

        Takes a route EXACTLY as returned by get_contact_routes() (e.g.
        "ceba->ed97"). Automatically reverses it to trace from bot outward
        and calculates the round-trip. Do NOT reverse the path yourself.

        Args:
            path: Route from get_contact_routes, e.g. "ceba->ed97"
        """
        logger.info("Tool call: traceroute(%s)", path)
        return _log_result("traceroute", await ctx.deps.traceroute(path, reverse=True))

    @agent.tool
    async def trace_explicit(
        ctx: RunContext[MeshConnection], path: str
    ) -> dict[str, Any]:
        """Trace an explicit route in the exact order given.

        Use when the user specifies the exact outbound path from bot
        (closest hop first). Does NOT reverse the path.

        Args:
            path: Outbound path from bot, e.g. "ed97,ceba" (ed97 closest)
        """
        logger.info("Tool call: trace_explicit(%s)", path)
        return _log_result("trace_explicit", await ctx.deps.traceroute(path, reverse=False))

    return agent
