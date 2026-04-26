"""PydanticAI agent factory with provider switching and mesh tools."""

import logging
import os
from typing import Any

from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.settings import ModelSettings

from meshbot.bot.mesh import MeshConnection
from meshbot.bot.pollen import fetch_pollen_data
from meshbot.bot.propagation import fetch_propagation
from meshbot.bot.weather import fetch_weather
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

LoRa radio: SF8 CR4/5 BW62.5kHz, suelo de demodulación ~-10 dB SNR. Por hop: \
≥0 dB excelente, -3..0 dB estable, -6..-3 dB marginal con pérdidas intermitentes, \
<-6 dB muy débil (sólo paquetes sueltos). Usa esto al interpretar SNR de traceroutes \
o al valorar la fiabilidad de un enlace.

You can answer general questions using your own knowledge.
When the question is about the mesh network, use your tools:
- Contact/node info+routes (by name or hex prefix) -> get_contact_info(query)
- Traceroute SNR -> first get_contact_info, then traceroute(path) with the most recent route in observed_routes. known_route="flood" means only that the bot has no fixed outbound path; observed inbound routes are still valid traceroute targets.
- Top repeaters -> get_top_repeaters()
- Recent adverts (with clock drift, SNR, location) -> recent_adverts(name?)
- Weather / tiempo / wx [ciudad] -> get_weather(location?) — empty location uses the configured default city
- HF propagation (SFI, Kp, band conditions) -> get_propagation(location?)
- Pollen/polen -> get_pollen_levels()
- What was discussed -> search_messages(query)
- Recent messages / activity -> recent_messages(channel)
Never invent mesh network data — always use tools for that.\
"""


def _format_trace_result(result: dict[str, Any]) -> str:
    """Format traceroute result as human-readable text."""
    if result.get("error"):
        return f"Trace error: {result['error']}"
    outbound = result.get("outbound", [])
    return_leg = result.get("return", [])

    def _fmt(hops: list[dict[str, Any]]) -> str:
        return "->".join(f"{h['name']}:{h['snr']}" for h in hops)

    parts = []
    if outbound:
        parts.append(f"Ida: {_fmt(outbound)}")
    if return_leg:
        parts.append(f"Vuelta: {_fmt(return_leg)}")
    return " | ".join(parts) if parts else "Sin datos"


def _log_result(name: str, result: Any) -> Any:
    """Log tool result and return it."""
    logger.info("Tool result %s: %s", name, result)
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
        model_settings=ModelSettings(thinking=False),
    )

    @agent.tool
    async def get_contact_info(
        ctx: RunContext[MeshConnection], query: str
    ) -> list[dict[str, Any]]:
        """Look up contact(s) by name pattern AND/OR by hex public-key prefix.

        Always returns a list of every match. Searches by name (case-insensitive
        substring) and, when the query is hex, also matches every contact whose
        public key starts with that prefix. The two result sets are merged and
        deduplicated, so a query like "dcc" finds nodes by either path.

        The field observed_routes contains the paths their
        messages took to reach us — use these for traceroute.

        Args:
            query: Name pattern or hex public-key prefix.
        """
        logger.info("Tool call: get_contact_info(%s)", query)
        results = await ctx.deps.get_contacts_by_name(query)
        if query and all(c in "0123456789abcdefABCDEF" for c in query):
            seen = {r["public_key"] for r in results}
            for entry in await ctx.deps.get_contacts_by_prefix(query):
                if entry["public_key"] not in seen:
                    results.append(entry)
                    seen.add(entry["public_key"])
        return _log_result("get_contact_info", results)

    @agent.tool
    async def get_top_repeaters(ctx: RunContext[MeshConnection]) -> list[dict[str, Any]]:
        """Get the most frequently seen repeaters in the mesh."""
        logger.info("Tool call: get_top_repeaters")
        top = ctx.deps.stats.get_top_repeaters(config.stats.repeaters_max)
        for entry in top:
            node = await ctx.deps.get_node_by_prefix(entry["prefix"])
            entry["name"] = node.get("adv_name", entry["prefix"]) if node else entry["prefix"]
        return _log_result("get_top_repeaters", top)

    @agent.tool
    async def get_pollen_levels(ctx: RunContext[MeshConnection]) -> str:
        """Fetch current pollen levels for Madrid."""
        logger.info("Tool call: get_pollen_levels")
        return _log_result("get_pollen_levels", await fetch_pollen_data())

    @agent.tool
    async def get_propagation(
        ctx: RunContext[MeshConnection], location: str = ""
    ) -> str:
        """Current HF propagation summary (SFI, K, A, geomagnetic state,
        band conditions, MUF). If location is given (or falls back to the
        configured default), the day/night band conditions are picked
        based on local time at that place. Forward the result as-is.

        Args:
            location: Optional place name, e.g. "Madrid, Spain". Empty
                falls back to config.weather_default_location.
        """
        loc = location.strip() or config.weather_default_location
        logger.info("Tool call: get_propagation(%s)", loc)
        return _log_result("get_propagation", await fetch_propagation(loc))

    @agent.tool
    async def get_weather(
        ctx: RunContext[MeshConnection], location: str = ""
    ) -> str:
        """Current weather for a place name (any city worldwide).

        Returns a single short line already formatted (place, condition,
        temp, wind, RH, dewpoint, pressure, today's high/low). Forward
        the result to the user without rephrasing — it is sized for a
        mesh packet.

        Args:
            location: Place name, e.g. "Madrid", "Alicante", "Loeches".
                Empty falls back to config.weather_default_location.
        """
        loc = location.strip() or config.weather_default_location
        logger.info("Tool call: get_weather(%s)", loc)
        return _log_result("get_weather", await fetch_weather(loc))

    @agent.tool
    async def search_messages(
        ctx: RunContext[MeshConnection], query: str
    ) -> list[dict[str, Any]]:
        """Search stored messages by keyword or sender name.

        Args:
            query: Keywords to search for.
        """
        logger.info("Tool call: search_messages(%s)", query)
        return _log_result("search_messages", ctx.deps.message_store.search(query, limit=5))

    @agent.tool
    async def recent_messages(
        ctx: RunContext[MeshConnection], channel: str = ""
    ) -> list[dict[str, Any]]:
        """Get the last messages, optionally from a specific channel.

        Args:
            channel: Channel name filter (e.g. "Public", "#b0b0t"). Empty = all.
        """
        logger.info("Tool call: recent_messages(%s)", channel or "all")
        return _log_result(
            "recent_messages",
            ctx.deps.message_store.get_recent(channel=channel or None, limit=5),
        )

    @agent.tool
    async def recent_adverts(
        ctx: RunContext[MeshConnection], name: str = ""
    ) -> list[dict[str, Any]]:
        """Get recently received advertisements, newest first.

        Each entry includes name, public_key, type, last_seen (relative),
        drift_seconds (advertised clock minus our clock), snr, rssi, and
        loc when published. drift_seconds = None means we couldn't parse
        the timestamp. Useful for spotting nodes with bad RTCs or just
        seeing who is on air.

        Args:
            name: Optional name substring filter (case-insensitive). Empty = all.
        """
        logger.info("Tool call: recent_adverts(%s)", name or "all")
        return _log_result(
            "recent_adverts",
            ctx.deps.get_recent_adverts(name_filter=name, limit=10),
        )

    @agent.tool
    async def traceroute(
        ctx: RunContext[MeshConnection], path: str
    ) -> str:
        """Trace a route and measure SNR at each hop (round-trip).

        Use the most recent entry in get_contact_info's
        observed_routes — those are the real paths their
        packets took to reach us and are valid for traceroute even when
        known_route is "flood" (which only means we don't have a fixed
        outbound path TO them, not that the inbound route is unknown).

        Forward the result EXACTLY to the user without summarizing.

        Args:
            path: Route to trace, e.g. "ceba->ed97"
        """
        logger.info("Tool call: traceroute(%s)", path)
        result = await ctx.deps.traceroute(path, reverse=True)
        return _log_result("traceroute", _format_trace_result(result))

    return agent
