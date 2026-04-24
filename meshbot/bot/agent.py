"""PydanticAI agent factory with provider switching and mesh tools."""

import logging
import os
from typing import Any

from pydantic_ai import Agent, RunContext

from meshbot.bot.mesh import MeshConnection
from meshbot.bot.pollen import fetch_pollen_data
from meshbot.models import BotConfig

logger = logging.getLogger("meshbot.agent")

SYSTEM_PROMPT_TEMPLATE = """\
You are {bot_name}, a helpful assistant on a mesh radio network.
Always respond in {language}.
Keep responses under {max_length} characters — bandwidth is extremely limited.
Be concise and direct. No markdown formatting. Plain text only.
Only respond to the LAST message marked with [From ...]. \
The channel log above it is background context only — do NOT respond to those messages.
If the message does not need a response (greetings between others, reactions, \
emojis, acknowledgements like "ok", "👍", "lol"), reply with exactly: NO_RESPONSE

You can answer general questions using your own knowledge.
When the question is about the mesh network, use your tools:
- Contact/node/person on the mesh -> call get_contact_info(name).
- Pollen/polen/allergies -> call get_pollen_levels().
- Node by hex prefix -> call get_node_by_prefix(prefix).
Never invent mesh network data — always use tools for that.\
"""


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
    async def get_node_by_prefix(
        ctx: RunContext[MeshConnection], prefix: str
    ) -> dict[str, Any] | None:
        """Look up a mesh node by its public key hex prefix and return its info.

        Args:
            prefix: Hex string prefix of the node's public key (e.g. "d2", "ab3f").
        """
        logger.debug("Tool call: get_node_by_prefix(%s)", prefix)
        result = await ctx.deps.get_node_by_prefix(prefix)
        if result is None:
            return None
        # Return a clean subset so the model gets useful info
        return {
            "name": result.get("adv_name", ""),
            "public_key": result.get("public_key", "")[:12],
            "type": result.get("type"),
            "hops": result.get("out_path_len"),
            "path": result.get("out_path", ""),
        }

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
        logger.debug("Tool call: get_contact_info(%s)", name)
        return await ctx.deps.get_contacts_by_name(name)

    @agent.tool
    async def get_pollen_levels(ctx: RunContext[MeshConnection]) -> str:
        """Fetch current pollen levels for Madrid from Clinica Subiza.

        Returns structured pollen data with levels and risk classification.
        Use this when asked about pollen, polen, allergies, or air quality.
        """
        logger.debug("Tool call: get_pollen_levels")
        return await fetch_pollen_data()

    return agent
