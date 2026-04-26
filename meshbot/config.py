"""Configuration loading: YAML file + CLI overrides."""

from pathlib import Path
from typing import Any

import yaml

from meshbot.models import BotConfig, MessageConfig, StatsConfig


def load_config(config_path: str | Path | None = None, **overrides: Any) -> BotConfig:
    """Load config from YAML file, then apply CLI overrides.

    Precedence: defaults < YAML < overrides (CLI args).
    None values in overrides are ignored.
    """
    data: dict[str, Any] = {}

    if config_path is not None:
        path = Path(config_path)
        if path.exists():
            with open(path) as f:
                data = yaml.safe_load(f) or {}

    # Apply non-None overrides
    for key, value in overrides.items():
        if value is not None:
            data[key] = value

    return _build_config(data)


def _build_config(data: dict[str, Any]) -> BotConfig:
    """Build a BotConfig from a flat/nested dict."""
    msg_data = data.pop("message", None)
    message = MessageConfig()
    if isinstance(msg_data, dict):
        if "max_length" in msg_data:
            message.max_length = int(msg_data["max_length"])
        if "max_parts" in msg_data:
            message.max_parts = int(msg_data["max_parts"])

    stats_data = data.pop("stats", None)
    stats = StatsConfig()
    if isinstance(stats_data, dict):
        if "repeaters_max" in stats_data:
            stats.repeaters_max = int(stats_data["repeaters_max"])
        if "exclude_prefixes" in stats_data:
            raw = stats_data["exclude_prefixes"]
            if isinstance(raw, list):
                stats.exclude_prefixes = [str(p).lower() for p in raw]

    # Filter to only valid BotConfig fields
    valid_fields = (
        {f.name for f in BotConfig.__dataclass_fields__.values()} - {"message", "stats"}
    )
    filtered = {k: v for k, v in data.items() if k in valid_fields}

    return BotConfig(message=message, stats=stats, **filtered)
