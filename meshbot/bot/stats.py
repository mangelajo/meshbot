"""Route statistics: repeater frequency and route type histograms."""

import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any

from meshbot.models import MeshMessage, split_path_prefixes

logger = logging.getLogger("meshbot.stats")

STATS_FILE = "route_stats.json"


class RouteStats:
    """Track repeater frequency and route type histograms, persisted to disk."""

    def __init__(self) -> None:
        # How often each repeater prefix appears in routes
        self.repeater_counts: Counter[str] = Counter()
        # How often each route type (by hash_size) is seen
        self.route_type_counts: Counter[str] = Counter()
        # Total routes recorded
        self.total_routes: int = 0
        self._load()

    def _load(self) -> None:
        path = Path(STATS_FILE)
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
            self.repeater_counts = Counter(data.get("repeaters", {}))
            self.route_type_counts = Counter(data.get("route_types", {}))
            self.total_routes = data.get("total_routes", 0)
            logger.info(
                "Loaded stats: %d repeaters, %d routes",
                len(self.repeater_counts), self.total_routes,
            )
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load %s: %s", STATS_FILE, e)

    def _save(self) -> None:
        try:
            data = {
                "repeaters": dict(self.repeater_counts),
                "route_types": dict(self.route_type_counts),
                "total_routes": self.total_routes,
            }
            Path(STATS_FILE).write_text(json.dumps(data, indent=2))
        except OSError as e:
            logger.warning("Failed to save %s: %s", STATS_FILE, e)

    def record(self, msg: MeshMessage) -> None:
        """Record a message's route in the statistics."""
        if msg.path_len == 0 or not msg.path:
            return

        self.total_routes += 1

        # Route type by hash size
        type_label = f"{msg.path_hash_size}-byte"
        self.route_type_counts[type_label] += 1

        # Count each repeater prefix in the path
        prefixes = split_path_prefixes(msg.path, msg.path_hash_size)
        for prefix in prefixes:
            self.repeater_counts[prefix] += 1

        self._save()

    def get_top_repeaters(self, limit: int = 10) -> list[dict[str, Any]]:
        """Return the most frequently seen repeater prefixes."""
        return [
            {"prefix": prefix, "count": count}
            for prefix, count in self.repeater_counts.most_common(limit)
        ]

    def get_route_types(self) -> dict[str, Any]:
        """Return route type distribution."""
        return {
            "total_routes": self.total_routes,
            "types": dict(self.route_type_counts),
        }

    def format_summary(self, max_length: int) -> str:
        """Format a concise stats summary for the channel."""
        if self.total_routes == 0:
            return "No routes recorded yet"

        top = self.repeater_counts.most_common(5)
        types = dict(self.route_type_counts)

        # Build type summary
        type_parts = [f"{k}:{v}" for k, v in sorted(types.items())]
        type_str = " ".join(type_parts)

        # Build repeater summary
        rep_parts = [f"{prefix}:{count}" for prefix, count in top]
        rep_str = " ".join(rep_parts)

        result = f"Routes:{self.total_routes} Types:{type_str} Top:{rep_str}"
        if len(result) <= max_length:
            return result

        # Shorter: just top 3
        rep_parts = [f"{prefix}:{count}" for prefix, count in top[:3]]
        return f"Routes:{self.total_routes} Top:{' '.join(rep_parts)}"
