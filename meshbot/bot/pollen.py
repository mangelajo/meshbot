"""Pollen data fetcher from Clínica Subiza (sigueros.es)."""

import logging
import re
from datetime import date, timedelta
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlencode

import httpx

logger = logging.getLogger("meshbot.pollen")

POLLEN_TYPES = [
    "CUPRE", "PALMA", "RUMEX", "MERCU", "MORUS", "URTIC", "ALNUS", "BETUL",
    "CAREX", "FRAXI", "QUERC", "OLEA", "PINUS", "ULMUS", "CASTA", "POPUL",
    "GRAMI", "QUEAM", "PLATA", "PLANT", "ARTEM", "ALTER",
]

BASE_URL = "https://sigueros.es/chart"

# Risk thresholds: (low_max, moderate_max, moderate_high_max)
# Values above moderate_high_max are "high"
THRESHOLDS: dict[str, tuple[int, int, int]] = {
    "Cupresáceas": (50, 92, 135),
    "Plátano": (50, 90, 130),
    "Platanus": (50, 90, 130),
    "Olivo": (100, 150, 200),
    "Olea": (100, 150, 200),
    "Fresno": (100, 150, 200),
    "Gramíneas": (10, 30, 50),
    "Plantago": (10, 30, 50),
    "Queno-Amaran": (10, 15, 20),
    "Urticáceas": (10, 15, 20),
    "Quercus": (50, 92, 135),
}

# Default threshold for types not explicitly listed
DEFAULT_THRESHOLD = (20, 50, 100)


def _build_url() -> str:
    """Build the pollen chart URL with current week dates."""
    today = date.today()
    from_date = today - timedelta(days=7)
    params = [("pollen[]", p) for p in POLLEN_TYPES]
    params.extend([
        ("symptoms", "true"),
        ("medication", "true"),
        ("from", from_date.isoformat()),
        ("to", today.isoformat()),
    ])
    return f"{BASE_URL}?{urlencode(params)}"


class _PollenTableParser(HTMLParser):
    """Parse the 'Recuentos de la Última Semana' table from HTML."""

    def __init__(self) -> None:
        super().__init__()
        self._in_tbody = False
        self._in_row = False
        self._in_cell = False
        self._in_span = False
        self._current_row: list[str] = []
        self._cell_text = ""
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tbody":
            self._in_tbody = True
        elif self._in_tbody and tag == "tr":
            self._in_row = True
            self._current_row = []
        elif self._in_row and tag == "td":
            self._in_cell = True
            self._cell_text = ""
        elif self._in_cell and tag == "span":
            self._in_span = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "tbody":
            self._in_tbody = False
        elif tag == "tr" and self._in_row:
            self._in_row = False
            if self._current_row:
                self.rows.append(self._current_row)
        elif tag == "td" and self._in_cell:
            self._in_cell = False
            self._current_row.append(self._cell_text.strip())
            self._cell_text = ""
        elif tag == "span" and self._in_span:
            self._in_span = False

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_text += data


def _classify(name: str, value: int) -> str:
    """Classify a pollen count as low/moderate/high."""
    thresholds = THRESHOLDS.get(name, DEFAULT_THRESHOLD)
    if value <= thresholds[0]:
        return "low"
    elif value <= thresholds[1]:
        return "mod"
    elif value <= thresholds[2]:
        return "mod-high"
    return "HIGH"


def _parse_table(html: str) -> list[dict[str, Any]]:
    """Parse pollen table from HTML, return list of {name, latest_value, level}."""
    parser = _PollenTableParser()
    parser.feed(html)

    results = []
    for row in parser.rows:
        if len(row) < 2:
            continue
        name = row[0]
        # Find the latest non-empty value (rightmost column)
        latest = 0
        for cell in reversed(row[1:]):
            cell = cell.strip()
            if cell and cell != "—":
                match = re.search(r"\d+", cell)
                if match:
                    latest = int(match.group())
                    break

        if latest > 0:
            level = _classify(name, latest)
            results.append({"name": name, "value": latest, "level": level})

    return results


_LEVEL_LABEL = {
    "HIGH": "MUY ALTO",
    "mod-high": "alto",
    "mod": "moderado",
    "low": "bajo",
}

_ALLERGY_TIPS = {
    "Quercus": "encina/roble",
    "Gramíneas": "cesped/hierba",
    "Olea": "olivo",
    "Olivo": "olivo",
    "Plantago": "llanten",
    "Cupresáceas": "cipres/arizonica",
    "Platanus": "platano de sombra",
    "Plátano": "platano de sombra",
    "Urticáceas": "ortiga/parietaria",
}


async def fetch_pollen_data() -> str:
    """Fetch current pollen data and return structured text for the agent to summarize."""
    url = _build_url()
    logger.debug("Fetching pollen data: %s", url)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("Failed to fetch pollen data: %s", e)
        return "Error fetching pollen data"

    results = _parse_table(resp.text)
    if not results:
        return "No pollen data available"

    # Sort by severity: HIGH first, then by value
    severity_order = {"HIGH": 0, "mod-high": 1, "mod": 2, "low": 3}
    results.sort(key=lambda r: (severity_order.get(r["level"], 9), -r["value"]))

    lines: list[str] = []
    for r in results:
        if r["level"] == "low":
            continue
        tip = _ALLERGY_TIPS.get(r["name"], "")
        name = f"{r['name']} ({tip})" if tip else r["name"]
        label = _LEVEL_LABEL[r["level"]]
        lines.append(f"- {name}: {r['value']} granos/m3 [{label}]")

    if not lines:
        return "All pollen levels are low today in Madrid."

    return "Madrid pollen levels today:\n" + "\n".join(lines)
