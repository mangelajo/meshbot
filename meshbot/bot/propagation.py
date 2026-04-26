"""HF propagation summary from HamQSL (N0NBH).

Pulls the public XML feed at hamqsl.com, extracts solar indices and the
day/night band conditions, and renders a compact one-message summary.

If a location is supplied (or inferred from config), the summary picks
the day or night band conditions appropriate to local time at that
location, instead of dumping both.
"""

import logging
import math
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import httpx

from meshbot.bot.geocode import geocode

logger = logging.getLogger("meshbot.propagation")

HAMQSL_URL = "https://www.hamqsl.com/solarxml.php"
HTTP_TIMEOUT = 10.0

# Map verbose HamQSL band condition values to one-letter codes for the
# tight mesh format.
_COND = {"Good": "G", "Fair": "F", "Poor": "P"}


def _solar_altitude_deg(lat_deg: float, lon_deg: float, ts: float) -> float:
    """Quick solar altitude approximation (degrees). Positive = sun above
    horizon. Good enough to pick day vs night."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    day_of_year = dt.timetuple().tm_yday
    decl_deg = 23.45 * math.sin(math.radians(360 / 365.25 * (day_of_year - 81)))
    utc_hours = dt.hour + dt.minute / 60 + dt.second / 3600
    hour_angle_deg = (utc_hours - 12) * 15 + lon_deg
    lat = math.radians(lat_deg)
    decl = math.radians(decl_deg)
    ha = math.radians(hour_angle_deg)
    sin_alt = math.sin(lat) * math.sin(decl) + math.cos(lat) * math.cos(decl) * math.cos(ha)
    return math.degrees(math.asin(max(-1.0, min(1.0, sin_alt))))


async def fetch_propagation(location: str = "") -> str:
    """Fetch current HF propagation summary, optionally tailored to local
    day/night at the given location."""
    is_day: bool | None = None
    place_label = ""
    if location.strip():
        place = await geocode(location)
        if place is not None:
            alt = _solar_altitude_deg(place.latitude, place.longitude, time.time())
            is_day = alt > 0
            place_label = place.name

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(HAMQSL_URL)
            resp.raise_for_status()
            xml = resp.text
    except (httpx.HTTPError, ValueError) as e:
        logger.warning("HamQSL fetch failed: %s", e)
        return "Error obteniendo propagación"

    try:
        return _format_propagation(xml, is_day=is_day, place_label=place_label)
    except (ET.ParseError, ValueError, KeyError) as e:
        logger.warning("HamQSL parse failed: %s", e)
        return "Error parseando propagación"


def _format_propagation(xml: str, is_day: bool | None, place_label: str) -> str:
    root = ET.fromstring(xml)
    sd = root.find("solardata")
    if sd is None:
        return "Datos de propagación no disponibles"

    def _txt(tag: str, default: str = "") -> str:
        node = sd.find(tag)
        return (node.text or "").strip() if node is not None else default

    sfi = _txt("solarflux")
    ssn = _txt("sunspots")
    k = _txt("kindex")
    a = _txt("aindex")
    geomag = _txt("geomagfield") or "?"
    aurora = _txt("aurora")
    muf = _txt("muf") or "?"
    xray = _txt("xray")

    bands: dict[tuple[str, str], str] = {}  # (band, day/night) -> condition
    cc = sd.find("calculatedconditions")
    if cc is not None:
        for b in cc.findall("band"):
            name = (b.get("name") or "").strip()
            tm = (b.get("time") or "").strip().lower()
            cond = (b.text or "").strip()
            bands[(name, tm)] = cond

    # Pick which slice of the band table to show
    if is_day is None:
        slot = "day"  # default to day if no location
        slot_icon = ""
    else:
        slot = "day" if is_day else "night"
        slot_icon = "☀️" if is_day else "🌙"

    band_lines = []
    for band_name in ("80m-40m", "30m-20m", "17m-15m", "12m-10m"):
        cond = bands.get((band_name, slot), "?")
        # Strip trailing 'm' from labels for compactness: "80-40 G"
        short = band_name.replace("m", "")
        band_lines.append(f"{short} {_COND.get(cond, '?')}")

    header_bits = [f"SFI{sfi}", f"SSN{ssn}", f"K{k}", f"A{a}"]
    if geomag and geomag != "?":
        header_bits.append(geomag.split()[-1].title())
    if place_label:
        header_bits.append(slot_icon)
    elif slot_icon:
        header_bits.append(slot_icon)
    header = " ".join(b for b in header_bits if b)

    # Two band entries per line keeps each line short
    line2 = f"{band_lines[0]} {band_lines[1]}"
    line3 = f"{band_lines[2]} {band_lines[3]}"
    footer_bits = []
    if aurora:
        footer_bits.append(f"Aur{aurora.strip()}")
    if muf and muf != "NoRpt":
        footer_bits.append(f"MUF{muf}")
    if xray:
        footer_bits.append(xray)
    footer = " ".join(footer_bits)

    parts = [header, line2, line3]
    if footer:
        parts.append(footer)
    return "\n".join(parts)
