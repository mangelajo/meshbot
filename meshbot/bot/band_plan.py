"""IARU amateur radio band plans (HF + VHF/UHF).

Currently encodes Region 1 (Europe / Africa / Middle East) since that's
where the bot is deployed. Region 2 / 3 can be filled in later — the
data structure is the same.

The data is intentionally a flat list of segments per band; the agent
slices and rephrases as needed. Numbers are kHz throughout.
"""

from typing import Any

# Each band: (low_kHz, high_kHz, [(segment_low, segment_high, use), ...])
_BAND_PLANS: dict[int, dict[str, tuple[int, int, list[tuple[int, int, str]]]]] = {
    1: {  # IARU Region 1
        "160m": (1810, 2000, [
            (1810, 1838, "CW"),
            (1838, 1840, "Narrow band modes"),
            (1840, 1843, "SSB digital (FT8/WSJT)"),
            (1843, 2000, "SSB"),
        ]),
        "80m": (3500, 3800, [
            (3500, 3510, "CW priority"),
            (3510, 3560, "CW, contests"),
            (3560, 3580, "CW"),
            (3580, 3600, "Narrow band data"),
            (3600, 3650, "ALL modes"),
            (3650, 3700, "ALL modes (SSB priority)"),
            (3700, 3775, "SSB"),
            (3775, 3800, "SSB, contests"),
        ]),
        "60m": (5351, 5367, [
            (5351, 5354, "CW"),
            (5354, 5366, "ALL modes (SSB)"),
            (5366, 5367, "Narrow band"),
        ]),
        "40m": (7000, 7200, [
            (7000, 7040, "CW"),
            (7040, 7050, "Narrow band data (digi)"),
            (7050, 7053, "Narrow band data"),
            (7053, 7060, "ALL modes"),
            (7060, 7100, "SSB, ALL modes"),
            (7100, 7200, "SSB"),
        ]),
        "30m": (10100, 10150, [
            (10100, 10130, "CW"),
            (10130, 10150, "Narrow band data"),
        ]),
        "20m": (14000, 14350, [
            (14000, 14070, "CW"),
            (14070, 14099, "Narrow band data (FT8 14074)"),
            (14099, 14101, "IBP beacons"),
            (14101, 14112, "Narrow band data"),
            (14112, 14125, "ALL modes"),
            (14125, 14300, "SSB"),
            (14300, 14350, "SSB"),
        ]),
        "17m": (18068, 18168, [
            (18068, 18095, "CW"),
            (18095, 18105, "Narrow band data"),
            (18105, 18109, "Narrow band data"),
            (18109, 18111, "IBP beacons"),
            (18111, 18168, "SSB, ALL modes"),
        ]),
        "15m": (21000, 21450, [
            (21000, 21070, "CW"),
            (21070, 21100, "Narrow band data"),
            (21100, 21149, "Narrow band data"),
            (21149, 21151, "IBP beacons"),
            (21151, 21450, "SSB"),
        ]),
        "12m": (24890, 24990, [
            (24890, 24915, "CW"),
            (24915, 24929, "Narrow band data"),
            (24929, 24931, "IBP beacons"),
            (24931, 24990, "SSB, ALL modes"),
        ]),
        "10m": (28000, 29700, [
            (28000, 28070, "CW"),
            (28070, 28190, "Narrow band data (FT8 28074)"),
            (28190, 28225, "Beacons"),
            (28225, 28300, "ALL modes"),
            (28300, 29200, "SSB"),
            (29200, 29300, "NBFM"),
            (29300, 29510, "Satellite"),
            (29510, 29700, "NBFM"),
        ]),
        "6m": (50000, 52000, [
            (50000, 50100, "Beacons, CW"),
            (50100, 50500, "SSB, CW"),
            (50500, 52000, "ALL modes (FM, digital)"),
        ]),
        "4m": (70000, 70500, [
            (70000, 70300, "ALL modes (where allocated)"),
            (70300, 70500, "FM, digital"),
        ]),
        "2m": (144000, 146000, [
            (144000, 144025, "EME, CW"),
            (144025, 144110, "CW"),
            (144110, 144150, "CW, MGM (digital)"),
            (144150, 144400, "SSB"),
            (144400, 144490, "Beacons"),
            (144500, 144800, "ALL modes"),
            (144800, 145000, "Digital (APRS 144.800)"),
            (145000, 145800, "FM repeaters / voice"),
            (145800, 146000, "Satellite"),
        ]),
        "70cm": (430000, 440000, [
            (430000, 432000, "Repeater outputs / ALL modes"),
            (432000, 432500, "CW, SSB"),
            (432500, 432800, "ALL modes"),
            (432800, 433000, "Beacons"),
            (433000, 433400, "FM repeater outputs"),
            (433400, 434600, "FM simplex / mixed"),
            (434600, 435000, "FM repeater inputs"),
            (435000, 438000, "Satellite"),
            (438000, 440000, "ATV, repeater outputs, ALL modes"),
        ]),
    },
}

_ALIASES = {
    "160": "160m", "80": "80m", "60": "60m", "40": "40m", "30": "30m",
    "20": "20m", "17": "17m", "15": "15m", "12": "12m", "10": "10m",
    "6": "6m", "4": "4m", "2": "2m", "70": "70cm",
}


def _normalize_band(band: str) -> str:
    """Resolve "20", "20 m", "20 metros" -> "20m"."""
    s = band.strip().lower()
    s = s.replace(" ", "").replace("metros", "m").replace("meter", "m").replace("cms", "cm")
    s = s.rstrip(".")
    if s in _ALIASES:
        return _ALIASES[s]
    return s


def get_band_plan(band: str, region: int = 1) -> dict[str, Any]:
    """Return the IARU band plan for a band id and region.

    The result has fields: band, region, khz_range, segments. On miss,
    returns {"error": ..., "available": [...]} so the agent can offer
    suggestions.
    """
    region_plans = _BAND_PLANS.get(region)
    if not region_plans:
        return {"error": f"IARU R{region} no soportada (solo R1 por ahora)",
                "supported_regions": list(_BAND_PLANS.keys())}

    band_norm = _normalize_band(band)
    plan = region_plans.get(band_norm)
    if not plan:
        return {"error": f"Banda '{band}' no encontrada",
                "available": sorted(region_plans.keys())}

    low, high, segments = plan
    return {
        "band": band_norm,
        "region": f"IARU R{region}",
        "khz_range": [low, high],
        "segments": [
            {"khz": f"{s[0]}-{s[1]}", "use": s[2]} for s in segments
        ],
    }
