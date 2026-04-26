"""Tiny wrapper around Open-Meteo's free geocoding API.

Exposes `geocode(query)` which accepts plain names or "Name, Country"
and returns the first matching place, or None.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import httpx

logger = logging.getLogger("meshbot.geocode")

GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
HTTP_TIMEOUT = 10.0


@dataclass
class Place:
    name: str
    country: str
    country_code: str
    latitude: float
    longitude: float


async def geocode(query: str) -> Optional[Place]:
    query = query.strip()
    if not query:
        return None
    parts = [p.strip() for p in query.split(",", 1)]
    name_q = parts[0]
    country_q = parts[1].lower() if len(parts) > 1 else ""

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        try:
            resp = await client.get(
                GEOCODE_URL,
                params={"name": name_q, "count": 10, "format": "json"},
            )
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("geocoding failed for %s: %s", query, e)
            return None

    results = data.get("results") or []
    if country_q:
        results = [
            r for r in results
            if country_q in (r.get("country", "").lower(), r.get("country_code", "").lower())
        ]
    if not results:
        return None
    r = results[0]
    return Place(
        name=r.get("name", name_q),
        country=r.get("country", ""),
        country_code=r.get("country_code", ""),
        latitude=float(r["latitude"]),
        longitude=float(r["longitude"]),
    )
