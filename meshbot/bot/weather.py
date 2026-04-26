"""Weather lookup using the Open-Meteo public APIs.

Open-Meteo exposes two free endpoints we use:
- geocoding: name -> {lat, lon, name, country_code, admin1}
- forecast: lat,lon -> current observations + daily summary

Both are public, no API key, generous rate limits.
"""

import logging
from typing import Any

import httpx

logger = logging.getLogger("meshbot.weather")

GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HTTP_TIMEOUT = 10.0

# WMO weather code -> (emoji, short Spanish label)
_WMO: dict[int, tuple[str, str]] = {
    0: ("☀️", "Despejado"),
    1: ("🌤", "Casi despejado"),
    2: ("⛅", "Parcialm. nublado"),
    3: ("☁️", "Nublado"),
    45: ("🌫", "Niebla"),
    48: ("🌫", "Niebla helada"),
    51: ("🌧", "Llovizna ligera"),
    53: ("🌧", "Llovizna"),
    55: ("🌧", "Llovizna fuerte"),
    56: ("🌧", "Llovizna helada"),
    57: ("🌧", "Llovizna helada"),
    61: ("🌧", "Lluvia ligera"),
    63: ("🌧", "Lluvia"),
    65: ("🌧", "Lluvia fuerte"),
    66: ("🌧", "Lluvia helada"),
    67: ("🌧", "Lluvia helada"),
    71: ("🌨", "Nieve ligera"),
    73: ("🌨", "Nieve"),
    75: ("🌨", "Nieve fuerte"),
    77: ("🌨", "Granizo"),
    80: ("🌧", "Chubascos"),
    81: ("🌧", "Chubascos"),
    82: ("🌧", "Chubascos fuertes"),
    85: ("🌨", "Chubascos nieve"),
    86: ("🌨", "Chubascos nieve"),
    95: ("⛈", "Tormenta"),
    96: ("⛈", "Torm. granizo"),
    99: ("⛈", "Torm. granizo"),
}

_DIRS = [
    (0, "N", "⬆"), (45, "NE", "↗"), (90, "E", "➡"), (135, "SE", "↘"),
    (180, "S", "⬇"), (225, "SW", "↙"), (270, "W", "⬅"), (315, "NW", "↖"),
]


def _wind_dir(deg: float) -> tuple[str, str]:
    """Return (cardinal, arrow_emoji) for a wind direction in degrees."""
    deg = deg % 360
    best = min(_DIRS, key=lambda d: min(abs(deg - d[0]), 360 - abs(deg - d[0])))
    return best[1], best[2]


async def fetch_weather(location: str) -> str:
    """Fetch current weather for a place name and return a short report.

    The string is shaped to fit in a single mesh packet for typical
    settings: place + condition icon + temp + wind + RH + dewpoint +
    pressure + today's max/min.
    """
    location = location.strip()
    if not location:
        return "Falta el nombre del sitio (p.ej. 'Madrid')"

    # Open-Meteo's geocoder takes only a city name. Accept "Madrid, Spain"
    # or "Madrid, ES" by splitting and filtering the result list by country.
    parts = [p.strip() for p in location.split(",", 1)]
    name_q = parts[0]
    country_q = parts[1].lower() if len(parts) > 1 else ""

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        try:
            geo_resp = await client.get(
                GEOCODE_URL,
                params={"name": name_q, "count": 10, "format": "json"},
            )
            geo_resp.raise_for_status()
            geo = geo_resp.json()
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("geocoding failed for %s: %s", location, e)
            return f"No pude geocodificar '{location}'"

        results = geo.get("results") or []
        if country_q:
            results = [
                r for r in results
                if country_q in (r.get("country", "").lower(), r.get("country_code", "").lower())
            ]
        if not results:
            return f"No encontré '{location}'"
        place = results[0]
        lat, lon = place["latitude"], place["longitude"]
        name = place.get("name", location)
        country = place.get("country_code", "")

        try:
            fc_resp = await client.get(
                FORECAST_URL,
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "current": (
                        "temperature_2m,relative_humidity_2m,dew_point_2m,"
                        "weather_code,wind_speed_10m,wind_direction_10m,"
                        "wind_gusts_10m,surface_pressure"
                    ),
                    "daily": "temperature_2m_max,temperature_2m_min",
                    "timezone": "auto",
                    "forecast_days": 1,
                    "wind_speed_unit": "kmh",
                },
            )
            fc_resp.raise_for_status()
            fc = fc_resp.json()
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("forecast failed for %s: %s", location, e)
            return f"Error tiempo {name}"

    return _format_weather(name, country, fc)


def _format_weather(name: str, country: str, fc: dict[str, Any]) -> str:
    cur = fc.get("current") or {}
    daily = fc.get("daily") or {}

    temp = cur.get("temperature_2m")
    rh = cur.get("relative_humidity_2m")
    dew = cur.get("dew_point_2m")
    wcode = int(cur.get("weather_code") or 0)
    wspd = cur.get("wind_speed_10m")
    wdir = cur.get("wind_direction_10m")
    wgust = cur.get("wind_gusts_10m")
    pres = cur.get("surface_pressure")

    tmax_arr = daily.get("temperature_2m_max") or []
    tmin_arr = daily.get("temperature_2m_min") or []
    tmax = tmax_arr[0] if tmax_arr else None
    tmin = tmin_arr[0] if tmin_arr else None

    icon, cond = _WMO.get(wcode, ("", "?"))
    parts = [f"{name},{country}: {icon}{cond}"]
    if temp is not None:
        parts.append(f"{round(temp)}°C")
    if wdir is not None and wspd is not None:
        cardinal, arrow = _wind_dir(wdir)
        wind = f"{arrow}{cardinal}{round(wspd)}"
        if wgust is not None and round(wgust) > round(wspd):
            wind += f"G{round(wgust)}"
        parts.append(wind)
    if rh is not None:
        parts.append(f"{round(rh)}%")
    if dew is not None:
        parts.append(f"💧{round(dew)}°C")
    if pres is not None:
        parts.append(f"{round(pres)}hPa")
    if tmax is not None and tmin is not None:
        parts.append(f"↑{round(tmax)} ↓{round(tmin)}")

    return " ".join(parts)
