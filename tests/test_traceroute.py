"""Tests for traceroute path calculation and result formatting."""

from meshbot.bot.commands import format_trace


def test_roundtrip_calculation():
    """Route history (far→close) is reversed and expanded to round-trip."""
    # Input from route history: df is farthest, ed is closest to bot
    hops_raw = ["df", "d2", "ed"]
    hops = list(reversed(hops_raw))  # → ed, d2, df (close→far)
    roundtrip = hops + list(reversed(hops))[1:]
    assert roundtrip == ["ed", "d2", "df", "d2", "ed"]


def test_roundtrip_single_hop():
    """Single hop round-trip."""
    hops_raw = ["ed"]
    hops = list(reversed(hops_raw))
    roundtrip = hops + list(reversed(hops))[1:]
    assert roundtrip == ["ed"]


def test_roundtrip_two_hops():
    """Two hop round-trip (route history: ceba far, ed97 close)."""
    hops_raw = ["ceba", "ed97"]
    hops = list(reversed(hops_raw))  # → ed97, ceba
    roundtrip = hops + list(reversed(hops))[1:]
    assert roundtrip == ["ed97", "ceba", "ed97"]


def test_path_normalization():
    """Arrow format is converted to comma format."""
    path = "ed->d2->df"
    normalized = path.replace("->", ",").replace(" ", "")
    hops = [h.strip() for h in normalized.split(",") if h.strip()]
    assert hops == ["ed", "d2", "df"]


def test_path_normalization_spaces():
    """Spaces around prefixes are stripped."""
    path = "ed , d2 , df"
    normalized = path.replace("->", ",").replace(" ", "")
    hops = [h.strip() for h in normalized.split(",") if h.strip()]
    assert hops == ["ed", "d2", "df"]


def test_format_trace_single_line():
    """Trace result fits in one line."""
    result = {
        "outbound": [
            {"prefix": "ed", "name": "ed", "snr": 12.5},
            {"prefix": "d2", "name": "d2", "snr": 8.0},
        ],
        "return": [
            {"prefix": "ed", "name": "ed", "snr": 11.0},
            {"prefix": "local", "name": "local", "snr": 9.0},
        ],
        "error": None,
    }
    out = format_trace(result, 200)
    assert "Ida:" in out
    assert "Vuelta:" in out
    assert "12.5" in out
    assert "|" in out  # single line


def test_format_trace_split():
    """Long trace splits into two messages."""
    result = {
        "outbound": [
            {"prefix": "ed97", "name": "ed97", "snr": 12.5},
            {"prefix": "d259", "name": "d259", "snr": 8.0},
            {"prefix": "ceba", "name": "ceba", "snr": 5.0},
        ],
        "return": [
            {"prefix": "d259", "name": "d259", "snr": 7.5},
            {"prefix": "ed97", "name": "ed97", "snr": 11.0},
            {"prefix": "local", "name": "local", "snr": 9.0},
        ],
        "error": None,
    }
    out = format_trace(result, 50)  # force split
    assert "\n" in out
    lines = out.split("\n")
    assert "Ida:" in lines[0]
    assert "Vuelta:" in lines[1]


def test_format_trace_error():
    """Error result."""
    result = {"outbound": [], "return": [], "error": "timeout"}
    # The command handler checks error before calling format_trace,
    # but format_trace with empty legs should still work
    out = format_trace(result, 200)
    assert out == ""


def test_format_trace_outbound_only():
    """Only outbound data (no return yet)."""
    result = {
        "outbound": [{"prefix": "ed", "name": "ed", "snr": 10.0}],
        "return": [],
        "error": None,
    }
    out = format_trace(result, 200)
    assert "Ida:" in out
    assert "Vuelta" not in out
