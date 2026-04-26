"""Tests for bot command parsing and handling."""

from meshbot.bot.commands import (
    is_command,
    pad_visual,
    parse_command,
    truncate_visual,
    visual_width,
)


def test_is_command():
    assert is_command("!ping")
    assert is_command("  !help  ")
    assert not is_command("hello")
    assert not is_command("")
    assert not is_command("not a !command")


def test_parse_command():
    assert parse_command("!ping") == ("ping", "")
    assert parse_command("!PING") == ("ping", "")
    assert parse_command("!prefix AB12") == ("prefix", "AB12")
    assert parse_command("!path AB CD EF") == ("path", "AB CD EF")
    assert parse_command("  !help  ") == ("help", "")
    assert parse_command("hello") == ("", "hello")


def test_parse_command_empty():
    cmd, args = parse_command("!")
    assert cmd == ""
    assert args == ""


def test_visual_width_ascii():
    assert visual_width("RRN-ALCO-RPT") == 12
    assert visual_width("") == 0


def test_visual_width_emoji():
    # Most emoji are East Asian Wide → 2 columns
    assert visual_width("🔋") == 2
    assert visual_width("🚫") == 2
    assert visual_width("GRN LOECHES RPT 🔋⚡") == 20


def test_visual_width_combining():
    # Precomposed accents stay 1 column; combining marks are 0
    assert visual_width("León") == 4
    assert visual_width("á") == 1  # 'a' + combining acute


def test_truncate_visual_no_change_when_fits():
    assert truncate_visual("RRN-ALCO-RPT", 14) == "RRN-ALCO-RPT"
    assert truncate_visual("EA4URG", 14) == "EA4URG"


def test_truncate_visual_trims_to_width():
    assert truncate_visual("MadMesh001 Alcobendas", 14) == "MadMesh001 A.."
    assert truncate_visual("EA4URG Alto del León", 14) == "EA4URG Alto.."


def test_truncate_visual_handles_emoji():
    # "GRN LOECHES " is 12 visual; next char would push past 12 so we stop
    assert truncate_visual("GRN LOECHES RPT 🔋⚡", 14) == "GRN LOECHES.."
    # Emoji counted as 2 columns when deciding what fits
    assert truncate_visual("MMR Alc Test 🚫", 14) == "MMR Alc Test.."


def test_pad_visual_pads_narrow():
    assert pad_visual("RRN-ALCO-RPT", 14) == "RRN-ALCO-RPT  "
    assert pad_visual("", 3) == "   "


def test_pad_visual_accounts_for_emoji():
    # "ab🔋" is visual width 4, padding to 6 should add 2 spaces
    assert pad_visual("ab🔋", 6) == "ab🔋  "


def test_pad_visual_no_change_when_already_wide_enough():
    assert pad_visual("MadMesh001 A..", 14) == "MadMesh001 A.."
