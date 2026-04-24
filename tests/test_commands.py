"""Tests for bot command parsing and handling."""

from meshbot.bot.commands import is_command, parse_command


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
