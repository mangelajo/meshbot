"""Tests for message length splitting."""

from meshbot.bot.router import _split_response


def test_short_message_unchanged():
    """Messages under max_length are returned as-is."""
    result = _split_response("hello world", 200, 3)
    assert result == "hello world"


def test_split_into_parts():
    """Long messages are split into labeled parts."""
    text = "A" * 50 + " " + "B" * 50 + " " + "C" * 50
    result = _split_response(text, 60, 3)
    lines = result.split("\n")
    assert len(lines) >= 2
    # Each line should have a part label
    for line in lines:
        assert "[" in line and "/" in line and "]" in line


def test_max_parts_respected():
    """Splitting stops at max_parts."""
    text = " ".join(["word"] * 200)
    result = _split_response(text, 30, 2)
    lines = result.split("\n")
    assert len(lines) <= 2


def test_split_word_boundary():
    """Splitting prefers word boundaries."""
    text = "the quick brown fox jumps over the lazy dog"
    result = _split_response(text, 30, 3)
    lines = result.split("\n")
    # No word should be cut in half
    for line in lines:
        # Strip the part label to check content
        content = line.rsplit("[", 1)[0].strip()
        assert not content.endswith("-")
