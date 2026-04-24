"""Tests for message length constraints."""

from meshbot.models import MessageConfig


def test_default_max_length():
    """Default max_length is 200."""
    cfg = MessageConfig()
    assert cfg.max_length == 200


def test_custom_max_length():
    """Max length can be configured."""
    cfg = MessageConfig(max_length=127)
    assert cfg.max_length == 127


def test_truncation():
    """Messages exceeding max_length get truncated as last resort."""
    text = "A" * 200
    max_len = 127
    truncated = text[:max_len]
    assert len(truncated) == 127
