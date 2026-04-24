"""Tests for message routing logic."""

from meshbot.bot.router import _looks_like_command, should_process, strip_mention
from meshbot.models import BotConfig, MeshMessage


def _make_msg(text: str) -> MeshMessage:
    return MeshMessage(text=text, sender="test", channel_idx=0, path_len=0, sender_timestamp=0)


def _make_config(**kwargs) -> BotConfig:
    return BotConfig(**kwargs)


def test_should_process_command_always():
    """Commands are always processed regardless of trigger mode."""
    config = _make_config(trigger_mode="mention", bot_name="b0b0t")
    assert should_process(_make_msg("!ping"), config)
    assert should_process(_make_msg("!help"), config)


def test_should_process_all_mode():
    """In 'all' mode, every message is processed."""
    config = _make_config(trigger_mode="all", bot_name="b0b0t")
    assert should_process(_make_msg("hello"), config)
    assert should_process(_make_msg("random text"), config)


def test_should_process_skip_other_mentions():
    """Messages mentioning another user with @ should be skipped."""
    config = _make_config(trigger_mode="all", bot_name="b0b0t")
    assert not should_process(_make_msg("@[otherbot] hello"), config)
    assert not should_process(_make_msg("@someone hi"), config)
    # But mentioning the bot itself is fine
    assert should_process(_make_msg("@[b0b0t] hello"), config)
    assert should_process(_make_msg("@b0b0t hello"), config)


def test_should_process_mention_mode():
    """In 'mention' mode, only messages containing bot name are processed."""
    config = _make_config(trigger_mode="mention", bot_name="b0b0t")
    assert should_process(_make_msg("hey @b0b0t, what time is it?"), config)
    assert should_process(_make_msg("@B0B0T help me"), config)
    assert should_process(_make_msg("b0b0t are you there?"), config)
    assert should_process(_make_msg("@[b0b0t] prefix d2"), config)
    assert not should_process(_make_msg("hello everyone"), config)


def test_strip_mention_at_prefix():
    """Strip @bot_name from message text."""
    assert strip_mention("@b0b0t what is 2+2?", "b0b0t") == "what is 2+2?"
    assert strip_mention("@B0B0T, hello", "b0b0t") == "hello"
    assert strip_mention("hey @b0b0t: help", "b0b0t") == "hey help"


def test_strip_mention_bracket_format():
    """Strip @[bot_name] mesh radio format."""
    assert strip_mention("@[b0b0t] prefix d2", "b0b0t") == "prefix d2"
    assert strip_mention("@[B0B0T] hello", "b0b0t") == "hello"


def test_strip_mention_bare_name():
    """Strip bare bot_name from message text."""
    assert strip_mention("b0b0t what is the weather?", "b0b0t") == "what is the weather?"


def test_strip_mention_no_match():
    """If bot name is not present, text is unchanged."""
    assert strip_mention("hello world", "b0b0t") == "hello world"


def test_looks_like_command():
    """Detect known commands without ! prefix."""
    assert _looks_like_command("prefix d2") == ("prefix", "d2")
    assert _looks_like_command("path ab12") == ("path", "ab12")
    assert _looks_like_command("ping") == ("ping", "")
    assert _looks_like_command("help") == ("help", "")
    assert _looks_like_command("hello world") is None
    assert _looks_like_command("") is None


def test_sender_parsing():
    """Message text with sender prefix is split correctly."""
    msg = MeshMessage.from_event_payload({
        "text": "Miguel EA4IPW 🧄: @b0b0t hello",
        "channel_idx": 2,
        "path_len": 1,
        "sender_timestamp": 123,
    })
    assert msg.sender == "Miguel EA4IPW 🧄"
    assert msg.text == "@b0b0t hello"


def test_sender_parsing_no_colon():
    """Message without sender prefix uses empty sender."""
    msg = MeshMessage.from_event_payload({
        "text": "just a message",
        "channel_idx": 0,
        "path_len": 0,
        "sender_timestamp": 0,
    })
    assert msg.sender == ""
    assert msg.text == "just a message"
