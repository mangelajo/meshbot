"""Tests for config loading."""

import tempfile

import yaml

from meshbot.config import load_config


def test_load_defaults():
    """Load config with no file and no overrides returns defaults."""
    cfg = load_config()
    assert cfg.serial_port == "/dev/ttyUSB0"
    assert cfg.baudrate == 115200
    assert cfg.channel == "#general"
    assert cfg.bot_name == "meshbot"
    assert cfg.trigger_mode == "mention"
    assert cfg.message.max_length == 200
    assert cfg.message.max_parts == 3


def test_load_from_yaml():
    """Load config from a YAML file."""
    data = {
        "serial_port": "/dev/ttyACM0",
        "channel": "#test",
        "provider": "anthropic",
        "model": "haiku",
        "message": {"max_length": 150, "max_parts": 2},
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(data, f)
        f.flush()
        cfg = load_config(f.name)

    assert cfg.serial_port == "/dev/ttyACM0"
    assert cfg.channel == "#test"
    assert cfg.provider == "anthropic"
    assert cfg.model == "haiku"
    assert cfg.message.max_length == 150
    assert cfg.message.max_parts == 2
    # Defaults still apply for unset fields
    assert cfg.baudrate == 115200


def test_cli_overrides_yaml():
    """CLI overrides take precedence over YAML."""
    data = {"serial_port": "/dev/ttyACM0", "channel": "#test"}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(data, f)
        f.flush()
        cfg = load_config(f.name, serial_port="/dev/ttyUSB1", channel=None)

    # Override applied
    assert cfg.serial_port == "/dev/ttyUSB1"
    # None override is ignored, YAML value kept
    assert cfg.channel == "#test"


def test_nonexistent_config_file():
    """Non-existent config file falls back to defaults."""
    cfg = load_config("/nonexistent/config.yaml")
    assert cfg.serial_port == "/dev/ttyUSB0"


def test_unknown_keys_ignored():
    """Unknown keys in YAML are silently ignored."""
    data = {"serial_port": "/dev/ttyACM0", "unknown_key": "value"}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(data, f)
        f.flush()
        cfg = load_config(f.name)

    assert cfg.serial_port == "/dev/ttyACM0"
