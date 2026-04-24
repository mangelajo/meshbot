"""Data models for meshbot."""

from dataclasses import dataclass, field


@dataclass
class MeshMessage:
    """A message received from the mesh network."""

    text: str
    sender: str
    channel_idx: int
    path_len: int
    sender_timestamp: int
    is_private: bool = False
    pubkey_prefix: str = ""
    txt_type: int = 0
    path: str = ""
    path_hash_size: int = 1
    snr: float | None = None

    @classmethod
    def from_channel_payload(cls, payload: dict) -> "MeshMessage":
        """Create from a channel message event payload.

        Channel message text has format "SenderName: message content".
        """
        raw_text = payload.get("text", "")
        sender, body = _split_sender(raw_text)

        path_len = payload.get("path_len", 0)
        if path_len == 255:
            path_len = 0

        return cls(
            text=body,
            sender=sender,
            channel_idx=payload.get("channel_idx", 0),
            path_len=path_len,
            sender_timestamp=payload.get("sender_timestamp", 0),
            is_private=False,
            txt_type=payload.get("txt_type", 0),
            path=payload.get("path", ""),
            path_hash_size=_deduce_hash_size(
                payload.get("path", ""), path_len, payload.get("path_hash_size"),
            ),
            snr=payload.get("SNR"),
        )

    @classmethod
    def from_private_payload(cls, payload: dict) -> "MeshMessage":
        """Create from a private (contact) message event payload.

        Private messages have pubkey_prefix but no sender name in text.
        """
        path_len = payload.get("path_len", 0)
        if path_len == 255:
            path_len = 0

        pubkey_prefix = payload.get("pubkey_prefix", "")

        return cls(
            text=payload.get("text", ""),
            sender=pubkey_prefix,
            channel_idx=-1,
            path_len=path_len,
            sender_timestamp=payload.get("sender_timestamp", 0),
            is_private=True,
            pubkey_prefix=pubkey_prefix,
            txt_type=payload.get("txt_type", 0),
            path=payload.get("path", ""),
            path_hash_size=_deduce_hash_size(
                payload.get("path", ""), path_len, payload.get("path_hash_size"),
            ),
            snr=payload.get("SNR"),
        )


def _deduce_hash_size(path: str, path_len: int, explicit: int | None) -> int:
    """Deduce path hash size from path hex string length and hop count.

    The meshcore RF log includes path_hash_size but it's not always
    passed through to the channel message event. We can deduce it:
    hash_size = len(path_hex) / (2 * hops)
    """
    if explicit is not None:
        return explicit
    if path and path_len > 0:
        hex_len = len(path)
        bytes_per_hop = hex_len // (2 * path_len)
        if bytes_per_hop >= 1 and hex_len == bytes_per_hop * 2 * path_len:
            return bytes_per_hop
    return 1


def split_path_prefixes(path: str, hash_size: int = 1) -> list[str]:
    """Split a path hex string into individual prefixes.

    Each prefix is hash_size bytes = hash_size*2 hex chars.
    e.g. path="edd2ab" with hash_size=1 -> ["ed", "d2", "ab"]
         path="ed33d244" with hash_size=2 -> ["ed33", "d244"]
    """
    if not path:
        return []
    chars_per_prefix = hash_size * 2
    return [path[i : i + chars_per_prefix] for i in range(0, len(path), chars_per_prefix)]


def _split_sender(raw: str) -> tuple[str, str]:
    """Split 'SenderName: message body' into (sender, body).

    If no colon found, returns ("", raw).
    """
    idx = raw.find(": ")
    if idx == -1:
        return "", raw
    return raw[:idx], raw[idx + 2 :]


@dataclass
class MessageConfig:
    """Message length constraints."""

    max_length: int = 200
    max_parts: int = 3


@dataclass
class BotConfig:
    """Bot configuration loaded from YAML + CLI overrides."""

    serial_port: str = "/dev/ttyUSB0"
    baudrate: int = 115200
    channel: str = "#general"
    bot_name: str = "meshbot"
    trigger_mode: str = "mention"  # "all" or "mention"
    poll_interval: float = 1.0
    provider: str = "ollama"
    model: str = "gemma3"
    ollama_base_url: str = "http://localhost:11434/v1"
    minimax_base_url: str = "https://api.minimax.chat/v1"
    language: str = "English"
    prompt_prefix: str = ""
    allow_private: bool = False
    cooldown: float = 10.0
    history_size: int = 10
    message: MessageConfig = field(default_factory=MessageConfig)
    debug: bool = False
    verbose: bool = False
