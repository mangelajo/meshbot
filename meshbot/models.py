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
    txt_type: int = 0
    path: str = ""
    snr: float | None = None

    @classmethod
    def from_event_payload(cls, payload: dict) -> "MeshMessage":
        """Create a MeshMessage from a meshcore event payload dict.

        The raw text field has format "SenderName: message content".
        We split sender and body here.
        """
        raw_text = payload.get("text", "")
        sender, body = _split_sender(raw_text)

        path_len = payload.get("path_len", 0)
        # path_len=255 means direct (no repeaters)
        if path_len == 255:
            path_len = 0

        return cls(
            text=body,
            sender=sender,
            channel_idx=payload.get("channel_idx", 0),
            path_len=path_len,
            sender_timestamp=payload.get("sender_timestamp", 0),
            txt_type=payload.get("txt_type", 0),
            path=payload.get("path", ""),
            snr=payload.get("SNR"),
        )


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
    history_size: int = 10
    message: MessageConfig = field(default_factory=MessageConfig)
    debug: bool = False
    verbose: bool = False
