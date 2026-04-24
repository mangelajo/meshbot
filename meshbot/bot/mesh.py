"""Mesh radio connection wrapper with event-driven message delivery."""

import asyncio
import logging
import time
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

import meshcore  # type: ignore[import-untyped]
from meshcore.events import EventType  # type: ignore[import-untyped]
from meshcore.packets import CommandType  # type: ignore[import-untyped]

from meshbot.models import BotConfig, MeshMessage

logger = logging.getLogger("meshbot.mesh")

MAX_CHANNEL_SLOTS = 8


def derive_channel_secret(channel_name: str) -> bytes:
    """Derive the 16-byte AES secret for a public channel from its name."""
    return sha256(channel_name.encode("utf-8")).digest()[:16]


class MeshConnection:
    """Owns the meshcore serial connection and delivers messages via async queue."""

    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.mc: Any = None
        self.channel_idx: int = -1
        self._subscription: Any = None
        self._queue: asyncio.Queue[MeshMessage] = asyncio.Queue()
        # Track when we last saw each sender (name -> unix timestamp)
        self.last_seen: dict[str, float] = {}

    async def connect(self) -> None:
        """Connect to the mesh device, join the channel, and start listening."""
        logger.info(
            "Connecting to %s at %d baud", self.config.serial_port, self.config.baudrate
        )
        self.mc = await meshcore.MeshCore.create_serial(
            self.config.serial_port, self.config.baudrate, self.config.debug
        )
        await self.mc.start_auto_message_fetching()
        # Enable channel log decryption so message paths are resolved
        self.mc.set_decrypt_channel_logs(True)

        logger.info(
            "Connected as %s (%s...)",
            self.mc.self_info.get("name", "?"),
            self.mc.self_info.get("public_key", "?")[:12],
        )

        # Resolve channel name to index, creating if needed
        self.channel_idx = await self._join_channel(self.config.channel)

        self._subscription = self.mc.subscribe(
            EventType.CHANNEL_MSG_RECV, self._on_channel_message
        )

    async def _join_channel(self, channel_name: str) -> int:
        """Find or create a channel by name, return its index."""
        # Scan existing channel slots
        empty_slot: int | None = None
        for idx in range(MAX_CHANNEL_SLOTS):
            data = bytes([CommandType.GET_CHANNEL.value, idx])
            result = await self.mc.commands.send(
                data, [EventType.CHANNEL_INFO, EventType.ERROR], timeout=3
            )
            if result.type == EventType.CHANNEL_INFO:
                name = result.payload.get("channel_name", "")
                if name == channel_name:
                    logger.info("Found channel %s at index %d", channel_name, idx)
                    return idx
                if not name and empty_slot is None:
                    empty_slot = idx

        # Channel not found — create it on the first empty slot
        if empty_slot is None:
            raise RuntimeError(
                f"Channel {channel_name} not found and no empty slots available"
            )

        secret = derive_channel_secret(channel_name)
        name_bytes = channel_name.encode("utf-8")
        name_padded = name_bytes + b"\x00" * (32 - len(name_bytes))

        data = bytes([CommandType.SET_CHANNEL.value, empty_slot]) + name_padded + secret
        result = await self.mc.commands.send(data, [EventType.OK, EventType.ERROR], timeout=5)
        if result.type == EventType.ERROR:
            raise RuntimeError(f"Failed to set channel {channel_name}: {result.payload}")

        logger.info("Created channel %s at index %d", channel_name, empty_slot)
        return empty_slot

    async def disconnect(self) -> None:
        """Disconnect from the mesh device."""
        if self._subscription:
            self._subscription.unsubscribe()
            self._subscription = None
        if self.mc:
            await self.mc.stop_auto_message_fetching()
            await self.mc.disconnect()
            logger.info("Disconnected from mesh device")
            self.mc = None

    async def _on_channel_message(self, event: Any) -> None:
        """Event callback: enqueue incoming channel messages and track sender."""
        msg = MeshMessage.from_event_payload(event.payload)
        logger.debug(
            "RX ch=%d sender=%s path_len=%d: %s",
            msg.channel_idx, msg.sender, msg.path_len, msg.text,
        )
        if msg.sender:
            self.last_seen[msg.sender] = time.time()
        await self._queue.put(msg)

    async def recv(self) -> MeshMessage:
        """Wait for and return the next incoming message."""
        return await self._queue.get()

    async def send(self, channel_idx: int, text: str) -> None:
        """Send a message to a channel."""
        logger.debug("TX ch=%d: %s", channel_idx, text)
        result = await self.mc.commands.send_chan_msg(channel_idx, text)
        if result.type == EventType.ERROR:
            logger.error("Failed to send message: %s", result.payload)
        else:
            logger.info("TX ch=%d: %s", channel_idx, text)

    async def get_contacts(self) -> list[dict[str, Any]]:
        """Return all known contacts."""
        await self.mc.ensure_contacts()
        return list(self.mc.contacts.values())

    async def get_repeaters(self) -> list[dict[str, Any]]:
        """Return all repeater contacts."""
        await self.mc.ensure_contacts()
        return [c for c in self.mc.contacts.values() if c.get("type") == 2]

    async def get_node_by_prefix(self, prefix: str) -> dict[str, Any] | None:
        """Look up a contact by public key prefix."""
        await self.mc.ensure_contacts()
        result: dict[str, Any] | None = self.mc.get_contact_by_key_prefix(prefix)
        return result

    async def get_contacts_by_name(self, pattern: str) -> list[dict[str, Any]]:
        """Search contacts by name pattern (case-insensitive substring match).

        Merges meshcore contact data with bot's own last_seen tracking.
        """
        await self.mc.ensure_contacts()
        pattern_lower = pattern.lower()
        results = []
        for contact in self.mc.contacts.values():
            name = contact.get("adv_name", "")
            if not name or pattern_lower not in name.lower():
                continue

            last_advert = contact.get("last_advert", 0)
            last_advert_str = _format_timestamp(last_advert) if last_advert else "unknown"

            bot_seen = self.last_seen.get(name)
            bot_seen_str = _format_ago(bot_seen) if bot_seen else "never on this channel"

            results.append({
                "name": name,
                "public_key": contact.get("public_key", "")[:12],
                "type": _contact_type_name(contact.get("type", 0)),
                "hops": contact.get("out_path_len", "?"),
                "last_advert": last_advert_str,
                "last_seen_on_channel": bot_seen_str,
            })

        return results

    async def get_status(self) -> dict[str, Any]:
        """Return connection status and device info."""
        await self.mc.ensure_contacts()
        return {
            "connected": self.mc.is_connected,
            "self_info": self.mc.self_info,
            "contact_count": len(self.mc.contacts),
        }

    async def __aenter__(self) -> "MeshConnection":
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.disconnect()


_CONTACT_TYPES = {0: "node", 1: "client", 2: "repeater", 3: "room", 4: "sensor"}


def _contact_type_name(t: int) -> str:
    return _CONTACT_TYPES.get(t, f"type={t}")


def _format_timestamp(ts: int) -> str:
    """Format a unix timestamp as a readable datetime."""
    try:
        dt = datetime.fromtimestamp(ts, tz=UTC)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except (OSError, ValueError):
        return str(ts)


def _format_ago(ts: float) -> str:
    """Format a timestamp as 'X ago' relative to now."""
    delta = time.time() - ts
    if delta < 60:
        return "just now"
    elif delta < 3600:
        mins = int(delta / 60)
        return f"{mins}m ago"
    elif delta < 86400:
        hours = int(delta / 3600)
        return f"{hours}h ago"
    else:
        days = int(delta / 86400)
        return f"{days}d ago"
