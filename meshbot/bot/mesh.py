"""Mesh radio connection wrapper with event-driven message delivery."""

import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import meshcore  # type: ignore[import-untyped]
from meshcore.events import EventType  # type: ignore[import-untyped]
from meshcore.packets import CommandType  # type: ignore[import-untyped]

from meshbot.models import BotConfig, MeshMessage

logger = logging.getLogger("meshbot.mesh")

MAX_CHANNEL_SLOTS = 8
LAST_SEEN_FILE = "last_seen.json"


def derive_channel_secret(channel_name: str) -> bytes:
    """Derive the 16-byte AES secret for a public channel from its name."""
    return sha256(channel_name.encode("utf-8")).digest()[:16]


class MeshConnection:
    """Owns the meshcore serial connection and delivers messages via async queue."""

    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.mc: Any = None
        self.channel_idx: int = -1
        self._chan_sub: Any = None
        self._priv_sub: Any = None
        self._queue: asyncio.Queue[MeshMessage] = asyncio.Queue()
        # Dedup: track recent message IDs (sender_timestamp + text hash)
        self._seen_msg_ids: set[str] = set()
        self._seen_msg_times: dict[str, float] = {}
        # Track when/where we last saw each sender
        # {name: {"time": unix_ts, "channel": "#name"}}
        self.last_seen: dict[str, dict[str, Any]] = {}
        self._load_last_seen()

    def _load_last_seen(self) -> None:
        """Load last_seen data from disk."""
        path = Path(LAST_SEEN_FILE)
        if path.exists():
            try:
                self.last_seen = json.loads(path.read_text())
                logger.info("Loaded %d contacts from %s", len(self.last_seen), LAST_SEEN_FILE)
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to load %s: %s", LAST_SEEN_FILE, e)

    def _save_last_seen(self) -> None:
        """Persist last_seen data to disk."""
        try:
            Path(LAST_SEEN_FILE).write_text(json.dumps(self.last_seen, indent=2))
        except OSError as e:
            logger.warning("Failed to save %s: %s", LAST_SEEN_FILE, e)

    def _record_seen(self, name: str, channel: str) -> None:
        """Record that a sender was seen now on a given channel."""
        if not name:
            return
        self.last_seen[name] = {"time": time.time(), "channel": channel}
        self._save_last_seen()

    def _is_duplicate(self, msg: MeshMessage) -> bool:
        """Check if we've already seen this message. Returns True if duplicate."""
        msg_id = f"{msg.sender_timestamp}:{hash(msg.text)}"
        now = time.time()
        # Clean old entries (older than 60s)
        stale = [k for k, t in self._seen_msg_times.items() if now - t > 60]
        for k in stale:
            self._seen_msg_ids.discard(k)
            del self._seen_msg_times[k]
        if msg_id in self._seen_msg_ids:
            return True
        self._seen_msg_ids.add(msg_id)
        self._seen_msg_times[msg_id] = now
        return False

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

        self._chan_sub = self.mc.subscribe(
            EventType.CHANNEL_MSG_RECV, self._on_channel_message
        )
        self._priv_sub = None
        if self.config.allow_private:
            self._priv_sub = self.mc.subscribe(
                EventType.CONTACT_MSG_RECV, self._on_private_message
            )
            logger.info("Private messages enabled")

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
        if self._chan_sub:
            self._chan_sub.unsubscribe()
            self._chan_sub = None
        if self._priv_sub:
            self._priv_sub.unsubscribe()
            self._priv_sub = None
        if self.mc:
            await self.mc.stop_auto_message_fetching()
            await self.mc.disconnect()
            logger.info("Disconnected from mesh device")
            self.mc = None

    async def _on_channel_message(self, event: Any) -> None:
        """Event callback: enqueue incoming channel messages and track sender."""
        msg = MeshMessage.from_channel_payload(event.payload)
        if self._is_duplicate(msg):
            logger.debug("Duplicate channel msg from %s, skipping", msg.sender)
            return
        logger.debug(
            "RX ch=%d sender=%s path_len=%d: %s",
            msg.channel_idx, msg.sender, msg.path_len, msg.text,
        )
        self._record_seen(msg.sender, self.config.channel)
        await self._queue.put(msg)

    async def _on_private_message(self, event: Any) -> None:
        """Event callback: enqueue incoming private messages."""
        msg = MeshMessage.from_private_payload(event.payload)
        if self._is_duplicate(msg):
            logger.debug("Duplicate DM from %s, skipping", msg.pubkey_prefix)
            return
        # Resolve sender name from contacts if possible
        await self.mc.ensure_contacts()
        node = self.mc.get_contact_by_key_prefix(msg.pubkey_prefix)
        if node:
            msg.sender = node.get("adv_name", msg.pubkey_prefix)
        logger.debug(
            "RX DM from=%s (%s): %s",
            msg.sender, msg.pubkey_prefix, msg.text,
        )
        self._record_seen(msg.sender, "DM")
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

    async def send_private(self, pubkey_prefix: str, text: str) -> bool:
        """Send a private message to a node (with retry). Returns True on success."""
        # Resolve full public key for better routing (allows path reset)
        await self.mc.ensure_contacts()
        node = self.mc.get_contact_by_key_prefix(pubkey_prefix)
        dst = node.get("public_key", pubkey_prefix) if node else pubkey_prefix
        name = node.get("adv_name", pubkey_prefix) if node else pubkey_prefix
        logger.debug("TX DM to=%s (%s): %s", name, dst[:12], text)
        result = await self.mc.commands.send_msg_with_retry(dst, text)
        if result is None:
            logger.error("Failed to send DM to %s: no ACK after retries", name)
            return False
        logger.info("TX DM to=%s: %s", name, text)
        return True

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

            seen = self.last_seen.get(name)
            if seen:
                bot_seen_str = f"{_format_ago(seen['time'])} on {seen['channel']}"
            else:
                bot_seen_str = "never seen by bot"

            results.append({
                "name": name,
                "public_key": contact.get("public_key", "")[:12],
                "type": _contact_type_name(contact.get("type", 0)),
                "hops": contact.get("out_path_len", "?"),
                "last_advert": last_advert_str,
                "last_seen": bot_seen_str,
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
