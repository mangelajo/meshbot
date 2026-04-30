"""Mesh radio connection wrapper with event-driven message delivery."""

import asyncio
import json
import logging
import random
import time
import unicodedata
from collections import deque
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterator

import meshcore  # type: ignore[import-untyped]
from meshcore.events import EventType  # type: ignore[import-untyped]
from meshcore.packets import CommandType  # type: ignore[import-untyped]

from meshbot.bot.message_store import MessageStore
from meshbot.bot.state_store import DB_FILENAME, StateStore
from meshbot.models import BotConfig, MeshMessage, split_path_prefixes

logger = logging.getLogger("meshbot.mesh")

MAX_CHANNEL_SLOTS = 8
MAX_ROUTES_PER_CONTACT = 20


def _normalize(text: str) -> str:
    """Normalize text for accent-insensitive comparison."""
    nfkd = unicodedata.normalize("NFKD", text.lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize_prefix_lengths(prefixes: list[str]) -> list[str]:
    """Truncate all prefixes to the minimum valid hash size.

    MeshCore uses 1-byte (2 hex), 2-byte (4 hex), or 4-byte (8 hex) hashes.
    Find the shortest prefix, round down to a valid size, and truncate all.
    e.g. ["d259", "cebada6a59c6"] → ["d259", "ceba"]  (4 hex = 2-byte)
         ["d2", "af32", "cebada"] → ["d2", "af", "ce"]  (2 hex = 1-byte)
    """
    if not prefixes:
        return prefixes
    min_len = min(len(p) for p in prefixes)
    # Round down to nearest valid hash size: 2, 4, or 8 hex chars
    if min_len >= 8:
        size = 8
    elif min_len >= 4:
        size = 4
    else:
        size = 2
    return [p[:size] for p in prefixes]


def derive_channel_secret(channel_name: str) -> bytes:
    """Derive the 16-byte AES secret for a public channel from its name."""
    return sha256(channel_name.encode("utf-8")).digest()[:16]


class MeshConnection:
    """Owns the meshcore serial connection and delivers messages via async queue."""

    def __init__(self, config: BotConfig, data_dir: str = ".") -> None:
        self.config = config
        self._data_dir = Path(data_dir)
        self.mc: Any = None
        self.channel_idx: int = -1
        self._chan_sub: Any = None
        self._priv_sub: Any = None
        self._rflog_sub: Any = None
        self._queue: asyncio.Queue[MeshMessage] = asyncio.Queue()
        # Dedup: track recent message IDs (sender_timestamp + text hash)
        self._seen_msg_ids: set[str] = set()
        self._seen_msg_times: dict[str, float] = {}
        # Multipath: collect all paths for each message ID
        # {msg_id: [{"path": str, "path_len": int, "path_hash_size": int, ...}]}
        self._multipath: dict[str, list[dict[str, Any]]] = {}
        # RF log cache for path correlation. A list (not a dict keyed by
        # recv_time) because the firmware can emit multiple TEXT_MSG
        # RX_LOG_DATA in the same firmware-second (separate physical
        # copies of one logical packet via different relay chains) and
        # using recv_time as a key was overwriting them. Each entry
        # carries its own wall-clock "arrival" plus the firmware
        # recv_time for any future debugging.
        self._rflog_cache: list[dict[str, Any]] = []
        # Counter that ticks every time a payload_type=2 (TEXT_MSG) RX_LOG_DATA
        # gets cached. Used by handlers to detect "a fresh rflog has arrived"
        # without races: callers snapshot the value, then poll until it grows.
        self._rflog_text_msg_count: int = 0
        # Wall-clock time at which each msg_id was decoded via
        # CONTACT_MSG_RECV/CHANNEL_MSG_RECV. Used to retroactively attach
        # late-arriving RX_LOG_DATA copies to the right multipath bucket
        # (the firmware emits one event per heard copy of a packet, so
        # relays keep coming in for several seconds after the first
        # decoded copy).
        self._recently_decoded: dict[str, float] = {}
        # Dedup for stats recording at the RF-log level (pkt_hash -> recv_ts).
        # The same packet can be relayed by multiple repeaters; without this we
        # would count its entry repeater multiple times.
        self._stats_pkt_hashes: dict[int, float] = {}
        # SQLite-backed state store. Owns the persisted runtime state
        # (adverts, routes_seen, route stats, last_seen). MessageStore
        # below shares the same DB file via its own connection — WAL
        # mode makes that safe.
        self.state = StateStore(self._data_dir / DB_FILENAME)
        # Message store. Same file as StateStore — both connections
        # coexist via WAL. StateStore was constructed first, so the
        # legacy messages.db has already been renamed to meshbot.db
        # by the time we get here.
        self.message_store = MessageStore(
            db_path=str(self._data_dir / DB_FILENAME),
            max_age_days=config.message_store_days,
        )
        # Channel index -> name mapping (populated during connect)
        self.channel_names: dict[int, str] = {}
        # Consecutive DM-send failures per pubkey_prefix. Reset on
        # success; when this hits 2 we force a reset_path to drop the
        # cached out_path so the next send falls back to flood routing.
        self._dm_failures: dict[str, int] = {}
        # Ring buffer of recent send failures for the !sendq command.
        # {time, name, kind ('DM'/'channel'), reason}
        self._send_failure_log: deque[dict[str, Any]] = deque(maxlen=20)

    def get_dm_history(self, pubkey: str, limit: int) -> list[tuple[str, str]]:
        """Return the most-recent (sender, text) entries from the DM thread
        with ``pubkey``, oldest first. Backed by the messages table since
        Phase 5 — incoming DMs land there via _on_private_message and
        outgoing replies via MessageStore.record_outgoing in loop.py."""
        if not pubkey:
            return []
        return self.message_store.get_dm_history(pubkey, limit)

    def _record_route(self, sender: str, msg: MeshMessage) -> None:
        """Record a message's route in the persistent route history."""
        if not sender or msg.path_len == 0:
            return
        route = "direct" if not msg.path else "->".join(
            split_path_prefixes(msg.path, msg.path_hash_size)
        )
        self.state.record_route(
            contact_name=sender,
            route=route,
            hops=msg.path_len,
            seen_at=time.time(),
            history_max=MAX_ROUTES_PER_CONTACT,
        )

    def _record_advert(self, payload: dict[str, Any], path_len: int) -> None:
        """Persist an advert in the SQLite state store and log a one-liner
        so live drift is watchable via journalctl. Also captures the path
        the advert took as an observed route — passive repeaters otherwise
        leave routes_seen empty since they never originate chat traffic."""
        adv_key = payload.get("adv_key", "")
        if not adv_key:
            return
        adv_name = payload.get("adv_name", "") or ""
        adv_path = payload.get("path", "") or ""
        if adv_name and path_len > 0 and adv_path:
            fake = MeshMessage(
                text="", sender=adv_name, channel_idx=-1, path_len=path_len,
                sender_timestamp=0, path=adv_path,
                path_hash_size=int(payload.get("path_hash_size", 1)),
            )
            self._record_route(adv_name, fake)

        now = time.time()
        adv_ts = payload.get("adv_timestamp", 0)
        # Sign convention: drift = sender's clock minus ours. So a node
        # whose clock is in our past (slow / never synced) reports a
        # negative drift; one in our future reports a positive drift.
        drift = int(adv_ts - now) if adv_ts else None
        snr = payload.get("snr")
        rssi = payload.get("rssi")
        self.state.record_advert(
            pubkey=adv_key,
            name=adv_name or None,
            recv_at=now,
            adv_ts=int(adv_ts) if adv_ts else None,
            drift=drift,
            snr=snr,
            rssi=int(rssi) if rssi is not None else None,
            path_len=int(path_len),
            adv_type=payload.get("adv_type"),
            lat=payload.get("adv_lat"),
            lon=payload.get("adv_lon"),
            path=adv_path or None,
        )

        drift_str = f"{drift:+d}s" if drift is not None else "n/a"
        logger.info(
            "ADVERT name=%s key=%s drift=%s snr=%s rssi=%s path_len=%s",
            adv_name, adv_key[:12], drift_str, snr, rssi, path_len,
        )

    def _record_seen(self, name: str, channel: str) -> None:
        """Record that a sender was seen now on a given channel."""
        if not name:
            return
        self.state.record_seen(name, channel, time.time())

    @staticmethod
    def _msg_id(msg: MeshMessage) -> str:
        return f"{msg.sender_timestamp}:{hash(msg.text)}"

    def get_multipath(self, msg: MeshMessage, max_age: float = 60) -> list[dict[str, Any]]:
        """Get all collected paths for a message, filtering out stale entries."""
        now = time.time()
        routes = self._multipath.get(self._msg_id(msg), [])
        return [r for r in routes if now - r.get("time", 0) <= max_age]

    def _is_duplicate(self, msg: MeshMessage) -> bool:
        """Check if we've already seen this message. Returns True if duplicate.

        Multipath route recording is handled exclusively by
        _correlate_path_from_rflog and the _on_rflog back-fill, so the
        path_len/path on the CONTACT_MSG_RECV/CHANNEL_MSG_RECV header
        (which is unreliable: the firmware often emits 0/sentinel even
        for routed packets) never reaches the multipath bucket.
        """
        msg_id = self._msg_id(msg)
        now = time.time()
        # Clean old entries (older than 60s)
        stale = [k for k, t in self._seen_msg_times.items() if now - t > 60]
        for k in stale:
            self._seen_msg_ids.discard(k)
            del self._seen_msg_times[k]
            self._multipath.pop(k, None)
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

        # Push host time to the companion so our outbound packets carry an
        # accurate timestamp. The host is assumed to be NTP-synced.
        try:
            now = int(time.time())
            res = await self.mc.commands.set_time(now)
            if getattr(res, "type", None) is EventType.OK:
                logger.info("Synced device clock to %d", now)
            else:
                logger.warning("set_time returned %s", res)
        except Exception as e:
            logger.warning("Failed to sync device clock: %s", e)

        # Resolve channel name to index, creating if needed
        self.channel_idx = await self._join_channel(self.config.channel)
        self.channel_names[self.channel_idx] = self.config.channel

        # Join additional listen-only channels
        for ch_name in self.config.listen_channels:
            idx = await self._join_channel(ch_name)
            self.channel_names[idx] = ch_name
            logger.info("Joined listen-only channel %s at index %d", ch_name, idx)

        self._chan_sub = self.mc.subscribe(
            EventType.CHANNEL_MSG_RECV, self._on_channel_message
        )
        # Subscribe to RF log to capture paths for private messages
        self._rflog_sub = self.mc.subscribe(
            EventType.RX_LOG_DATA, self._on_rflog
        )
        # Subscribe to advertisements to track repeater routes
        self._advert_sub = self.mc.subscribe(
            EventType.ADVERTISEMENT, self._on_advertisement
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
        for sub in (self._chan_sub, self._priv_sub, self._rflog_sub, self._advert_sub):
            if sub:
                sub.unsubscribe()
        self._chan_sub = self._priv_sub = self._rflog_sub = self._advert_sub = None
        if self.mc:
            await self.mc.stop_auto_message_fetching()
            await self.mc.disconnect()
            logger.info("Disconnected from mesh device")
            self.mc = None
        self.message_store.close()

    async def _on_channel_message(self, event: Any) -> None:
        """Event callback: enqueue incoming channel messages and track sender."""
        msg = MeshMessage.from_channel_payload(event.payload)
        if self._is_duplicate(msg):
            logger.debug("Duplicate channel msg from %s, skipping", msg.sender)
            return
        await self._correlate_path_from_rflog(msg, kind="ch")
        logger.debug(
            "RX ch=%d sender=%s path_len=%d: %s",
            msg.channel_idx, msg.sender, msg.path_len, msg.text,
        )
        channel_name = self.channel_names.get(msg.channel_idx, f"ch{msg.channel_idx}")
        self._record_seen(msg.sender, channel_name)
        self._record_route(msg.sender, msg)
        self.message_store.store(msg, channel_name)
        await self._queue.put(msg)

    async def _on_advertisement(self, event: Any) -> None:
        """Track routes from repeater/node advertisements."""
        payload = event.payload
        name = payload.get("adv_name", "")
        if not name:
            return
        path = payload.get("path", "")
        path_len = payload.get("path_len", 0)
        path_hash_size = payload.get("path_hash_size", 1)
        if path_len == 0:
            return
        # Build a minimal MeshMessage-like for _record_route. Stats recording
        # for adverts happens via the RF-log handler instead.
        fake_msg = MeshMessage(
            text="", sender=name, channel_idx=-1, path_len=path_len,
            sender_timestamp=0, path=path, path_hash_size=path_hash_size,
        )
        self._record_route(name, fake_msg)

    async def _on_rflog(self, event: Any) -> None:
        """Record route stats for any received packet, and cache TEXT_MSG
        entries for path correlation with private messages we decode."""
        payload = event.payload

        # Stats: count every routed packet we hear, even ones we can't decode.
        # Dedup by packet hash within a 60s window so multi-relay copies don't
        # over-weight the entry repeater.
        pkt_hash = payload.get("pkt_hash")
        path_len = payload.get("path_len", 0)
        path = payload.get("path", "")
        if pkt_hash is not None and path_len > 0 and path:
            now = time.time()
            self._stats_pkt_hashes = {
                h: t for h, t in self._stats_pkt_hashes.items() if now - t < 60
            }
            if pkt_hash not in self._stats_pkt_hashes:
                self._stats_pkt_hashes[pkt_hash] = now
                self.state.record_path(
                    path, path_len, int(payload.get("path_hash_size", 1))
                )

        # payload_type 4 = ADVERT — record the advertised clock drift and
        # other identifying fields so we can inspect the network and spot
        # repeaters with a wrong RTC via !clocks.
        if payload.get("payload_type") == 4:
            self._record_advert(payload, path_len)

        # payload_type 2 = TEXT_MSG (private messages and channel messages) —
        # cache for path correlation
        if payload.get("payload_type") != 2:
            return
        recv_time = payload.get("recv_time", 0)
        if recv_time:
            self._rflog_cache.append({
                "recv_time": recv_time,
                "path": payload.get("path", ""),
                "path_len": payload.get("path_len", 0),
                "path_hash_size": payload.get("path_hash_size", 1),
                "snr": payload.get("snr"),
                "rssi": payload.get("rssi"),
                "arrival": time.time(),
            })
            self._rflog_text_msg_count += 1
            logger.debug(
                "RFLOG TEXT_MSG cached recv_time=%s path_len=%s path=%s snr=%s",
                recv_time, payload.get("path_len"), payload.get("path"),
                payload.get("snr"),
            )
            # Attach this copy to the most-recently-decoded msg so
            # !multipath sees relays that arrive after CONTACT_MSG_RECV.
            # Only the most recent qualifies: associating to multiple
            # would contaminate them, and in practice copies of the same
            # logical packet keep arriving for several seconds.
            now = time.time()
            recent = [
                (t, mid) for mid, t in self._recently_decoded.items()
                if now - t <= 15
            ]
            if recent and payload.get("path") and payload.get("path_len", 0) > 0:
                _, recent_mid = max(recent)
                self._multipath_add_entry(
                    recent_mid, payload.get("path", ""),
                    payload.get("path_len", 0),
                    payload.get("path_hash_size", 1),
                    payload.get("snr"),
                )
            # Garbage-collect stale decoded-msg markers
            self._recently_decoded = {
                mid: t for mid, t in self._recently_decoded.items()
                if now - t <= 60
            }
            # Keep cache bounded (FIFO, drop oldest half when over 100)
            if len(self._rflog_cache) > 100:
                self._rflog_cache = self._rflog_cache[-50:]

    def _rflog_in_window(
        self, arrival: float, window: float = 8.0
    ) -> list[dict[str, Any]]:
        """All cached TEXT_MSG RX_LOG_DATA entries with `arrival` within
        `window` seconds (wall clock) of the given timestamp.

        Window of 8s is generous on purpose: LoRa SF8 BW62.5 has ~1s
        airtime per packet, and successive relay copies of the same
        logical packet can arrive 4-7s apart. A tighter window was
        excluding legitimate copies whose CONTACT_MSG_RECV decode came
        late.
        """
        return [
            e for e in self._rflog_cache
            if abs(e.get("arrival", 0) - arrival) <= window
        ]

    def _multipath_add_entry(
        self, msg_id: str, path: str, path_len: int,
        path_hash_size: int, snr: Any,
    ) -> None:
        """Append a multipath route for msg_id, deduping by (path, path_len)."""
        routes = self._multipath.setdefault(msg_id, [])
        for r in routes:
            if r.get("path") == path and r.get("path_len") == path_len:
                return
        routes.append({
            "path": path,
            "path_len": path_len,
            "path_hash_size": path_hash_size,
            "snr": snr,
            "is_direct": path_len == 0,
            "time": time.time(),
        })

    def _find_rflog_path(
        self, msg: MeshMessage, arrival: float, window: float = 8.0
    ) -> dict[str, Any] | None:
        """Find an RF log entry that matches a TEXT_MSG we just decoded.

        CONTACT_MSG_RECV/CHANNEL_MSG_RECV sometimes carry the firmware's
        "no path info" sentinel (255, decoded as 0 by us) even when the
        packet actually traversed repeaters, so the RF log's raw view is
        more reliable. We restrict the search to entries that arrived
        within `window` seconds of `arrival` (wall clock) to avoid
        attributing the path of an unrelated earlier packet.

        The firmware emits multiple RX_LOG_DATA per logical packet when
        we hear both the original transmission (path_len=0, strong SNR)
        and one or more relay copies (path_len>0, weaker SNR). For the
        msg's "main" path we want the FIRST copy heard (the route that
        delivered the packet fastest); later copies are alternative
        routes that !multipath collects separately.

        Selection: among in-window entries with explicit path info,
        return the earliest by arrival; if msg.path_len already > 0,
        prefer an exact-length match. Only fall back to a no-path
        entry when no path-bearing one exists in the window.
        """
        if not self._rflog_cache:
            return None
        in_window = self._rflog_in_window(arrival, window)
        if not in_window:
            return None
        with_path = [
            e for e in in_window
            if e.get("path") and e.get("path_len", 0) > 0
        ]
        if msg.path_len > 0:
            exact = [e for e in with_path if e.get("path_len", 0) == msg.path_len]
            if exact:
                return min(exact, key=lambda e: e.get("arrival", 0))
        if with_path:
            return min(with_path, key=lambda e: e.get("arrival", 0))
        return min(in_window, key=lambda e: e.get("arrival", 0))

    async def _wait_for_text_rflog(
        self, since_count: int, timeout: float = 2.0
    ) -> bool:
        """Wait up to `timeout` seconds for a fresh TEXT_MSG RX_LOG_DATA.

        Returns True if `_rflog_text_msg_count` advanced past `since_count`
        before the deadline, False on timeout. Polls at 100ms — adds at
        most that much latency in the rare race case and zero in the
        fast path (callers only call this when the immediate cache
        lookup already failed).
        """
        deadline = time.time() + timeout
        while self._rflog_text_msg_count <= since_count:
            if time.time() >= deadline:
                return False
            await asyncio.sleep(0.1)
        return True

    async def _on_private_message(self, event: Any) -> None:
        """Event callback: enqueue incoming private messages."""
        msg = MeshMessage.from_private_payload(event.payload)
        if self._is_duplicate(msg):
            logger.debug("Duplicate DM from %s, skipping", msg.pubkey_prefix)
            return
        # Resolve sender name from contacts
        await self.mc.ensure_contacts()
        node = self.mc.get_contact_by_key_prefix(msg.pubkey_prefix)
        if node:
            msg.sender = node.get("adv_name", msg.pubkey_prefix)
        await self._correlate_path_from_rflog(msg, kind="DM")
        logger.debug(
            "RX DM from=%s (%s) hops=%d path=%s: %s",
            msg.sender, msg.pubkey_prefix, msg.path_len, msg.path, msg.text,
        )
        self._record_seen(msg.sender, "DM")
        self._record_route(msg.sender, msg)
        self.message_store.store(msg, "DM")
        await self._queue.put(msg)

    async def _correlate_path_from_rflog(
        self, msg: MeshMessage, *, kind: str
    ) -> None:
        """Backfill msg.path / msg.path_len / msg.snr from the RF log cache.

        CONTACT_MSG_RECV and CHANNEL_MSG_RECV sometimes deliver the
        decoded message before the corresponding RX_LOG_DATA has been
        processed (or with a sentinel path_len). When the immediate
        cache lookup misses, wait briefly for a fresh RX_LOG_DATA to
        arrive and retry. Only mutates msg fields that the rflog
        actually has and that are stronger than what we already have.
        """
        arrival = time.time()
        rflog = self._find_rflog_path(msg, arrival)
        waited = False
        if (rflog is None or not rflog.get("path")) and msg.path_len == 0:
            since = self._rflog_text_msg_count
            logger.info(
                "RFLOG race: %s from=%s no fresh entry yet (path_len=%d), "
                "waiting up to 2s",
                kind, msg.sender or msg.pubkey_prefix or "?", msg.path_len,
            )
            arrived = await self._wait_for_text_rflog(since, timeout=2.0)
            waited = True
            if arrived:
                rflog = self._find_rflog_path(msg, arrival)
                logger.info(
                    "RFLOG race: %s from=%s post-wait match=%s "
                    "(path_len=%s path=%s)",
                    kind, msg.sender or msg.pubkey_prefix or "?", bool(rflog),
                    rflog.get("path_len") if rflog else None,
                    rflog.get("path") if rflog else None,
                )
            else:
                logger.info(
                    "RFLOG race: %s from=%s timed out (no RX_LOG_DATA in 2s)",
                    kind, msg.sender or msg.pubkey_prefix or "?",
                )
        if rflog and rflog.get("path"):
            msg.path = rflog["path"]
            msg.path_hash_size = rflog["path_hash_size"]
            if msg.path_len == 0 and rflog.get("path_len", 0) > 0:
                msg.path_len = rflog["path_len"]
            if rflog.get("snr") is not None:
                msg.snr = rflog["snr"]
        # Every in-window rflog entry with a real path is a separate
        # copy of this packet that traversed a different relay chain.
        # Register each one as a multipath route. Skip path_len=0
        # entries: those are either an overheard original transmission
        # (the bot is geographically close to the sender) or the final
        # delivery hop with the path field consumed; either way they
        # don't represent a route the packet took to reach the bot, so
        # they'd be misleading in !multipath output.
        msg_id = self._msg_id(msg)
        for e in self._rflog_in_window(arrival):
            if not e.get("path") or e.get("path_len", 0) <= 0:
                continue
            self._multipath_add_entry(
                msg_id, e.get("path", ""), e.get("path_len", 0),
                e.get("path_hash_size", 1), e.get("snr"),
            )
        # Mark this msg_id as recently decoded so _on_rflog can attach
        # late-arriving copies to it during the multipath wait window.
        self._recently_decoded[msg_id] = time.time()
        if waited:
            logger.debug(
                "%s from=%s after rflog: hops=%d path=%s",
                kind, msg.sender or msg.pubkey_prefix or "?", msg.path_len,
                msg.path,
            )

    async def recv(self) -> MeshMessage:
        """Wait for and return the next incoming message."""
        return await self._queue.get()

    def _record_send_failure(self, *, name: str, kind: str, reason: str) -> None:
        """Push a send-failure into the ring buffer surfaced by !sendq."""
        self._send_failure_log.append({
            "time": time.time(),
            "name": name,
            "kind": kind,
            "reason": reason,
        })

    async def send(self, channel_idx: int, text: str) -> None:
        """Send a message to a channel."""
        logger.debug("TX ch=%d: %s", channel_idx, text)
        result = await self.mc.commands.send_chan_msg(channel_idx, text)
        if result.type == EventType.ERROR:
            chan = self.channel_names.get(channel_idx, f"ch{channel_idx}")
            reason = str(result.payload) or "send error"
            logger.error("Failed to send message: %s", result.payload)
            self._record_send_failure(name=chan, kind="channel", reason=reason)
        else:
            logger.info("TX ch=%d: %s", channel_idx, text)

    async def send_private(self, pubkey_prefix: str, text: str) -> bool:
        """Send a private message to a node (with retry). Returns True on success.

        Tracks consecutive failures per recipient. After two in a row
        we explicitly reset the cached out_path on the device, so the
        next attempt to that contact starts from flood-routing — useful
        when a previously-discovered direct path has gone stale.
        """
        await self.mc.ensure_contacts()
        node = self.mc.get_contact_by_key_prefix(pubkey_prefix)
        dst = node.get("public_key", pubkey_prefix) if node else pubkey_prefix
        name = node.get("adv_name", pubkey_prefix) if node else pubkey_prefix
        logger.debug("TX DM to=%s (%s): %s", name, dst[:12], text)
        result = await self.mc.commands.send_msg_with_retry(dst, text)
        if result is None:
            n = self._dm_failures.get(pubkey_prefix, 0) + 1
            self._dm_failures[pubkey_prefix] = n
            logger.error(
                "Failed to send DM to %s: no ACK after retries (consecutive=%d)",
                name, n,
            )
            self._record_send_failure(name=name, kind="DM", reason="no ACK")
            # First time we hit the 2-consecutive threshold for this peer:
            # invalidate the cached out_path and retry once. Subsequent
            # failures in the same streak skip this — the path is already
            # cleared and the firmware is already flooding.
            if n == 2 and node and node.get("public_key"):
                full_pubkey = node["public_key"]
                logger.info(
                    "Resetting cached path to %s after %d failures, retrying",
                    name, n,
                )
                try:
                    await self.mc.commands.reset_path(full_pubkey)
                except Exception as e:
                    logger.warning("reset_path failed for %s: %s", name, e)
                    return False
                result = await self.mc.commands.send_msg_with_retry(full_pubkey, text)
                if result is not None:
                    self._dm_failures.pop(pubkey_prefix, None)
                    logger.info("TX DM to=%s (after path reset): %s", name, text)
                    return True
                logger.error(
                    "Failed to send DM to %s even after path reset", name,
                )
                self._record_send_failure(
                    name=name, kind="DM", reason="no ACK (post-reset)",
                )
            return False
        # Success: clear the failure streak.
        self._dm_failures.pop(pubkey_prefix, None)
        logger.info("TX DM to=%s: %s", name, text)
        return True

    async def fetch_neighbours(
        self, contact_query: str, password: str = ""
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Login to a known contact (typically a repeater) and fetch its
        neighbour list. Returns (contact, neighbours) where neighbours is
        sorted by SNR descending. Each neighbour has pubkey, secs_ago,
        snr, plus a resolved 'name' from our local contact table when
        possible.

        This is slow: login involves a round-trip + ~10s LOGIN_SUCCESS
        wait, then fetch_all_neighbours runs another 15-30s. Callers
        should warn the user before invoking.
        """
        await self.mc.ensure_contacts()
        contact = self._find_contact_for_query(contact_query)
        name = contact.get("adv_name", "?")

        # Login phase: up to 3 attempts, racing LOGIN_SUCCESS vs
        # LOGIN_FAILED so we tell "rejected (likely password required)"
        # apart from "no response". Mirrors the meshmap CLI's retry
        # pattern, which reliably reaches repeaters that intermittently
        # drop the first send on a flaky link.
        logged_in = False
        rejected = False
        for attempt in range(1, 4):
            logger.info("Login to %s, attempt %d/3", name, attempt)
            send_evt = await self.mc.commands.send_login(contact, password)
            if send_evt is None or getattr(send_evt, "type", None) is EventType.ERROR:
                if attempt < 3:
                    await asyncio.sleep(2)
                continue
            ok_task = asyncio.create_task(
                self.mc.wait_for_event(EventType.LOGIN_SUCCESS, timeout=10)
            )
            fail_task = asyncio.create_task(
                self.mc.wait_for_event(EventType.LOGIN_FAILED, timeout=10)
            )
            done, pending = await asyncio.wait(
                {ok_task, fail_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
            if fail_task in done and fail_task.result() is not None:
                rejected = True
                break
            if ok_task in done and ok_task.result() is not None:
                logged_in = True
                break
            if attempt < 3:
                await asyncio.sleep(2)

        if rejected:
            raise RuntimeError(
                f"login a {name} rechazado (¿requiere contraseña?)"
            )
        if not logged_in:
            raise RuntimeError(f"login a {name} sin respuesta tras 3 intentos")

        # Fetch phase: also up to 3 attempts.
        result = None
        try:
            for attempt in range(1, 4):
                logger.info(
                    "Fetching neighbours of %s, attempt %d/3", name, attempt
                )
                result = await self.mc.commands.fetch_all_neighbours(
                    contact, timeout=30, min_timeout=15,
                )
                if result is not None:
                    break
                if attempt < 3:
                    await asyncio.sleep(2)
        finally:
            try:
                await self.mc.commands.send_logout(contact)
            except Exception as e:
                logger.warning("send_logout failed for %s: %s", name, e)

        if result is None:
            raise RuntimeError(
                f"sin respuesta de vecinos desde {name} tras 3 intentos"
            )

        neighbours: list[dict[str, Any]] = list(result.get("neighbours", []))
        for nb in neighbours:
            prefix = (nb.get("pubkey") or "").lower()
            nb["name"] = None
            if not prefix:
                continue
            for pk, c in self.mc.contacts.items():
                if pk.lower().startswith(prefix):
                    nb["name"] = c.get("adv_name")
                    break
        neighbours.sort(key=lambda nb: nb.get("snr") or float("-inf"), reverse=True)
        return contact, neighbours

    def _find_contact_for_query(self, query: str) -> dict[str, Any]:
        """Look up a contact by name substring or pubkey prefix.
        Raises ValueError when nothing matches."""
        qlow = _normalize(query)
        qhex = query.lower()
        for pk, c in self.mc.contacts.items():
            name = c.get("adv_name", "") or ""
            if name and qlow in _normalize(name):
                return c
            if pk.lower().startswith(qhex):
                return c
        raise ValueError(f"no conozco repe '{query}'")

    async def traceroute(
        self, path: str, timeout: float = 0, reverse: bool = True
    ) -> dict[str, Any]:
        """Send a round-trip trace along a path and return SNR per hop.

        Args:
            path: Route to trace, e.g. "ceba,ed97" or "ceba->ed97".
            timeout: Max seconds to wait. 0 = auto (2s per hop in round-trip).
            reverse: If True (default), reverse the path assuming it comes from
                route history (farthest→closest). Set False for explicit paths
                where the user specifies the outbound order directly.
        """
        # Normalize path format
        normalized = path.replace("->", ",").replace(" ", "")
        hops_raw = [h.strip() for h in normalized.split(",") if h.strip()]
        if not hops_raw:
            return {"outbound": [], "return": [], "error": "empty path"}

        # Normalize prefix lengths: use minimum valid hash size (2, 4, or 8 hex)
        hops_raw = _normalize_prefix_lengths(hops_raw)

        # Reverse if path comes from route history (farthest→closest)
        hops = list(reversed(hops_raw)) if reverse else hops_raw

        # Build round-trip: outbound + reverse(outbound)[1:]
        roundtrip = hops + list(reversed(hops))[1:]

        # Auto timeout: 2s per hop in round-trip, minimum 10s
        if timeout == 0:
            timeout = max(10, len(roundtrip) * 2)
        roundtrip_str = ",".join(roundtrip)
        logger.info("Traceroute: %s (round-trip: %s)", ",".join(hops), roundtrip_str)

        # Send trace with explicit tag so we can match the response
        tag = random.randint(1, 0xFFFFFFFF)
        result = await self.mc.commands.send_trace(path=roundtrip_str, tag=tag)
        if result.type == EventType.ERROR:
            return {"outbound": [], "return": [], "error": str(result.payload)}

        logger.debug("Trace sent, tag=%d, waiting %ds for response", tag, timeout)

        # Wait for trace response
        trace_event = await self.mc.wait_for_event(
            EventType.TRACE_DATA,
            attribute_filters={"tag": tag},
            timeout=timeout,
        )
        if trace_event is None:
            return {"outbound": [], "return": [], "error": "timeout"}

        # Parse response into outbound/return legs
        trace_path = trace_event.payload.get("path", [])
        n_out = len(hops)

        outbound: list[dict[str, Any]] = []
        return_leg: list[dict[str, Any]] = []

        for i, hop in enumerate(trace_path):
            prefix = hop.get("hash", "")
            snr = hop.get("snr", 0)
            # Resolve name
            name = prefix
            if prefix:
                node = await self.get_node_by_prefix(prefix)
                if node:
                    name = node.get("adv_name", prefix)

            entry = {"prefix": prefix or "local", "name": name or "local", "snr": snr}
            if i < n_out:
                outbound.append(entry)
            else:
                return_leg.append(entry)

        return {"outbound": outbound, "return": return_leg, "error": None}

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

    def _enrich_contact(self, contact: dict[str, Any]) -> dict[str, Any] | None:
        """Build the chat-friendly view of a single contact.

        Returns None if the contact has no name (and therefore can't be
        rendered usefully).
        """
        name = contact.get("adv_name", "")
        if not name:
            return None

        last_advert = contact.get("last_advert", 0)
        last_advert_str = _format_timestamp(last_advert) if last_advert else "unknown"

        # We've seen this node if either (a) it sent a message we caught
        # (state.last_seen, keyed by name) or (b) we received an advert
        # from it (state.adverts, keyed by pubkey). Use the most recent.
        candidates: list[tuple[float, str]] = []
        msg_seen = self.state.get_last_seen(name)
        if msg_seen:
            candidates.append(
                (float(msg_seen["time"]),
                 f"{_format_ago(msg_seen['time'])} on {msg_seen['channel']}")
            )
        pubkey = contact.get("public_key", "")
        adv_seen = self.state.get_advert_record(pubkey) if pubkey else None
        if adv_seen and adv_seen.get("last_seen"):
            candidates.append(
                (float(adv_seen["last_seen"]),
                 f"{_format_ago(adv_seen['last_seen'])} via advert")
            )
        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            bot_seen_str = candidates[0][1]
        else:
            bot_seen_str = "never seen by bot"

        out_path = contact.get("out_path", "")
        out_path_len = contact.get("out_path_len", -1)
        out_hash_mode = contact.get("out_path_hash_mode", 0)
        if out_path and out_path_len > 0:
            hash_size = (out_hash_mode + 1) if out_hash_mode >= 0 else 1
            known_route = "->".join(split_path_prefixes(out_path, hash_size))
        elif out_path_len == 0:
            known_route = "direct"
        elif out_path_len == -1:
            known_route = "flood"
        else:
            known_route = "unknown"

        recent_routes = self.state.get_recent_routes(name, limit=3)

        # Translate the firmware's out_path_len into something the agent
        # can't misread: meshcore uses 0 for "direct neighbour", -1 for
        # "flood / no fixed outbound path", positive N for an N-hop route.
        if out_path_len > 0:
            hops_value: Any = out_path_len
        elif out_path_len == 0:
            hops_value = "direct"
        else:
            hops_value = "flood"

        return {
            "name": name,
            "public_key": contact.get("public_key", "")[:12],
            "type": _contact_type_name(contact.get("type", 0)),
            "hops": hops_value,
            "known_route": known_route,
            "observed_routes": recent_routes,
            "last_advert": last_advert_str,
            "last_seen": bot_seen_str,
        }

    async def get_contacts_by_name(self, pattern: str) -> list[dict[str, Any]]:
        """Search contacts by name pattern (case-insensitive substring match)."""
        await self.mc.ensure_contacts()
        pattern_norm = _normalize(pattern)
        results = []
        for contact in self.mc.contacts.values():
            name = contact.get("adv_name", "")
            if not name or pattern_norm not in _normalize(name):
                continue
            entry = self._enrich_contact(contact)
            if entry:
                results.append(entry)
        return results

    async def get_contacts_by_prefix(self, prefix: str) -> list[dict[str, Any]]:
        """Search contacts whose public key starts with the given hex prefix."""
        await self.mc.ensure_contacts()
        prefix_lc = prefix.lower()
        results = []
        for contact in self.mc.contacts.values():
            pubkey = contact.get("public_key", "").lower()
            if not pubkey.startswith(prefix_lc):
                continue
            entry = self._enrich_contact(contact)
            if entry:
                results.append(entry)
        return results

    async def get_contact_routes(self, name: str, max_age_days: float = 7) -> list[dict[str, Any]]:
        """Get route history for contacts matching a name pattern.

        Returns routes seen in the last max_age_days, plus meshcore's
        known route as a fallback if we haven't observed any.
        """
        if self.mc:
            await self.mc.ensure_contacts()
        cutoff = time.time() - (max_age_days * 86400)
        results: list[dict[str, Any]] = []
        for contact_name, recent in self.state.routes_by_name_pattern(name, cutoff).items():
            if not recent:
                continue
            route_list = [
                {"route": r["route"], "hops": r["hops"], "when": _format_ago(r["time"])}
                for r in recent
            ]
            results.append({"name": contact_name, "routes": route_list})

        # Fallback: meshcore contacts' out_path if we have no observations
        if not results and self.mc:
            name_norm = _normalize(name)
            for contact in self.mc.contacts.values():
                cname = contact.get("adv_name", "")
                if not cname or name_norm not in _normalize(cname):
                    continue
                out_path = contact.get("out_path", "")
                out_path_len = contact.get("out_path_len", -1)
                if out_path and out_path_len > 0:
                    out_hash_mode = contact.get("out_path_hash_mode", 0)
                    hash_size = (out_hash_mode + 1) if out_hash_mode >= 0 else 1
                    route = "->".join(split_path_prefixes(out_path, hash_size))
                    results.append({
                        "name": cname,
                        "routes": [{"route": route, "hops": out_path_len, "when": "meshcore"}],
                    })

        return results

    def compute_clock_drift_stats(self, window_hours: float = 48) -> dict[str, Any]:
        """Statistical view of clock drift across nodes heard in the
        window. Delegates to the SQLite state store."""
        return self.state.compute_clock_drift_stats(window_hours)

    def iter_adverts(
        self, *, since: float = 0, repeater_only: bool = False
    ) -> Iterator[dict[str, Any]]:
        """Yield advert rows from state, newer than ``since``."""
        return self.state.iter_adverts(since=since, repeater_only=repeater_only)

    async def get_top_repeaters_grouped(
        self,
        exclude_prefixes: list[str] | None = None,
        limit: int | None = None,
    ) -> list[tuple[str, int]]:
        """Top repeaters by observed routes, deduped by resolved adv_name.

        Prefixes in exclude_prefixes are dropped from the result while their
        counts still contribute to total_routes. Unresolved hashes are
        labelled "unknown: <prefix>" so two unknowns with different prefixes
        stay distinct.
        """
        excluded = {p.lower() for p in (exclude_prefixes or [])}
        grouped: dict[str, int] = {}
        for prefix, count in self.state.iter_repeater_counts():
            if prefix.lower() in excluded:
                continue
            node = await self.get_node_by_prefix(prefix)
            name = node.get("adv_name") if node else None
            key = name or f"unknown: {prefix}"
            grouped[key] = grouped.get(key, 0) + count
        items = sorted(grouped.items(), key=lambda kv: kv[1], reverse=True)
        if limit is not None:
            items = items[:limit]
        return items

    def get_recent_adverts(
        self, name_filter: str = "", limit: int = 10
    ) -> list[dict[str, Any]]:
        """Return recent advertisements, newest first, optionally filtered
        by name (case- and accent-insensitive substring). Backed by SQLite."""
        rows = self.state.get_recent_adverts(name_filter=name_filter, limit=limit)
        out: list[dict[str, Any]] = []
        for r in rows:
            last_seen = r.get("last_seen") or 0
            path_len = r.get("last_path_len")
            entry: dict[str, Any] = {
                "name": r.get("name") or "",
                "public_key": (r.get("pubkey") or "")[:12],
                "type": _contact_type_name(r.get("adv_type") or 0),
                "last_seen": _format_ago(last_seen) if last_seen else "unknown",
                "drift_seconds": r.get("last_drift"),
                "advert_hops": path_len if path_len is not None else "?",
            }
            # SNR/RSSI on a flooded advert describe only the last-hop
            # link to whoever rebroadcast it, NOT the link to the
            # advertised node. Only include them when the advert
            # reached us direct (path_len == 0).
            if path_len == 0:
                entry["snr"] = r.get("last_snr")
                entry["rssi"] = r.get("last_rssi")
            lat, lon = r.get("lat"), r.get("lon")
            if lat is not None and lon is not None:
                entry["loc"] = f"{lat:.4f},{lon:.4f}"
            out.append(entry)
        return out

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
