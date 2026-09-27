"""UPB to MQTT bridge with Home Assistant discovery.

upb-lib owns the PIM (same library and state model as HA's UPB integration);
this module maps its devices and links onto MQTT. Device state is published
retained, so Home Assistant restarts cost no powerline traffic.

Topics (prefix defaults to "upb"; <idx> is upb-lib's index, e.g. 1_14_0 / 1_17):
  <prefix>/status                      bridge online/offline (LWT, retained)
  <prefix>/pim/status                  PIM online/offline (retained)
  <prefix>/pim/signal                  {"signal_strength", "noise_floor", "noise_counts"}
                                       from the PIM's own registers (retained)
  <prefix>/cmd                         {"command": "sync"}
  <prefix>/light/<idx>/state           {"state": "ON", "brightness": 0-100} (retained)
  <prefix>/light/<idx>/attributes      static device info (retained)
  <prefix>/light/<idx>/set             HA JSON light schema command; commands
                                       arriving together (a light group) are
                                       sent as a link command where one matches
  <prefix>/light/<idx>/cmd             fade_start | fade_stop | blink | update_status
  <prefix>/link/<idx>/set              "ON" activates (HA scene)
  <prefix>/link/<idx>/cmd              activate | deactivate | goto | fade_start |
                                       fade_stop | blink
  <prefix>/link/<idx>/event            {"event_type": "activated", ...} (not retained)
  <prefix>/link/<idx>/attributes       link membership (retained)
  <prefix>/keypad/<idx>/leds           keypad LEDs as last known (retained)
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import re
import signal
import urllib.request
from pathlib import Path
from typing import Any

import aiomqtt
from upb_lib import UpbPim
from upb_lib import connection as upb_connection
from upb_lib.const import PimCommand, UpbCommand
from upb_lib.devices import UpbAddr, UpbDevice
from upb_lib.links import UPB_COMMAND_TO_ACTION_MAPPING, Link
from upb_lib.message import PIM_ID

from keypads import INDICATE, LEVEL_ON, Keypad, Members, parse_keypads, scene_active
from scenes import ACTIVATE, Scene, plan
from txcount import TxCounts, TxEncoder

VERSION = "0.5.0"
LOG = logging.getLogger("upb2mqtt")

OPTIONS_FILE = Path("/data/options.json")
DISCOVERY_STATE_FILE = Path("/data/discovery_topics.json")

EVENT_TYPES = sorted(UPB_COMMAND_TO_ACTION_MAPPING.values())
# HA's JSON light sends flash as flash_time_short/long (integer seconds), so
# discovery sets those to 1/2 and they are mapped back to the blink periods
# HA's UPB light used for short/long flash.
FLASH_TIME_SHORT, FLASH_TIME_LONG = 1, 2
FLASH_SECONDS = {FLASH_TIME_SHORT: 0.5, FLASH_TIME_LONG: 1.5, "short": 0.5, "long": 1.5}

# PIM setup registers (PCS PIM Description v1.7): 0xF9 signal strength of the
# last message received, 0xFA noise floor over the last 128 half-cycles,
# 0xFB-0xFF the raw noise counts behind it. Reading them is local to the PIM's
# serial link; nothing is sent on the powerline.
SIGNAL_REGISTER, SIGNAL_REGISTER_COUNT = 0xF9, 7
SIGNAL_INTERVAL = 60  # seconds between reads (also read on every PIM connect)

# tx_count "auto": a device counts as answering a single transmission only if
# it answers every one of these queries in a row; the rest get TX_AUTO_REPEAT.
# Measured on a real PIM, replies to a single transmission arrive 0.74-0.99 s
# after the query is sent (the query 0.35 s, the reply about as long).
TX_PROBES = 2
TX_PROBE_TIMEOUT = 1.5
TX_AUTO_REPEAT = 2
UPB_RESPONSE_TIME = upb_connection.MESSAGE_RESPONSE_TIME


def slugify(text: str) -> str:
    """Approximation of homeassistant.util.slugify, for default entity ids."""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_") or "unknown"


def brightness_pct(payload: dict[str, Any]) -> float:
    """Level from a command: brightness (0-255) or brightness_pct, as HA's services."""
    if (brightness := payload.get("brightness")) is not None:
        return round(float(brightness) / 2.55)
    return float(payload.get("brightness_pct", 100))


def device_level(device: UpbDevice, brightness: float) -> int:
    """The level a device ends up at, as upb-lib clamps it (relays: 0 or 100)."""
    level = max(0, min(100, round(brightness)))
    return 100 if level and not device.dimmable else level


def register_read(start: int, count: int) -> str:
    """Body of a PIM "read registers" command: start, count, checksum."""
    return f"{start:02X}{count:02X}{-(start + count) & 0xFF:02X}"


def blink_pulses(payload: dict[str, Any]) -> int:
    """Blink rate in seconds -> 60Hz pulses, as HA's UPB services."""
    return int(float(payload.get("blink_rate", 0.5)) * 60)


class Bridge:
    """Glue between upb-lib elements and MQTT topics."""

    def __init__(self, opts: argparse.Namespace) -> None:
        self.opts = opts
        self.prefix = opts.topic_prefix.rstrip("/")
        self.disc_prefix = opts.discovery_prefix.rstrip("/")
        self.pim = UpbPim({"url": opts.pim_url, "UPStartExportFile": opts.upe_file})

        self.retained: dict[str, str] = {}  # topic -> payload; replayed on reconnect
        self.outbox: asyncio.Queue[tuple[str, str, bool]] = asyncio.Queue()
        self.connected_to_broker = False
        self.discovery: dict[str, str] = {}

        self.keypads: dict[int, Keypad] = {}  # upb_id -> keypad
        self.managed_leds: dict[int, list[int]] = {}  # upb_id -> LED bits we drive
        self.device_keypads: dict[str, set[int]] = {}  # device index -> keypads
        self.link_keypads: dict[int, set[int]] = {}  # link id -> keypads
        self.keypad_timers: dict[int, asyncio.TimerHandle] = {}

        self.scenes: list[Scene] = []
        self.batch: dict[str, tuple[UpbDevice, dict[str, Any]]] = {}  # index -> command
        self.batch_timer: asyncio.TimerHandle | None = None
        self.fading: dict[str, float] = {}  # device index -> loop time its fade ends
        self.signal_enabled = False

        mode = str(opts.tx_count).strip().lower()
        if mode != "auto" and mode not in ("1", "2", "3", "4"):
            raise SystemExit(f"tx_count must be a number or auto, not {opts.tx_count!r}")
        self.tx_auto = mode == "auto"
        self.tx_default = TX_AUTO_REPEAT if self.tx_auto else int(mode)
        self.tx: TxCounts | None = None
        self.tx_names: dict[int, str] = {}  # every UPB unit, keypads included
        self.tx_probing: tuple[int, asyncio.Future[bool]] | None = None
        self.tx_task: asyncio.Task[None] | None = None

    # ---- topics ----------------------------------------------------------

    @property
    def status_topic(self) -> str:
        return f"{self.prefix}/status"

    @property
    def pim_topic(self) -> str:
        return f"{self.prefix}/pim/status"

    def light_topic(self, device: UpbDevice, leaf: str) -> str:
        return f"{self.prefix}/light/{device.index}/{leaf}"

    def link_topic(self, link: Link, leaf: str) -> str:
        return f"{self.prefix}/link/{link.index}/{leaf}"

    @property
    def signal_topic(self) -> str:
        return f"{self.prefix}/pim/signal"

    def keypad_topic(self, keypad: Keypad, leaf: str) -> str:
        return f"{self.prefix}/keypad/{self.pim.network_id}_{keypad.upb_id}/{leaf}"

    # ---- setup -----------------------------------------------------------

    async def load(self) -> None:
        await self.pim.load_upstart_file()
        if not self.pim.config_ok or self.pim.network_id is None:
            raise SystemExit(f"Cannot load UPStart export file {self.opts.upe_file}")
        # The app option wins over a tx_count in the UPE file's "98," flags record
        if self.pim.flags.get("tx_count") not in (None, self.tx_default) or (
            self.tx_auto and "tx_count" in self.pim.flags
        ):
            LOG.warning("Ignoring tx_count=%s in %s; using the tx_count option (%s)",
                        self.pim.flags["tx_count"], self.opts.upe_file, self.opts.tx_count)
        self.pim.flags["tx_count"] = self.opts.tx_count
        # Keypads are controllers: their presses arrive as link events, and they
        # have no load to show as a light. Dropping them also keeps upb-lib's
        # sync from querying them.
        devices = self.pim.devices.elements
        for device in sorted(devices.values(), key=lambda d: d.addr.channel, reverse=True):
            self.tx_names[device.addr.upb_id] = device.name  # channel 0's name wins
        self.tx_names = dict(sorted(self.tx_names.items()))
        keypads = [index for index, device in devices.items() if device.isKeypad()]
        keypad_ids = {devices[index].addr.upb_id for index in keypads}
        for index in keypads:
            del devices[index]
        all_keypads = parse_keypads(self.opts.upe_file, keypad_ids) if keypad_ids else {}
        self._load_tx_counts(all_keypads)
        LOG.info(
            "Loaded %s: network %d, %d device channels (%d keypads skipped), "
            "%d links, flags %s",
            self.opts.upe_file,
            self.pim.network_id,
            len(devices),
            len(keypads),
            len(self.pim.links.elements),
            self.pim.flags,
        )

        self.pim.add_handler("connected", lambda: self._set_pim_status(True))
        self.pim.add_handler("disconnected", lambda: self._set_pim_status(False))
        self._set_pim_status(False)
        if self.opts.keypad_sync and all_keypads:
            self._load_keypads(all_keypads)
        self.signal_enabled = self._watch_register_reports()
        if self.signal_enabled:
            self.pim.add_handler("connected", self._read_signal)

        for device in self.pim.devices.elements.values():
            device.add_callback(self._device_changed)
        self._publish_device_attributes()
        for link in self.pim.links.elements.values():
            link.add_callback(self._link_changed)
            self._publish(
                self.link_topic(link, "attributes"),
                {
                    "address": link.index,
                    "link_id": link.addr.upb_id,
                    "devices": [
                        {"device": d.device_id, "level": d.device_level}
                        for d in link.devices
                    ],
                },
            )
        self.scenes = self._load_scenes()
        self.discovery = self._build_discovery()

    def _load_scenes(self) -> list[Scene]:
        """Links that can stand in for a batch of light commands."""
        scenes = []
        devices = self.pim.devices.elements
        for link in self.pim.links.elements.values():
            # A link that also drives something we don't model can't be used
            if not link.devices or any(d.device_id not in devices for d in link.devices):
                continue
            presets = {
                d.device_id: device_level(devices[d.device_id], d.device_level)
                for d in link.devices
            }
            scenes.append(Scene(link.addr.upb_id, link.name, presets))
        return scenes

    def _load_keypads(self, keypads: dict[int, Keypad]) -> None:
        self.keypads = keypads
        for upb_id, keypad in self.keypads.items():
            bits = keypad.managed_bits(lambda link_id: bool(self._members(link_id)))
            self.managed_leds[upb_id] = bits
            for bit in bits:
                for link_id in keypad.on_rows(bit):
                    link = self._link(link_id)
                    for device_link in link.devices if link else []:
                        self.device_keypads.setdefault(device_link.device_id, set()).add(upb_id)
            for link_id in keypad.links():
                self.link_keypads.setdefault(link_id, set()).add(upb_id)
            LOG.info(
                "Keypad %s (%d): syncing LEDs %s",
                keypad.name,
                upb_id,
                ", ".join(keypad.label(bit) for bit in bits) or "none",
            )
        # Queued after upb-lib's own connect-time device sync
        self.pim.add_handler("connected", self._query_keypads)
        self.pim.add_handler(UpbCommand.DEVICE_STATE_REPORT.value, self._keypad_report)

    def _publish_device_attributes(self) -> None:
        for device in self.pim.devices.elements.values():
            self._publish(
                self.light_topic(device, "attributes"),
                {
                    "address": device.index,
                    "upb_id": device.addr.upb_id,
                    "channel": device.addr.channel,
                    "kind": device.kind,
                    "manufacturer": device.manufacturer,
                    "product": device.product,
                    "version": device.version,
                    "dimmable": device.dimmable,
                    "tx_count": self.tx.device(device.addr.upb_id),
                },
            )

    # ---- state -> MQTT ---------------------------------------------------

    def _publish(self, topic: str, payload: Any, retain: bool = True) -> None:
        if not isinstance(payload, str):
            payload = json.dumps(payload)
        if retain:
            if self.retained.get(topic) == payload:
                return
            self.retained[topic] = payload
        if self.connected_to_broker:
            self.outbox.put_nowait((topic, payload, retain))

    def _set_pim_status(self, online: bool) -> None:
        LOG.info("PIM %s", "connected" if online else "disconnected")
        self._publish(self.pim_topic, "online" if online else "offline")

    def _device_changed(self, device: UpbDevice, changeset: dict[str, Any]) -> None:
        status = device.status
        if not isinstance(status, int) or status < 0:
            return  # unknown (never reported, or goto without a level)
        state: dict[str, Any] = {"state": "ON" if status else "OFF"}
        if device.dimmable:
            state["brightness"] = min(status, 100)
        LOG.debug("%s (%s) -> %s", device.name, device.index, state)
        self._publish(self.light_topic(device, "state"), state)
        for upb_id in self.device_keypads.get(device.index, ()):
            self._schedule_keypad(upb_id)

    def _link_changed(self, link: Link, changeset: dict[str, Any]) -> None:
        if not (change := changeset.get("last_change")) or not change.get("command"):
            return
        event = {
            "event_type": change["command"],
            "address": link.index,
            "brightness_pct": change.get("level", -1),
            "rate": change.get("rate", -1),
        }
        LOG.debug("Link %s (%s) %s", link.name, link.index, event)
        self._publish(self.link_topic(link, "event"), event, retain=False)
        for upb_id in self.link_keypads.get(link.addr.upb_id, ()):
            keypad = self.keypads[upb_id]
            keypad.apply_link(link.addr.upb_id, change["command"], change.get("level", -1))
            self._publish_leds(keypad)
            self._schedule_keypad(upb_id)

    # ---- PIM signal / noise ----------------------------------------------

    def _watch_register_reports(self) -> bool:
        """See PIM register reports, which upb-lib reads but discards.

        Wraps upb-lib's private line handler (upb-lib 0.7.2); every line still
        goes on to upb-lib unchanged.
        """
        # pylint: disable=protected-access
        connection = getattr(self.pim, "_connection", None)
        original = getattr(connection, "_handle_pim_command", None)
        if original is None:
            LOG.warning("This upb-lib hides PIM register reports; no signal/noise sensors")
            return False

        async def handle(command: str, data: str) -> None:
            if command == "PR":
                try:
                    self._register_report(data)
                except Exception:  # pylint: disable=broad-except
                    LOG.exception("Error handling PIM register report %r", data)
            await original(command, data)

        connection._handle_pim_command = handle
        return True

    def _read_signal(self) -> None:
        self.pim.send(
            register_read(SIGNAL_REGISTER, SIGNAL_REGISTER_COUNT),
            command=PimCommand.READ_PIM_REGISTERS,
        )

    def _register_report(self, data: str) -> None:
        try:
            start, values = int(data[:2], 16), bytes.fromhex(data[2:])
        except ValueError:
            return
        if start != SIGNAL_REGISTER or len(values) < SIGNAL_REGISTER_COUNT:
            return  # e.g. upb-lib's own heartbeat read
        LOG.debug("PIM signal %d, noise floor %d, noise counts %s",
                  values[0], values[1], values[2:SIGNAL_REGISTER_COUNT].hex())
        self._publish(
            self.signal_topic,
            {
                "signal_strength": values[0],
                "noise_floor": values[1],
                "noise_counts": list(values[2:SIGNAL_REGISTER_COUNT]),
            },
        )

    async def run_signal(self) -> None:
        """Poll the PIM's signal/noise registers (also read on every connect)."""
        if not self.signal_enabled:
            return
        while True:
            await asyncio.sleep(SIGNAL_INTERVAL)
            if self.pim.is_connected():
                self._read_signal()

    # ---- transmit counts -------------------------------------------------

    def _load_tx_counts(self, keypads: dict[int, Keypad]) -> None:
        self.tx = TxCounts(self.tx_default)
        # A link is heard by the devices in it and by keypads showing it on an LED
        for link in self.pim.links.elements.values():
            self.tx.add_listeners(
                link.addr.upb_id, (int(d.device_id.split("_")[1]) for d in link.devices)
            )
        for upb_id, keypad in keypads.items():
            for link_id in keypad.links():
                self.tx.add_listeners(link_id, [upb_id])
        self.pim.encoder = TxEncoder(self.tx.for_addr)
        if not self.tx_auto:
            LOG.info("Transmit count %d for every device (tx_count option)", self.tx_default)
            return
        # Reading each device with a single transmission first replaces upb-lib's
        # device sync: on connect, the refresh button and every refresh_hours
        self.pim.devices.sync = self._start_tx_probe
        self.pim.add_handler("disconnected", self._stop_tx_probe)
        self.pim.add_handler("timeout", self._tx_timeout)
        self.pim.add_handler(UpbCommand.DEVICE_STATE_REPORT.value, self._tx_report)
        # A device missing a single transmission is expected, not worth a warning
        logging.getLogger("upb_lib.upb").addFilter(
            lambda record: not (self.tx_probing and record.getMessage().startswith("Timeout"))
        )

    def _start_tx_probe(self) -> None:
        self._stop_tx_probe()
        self.tx_task = asyncio.get_running_loop().create_task(self._probe_tx())

    def _stop_tx_probe(self) -> None:
        if self.tx_task:
            self.tx_task.cancel()
            self.tx_task = None

    async def _probe_tx(self) -> None:
        """Read every device, learning which answer a message sent only once."""
        loop = asyncio.get_running_loop()
        started = loop.time()
        LOG.info("Reading %d devices, single transmission first", len(self.tx_names))
        devices = {d.addr.upb_id: d for d in self.pim.devices.elements.values()
                   if d.addr.channel == 0}
        for upb_id in self.tx_names:
            single = True
            for _ in range(TX_PROBES):
                if not await self._probe_once(upb_id):
                    single = False
                    break
            self.tx.single[upb_id] = single
            if not single and (device := devices.get(upb_id)):
                device.update_status()  # its state, at the repeat count
        self.tx_task = None
        self._publish_device_attributes()
        LOG.info("Transmit counts learned in %.0f s", loop.time() - started)
        for count, single in ((1, True), (self.tx_default, False)):
            ids = [i for i in self.tx_names if self.tx.single.get(i) is single]
            LOG.info("Transmit count %d, %d devices:%s", count, len(ids),
                     "".join(f"\n  {i:3} {self.tx_names[i]}" for i in ids))

    async def _probe_once(self, upb_id: int) -> bool:
        # Wait for the PIM queue to empty: probes go after other traffic, and
        # a command waits for at most one probe
        # pylint: disable=protected-access
        connection = self.pim._connection
        while connection._write_queue:
            await asyncio.sleep(0.1)
        future = asyncio.get_running_loop().create_future()
        self.tx_probing = (upb_id, future)
        try:
            addr = UpbAddr(self.pim.network_id, upb_id, 0)
            # Control word 0: one transmission, no ack, no link
            self.pim.send(self.pim.encoder.report_state(addr, ctl=0),
                          bytearray([self.pim.network_id, upb_id]))
            connection._write_queue[-1].retry_count = 0  # no second try
            # upb-lib reads its response timeout when it sends; the next
            # message must get the normal one again (see _end_probe)
            upb_connection.MESSAGE_RESPONSE_TIME = TX_PROBE_TIMEOUT
            return await future
        finally:
            self._end_probe(False)
            self.tx_probing = None

    def _end_probe(self, answered: bool) -> None:
        upb_connection.MESSAGE_RESPONSE_TIME = UPB_RESPONSE_TIME
        if self.tx_probing and not self.tx_probing[1].done():
            self.tx_probing[1].set_result(answered)

    def _tx_report(self, msg) -> None:
        # A device answers with as many copies as it was sent, so a reply sent
        # once is the answer to the probe, not to an earlier query
        if self.tx_probing and self.tx_probing[0] == msg.src_id and not msg.link \
                and msg.tx_count == 0:
            self._end_probe(True)

    def _tx_timeout(self, addr: bytearray | None) -> None:
        if not addr:
            return
        upb_id = addr[1]
        if self.tx_probing and self.tx_probing[0] == upb_id:
            self._end_probe(False)
        elif self.tx.single.get(upb_id):
            LOG.warning("Device %d stopped answering single transmissions; now sending %d",
                        upb_id, self.tx_default)
            self.tx.single[upb_id] = False
            self._publish_device_attributes()

    # ---- keypad LEDs -----------------------------------------------------

    def _link(self, link_id: int) -> Link | None:
        return self.pim.links.elements.get(f"{self.pim.network_id}_{link_id}")

    def _members(self, link_id: int) -> Members:
        """(level, preset) of each light in a link; level None if unknown."""
        members: Members = []
        link = self._link(link_id)
        for device_link in link.devices if link else []:
            if device := self.pim.devices.elements.get(device_link.device_id):
                status = device.status
                level = status if isinstance(status, int) and status >= 0 else None
                members.append((level, device_link.device_level))
        return members

    def _desired_led(self, keypad: Keypad, bit: int) -> bool | None:
        """Lit if any scene that turns the LED on is active; None if unknown."""
        states = [
            scene_active(self._members(link_id), self.opts.keypad_scene_match)
            for link_id in keypad.on_rows(bit)
        ]
        if True in states:
            return True
        return None if None in states else False

    def _query_keypads(self) -> None:
        for upb_id in self.keypads:
            addr = UpbAddr(self.pim.network_id, upb_id, 0)
            self.pim.send(
                self.pim.encoder.report_state(addr),
                bytearray([self.pim.network_id, upb_id]),
            )

    def _keypad_report(self, msg) -> None:
        if msg.link or not msg.data or not (keypad := self.keypads.get(msg.src_id)):
            return
        if self.tx_probing and self.tx_probing[0] == msg.src_id:
            return  # may predate an Indicate still queued; read at connect anyway
        keypad.set_report(msg.data[0])
        LOG.debug("Keypad %s reports LEDs %02X", keypad.name, msg.data[0])
        self._publish_leds(keypad)
        self._schedule_keypad(keypad.upb_id)

    def _schedule_keypad(self, upb_id: int) -> None:
        # Let a scene finish and the keypad react to it before comparing
        if timer := self.keypad_timers.get(upb_id):
            timer.cancel()
        self.keypad_timers[upb_id] = asyncio.get_running_loop().call_later(
            self.opts.keypad_delay, self._reconcile_keypad, upb_id
        )

    def _reconcile_keypad(self, upb_id: int) -> None:
        self.keypad_timers.pop(upb_id, None)
        keypad = self.keypads[upb_id]
        if not keypad.initialized or not self.pim.is_connected():
            return  # its state report (queued on connect) will reschedule
        addr = UpbAddr(self.pim.network_id, upb_id, 0)
        for bit in self.managed_leds[upb_id]:
            desired = self._desired_led(keypad, bit)
            if desired is None or keypad.leds.get(bit) == desired:
                continue
            LOG.debug("Keypad %s: %s LED %s", keypad.name, keypad.label(bit), "on" if desired else "off")
            level = LEVEL_ON if desired else 0
            # pylint: disable=protected-access
            self.pim.send(
                self.pim.encoder._encode_message(
                    -1, addr, PIM_ID, INDICATE, bytearray([level, 0, bit + 1])
                )
            )
            keypad.leds[bit] = desired
        self._publish_leds(keypad)

    def _publish_leds(self, keypad: Keypad) -> None:
        if not keypad.initialized:
            return
        self._publish(
            self.keypad_topic(keypad, "leds"),
            {
                "leds": {keypad.label(bit): state for bit, state in keypad.leds.items()
                         if bit in keypad.labels},
                "synced": [keypad.label(bit) for bit in self.managed_leds[keypad.upb_id]],
            },
        )

    def _sync_all(self) -> None:
        self.pim.devices.sync()
        self._query_keypads()

    # ---- discovery -------------------------------------------------------

    def _availability(self) -> dict[str, Any]:
        return {
            "availability": [{"topic": self.status_topic}, {"topic": self.pim_topic}],
            "availability_mode": "all",
        }

    def _origin(self) -> dict[str, str]:
        return {"name": "upb2mqtt", "sw_version": VERSION}

    def _bridge_device_id(self) -> str:
        return f"upb_{self.pim.network_id}_pim"

    def _build_discovery(self) -> dict[str, str]:
        """All discovery topics -> payloads."""
        configs: dict[str, dict[str, Any]] = {}

        # Bridge device: PIM connectivity + resync button
        bridge_id = self._bridge_device_id()
        configs[f"{self.disc_prefix}/device/{bridge_id}/config"] = {
            "device": {
                "identifiers": [bridge_id],
                "name": "UPB PIM",
                "manufacturer": "Powerline Control Systems",
                "model": "PIM",
                "sw_version": VERSION,
            },
            "origin": self._origin(),
            "components": {
                "pim": {
                    "platform": "binary_sensor",
                    "unique_id": f"{bridge_id}_connected",
                    "name": "Connected",
                    "device_class": "connectivity",
                    "entity_category": "diagnostic",
                    "state_topic": self.pim_topic,
                    "payload_on": "online",
                    "payload_off": "offline",
                    "availability_topic": self.status_topic,
                },
                **(self._signal_components(bridge_id) if self.signal_enabled else {}),
                "sync": {
                    "platform": "button",
                    "unique_id": f"{bridge_id}_sync",
                    "name": "Refresh device states",
                    "entity_category": "config",
                    "command_topic": f"{self.prefix}/cmd",
                    "payload_press": json.dumps({"command": "sync"}),
                    **self._availability(),
                },
            },
        }

        # One HA device per UPB device; one light per channel
        by_upb_id: dict[int, list[UpbDevice]] = {}
        for device in self.pim.devices.elements.values():
            by_upb_id.setdefault(device.addr.upb_id, []).append(device)
        for upb_id, channels in by_upb_id.items():
            first = channels[0]
            multi = len(channels) > 1
            device_id = f"upb_{self.pim.network_id}_{upb_id}"
            name = first.name
            if multi and name.endswith(f" {first.addr.channel}"):
                name = name.rsplit(" ", 1)[0]
            components = {}
            for device in channels:
                dimmable = bool(device.dimmable)
                components[f"light_{device.addr.channel}"] = {
                    "platform": "light",
                    "schema": "json",
                    "unique_id": f"upb_{device.index}",
                    "name": f"Channel {device.addr.channel + 1}" if multi else None,
                    # Same entity id the UPB integration produced
                    "default_entity_id": f"light.{slugify(device.name)}",
                    "command_topic": self.light_topic(device, "set"),
                    "state_topic": self.light_topic(device, "state"),
                    "json_attributes_topic": self.light_topic(device, "attributes"),
                    "supported_color_modes": ["brightness" if dimmable else "onoff"],
                    "brightness_scale": 100,
                    "flash": True,
                    "flash_time_short": FLASH_TIME_SHORT,
                    "flash_time_long": FLASH_TIME_LONG,
                    "transition": dimmable,
                    **self._availability(),
                }
            # A state query covers every channel of the device
            components["refresh"] = {
                "platform": "button",
                "unique_id": f"upb_{first.index}_refresh",
                "name": "Refresh state",
                "default_entity_id": f"button.{slugify(name)}_refresh_state",
                "entity_category": "diagnostic",
                "command_topic": self.light_topic(first, "cmd"),
                "payload_press": json.dumps({"command": "update_status"}),
                **self._availability(),
            }
            configs[f"{self.disc_prefix}/device/{device_id}/config"] = {
                "device": {
                    "identifiers": [device_id],
                    "name": name,
                    "manufacturer": first.manufacturer,
                    "model": first.product,
                    "sw_version": first.version,
                    "via_device": bridge_id,
                },
                "origin": self._origin(),
                "components": components,
            }

        # Links: device-less entities, as the UPB integration had them
        for link in self.pim.links.elements.values():
            base = f"upb_{link.index}"
            slug = slugify(link.name or base)
            common = {
                "json_attributes_topic": self.link_topic(link, "attributes"),
                "origin": self._origin(),
                **self._availability(),
            }
            configs[f"{self.disc_prefix}/scene/{base}/config"] = {
                "unique_id": f"upb_link_{link.index}",
                "name": link.name,
                "default_entity_id": f"scene.{slug}",
                "command_topic": self.link_topic(link, "set"),
                # Optional in HA's schema but read unconditionally on activation
                "payload_on": "ON",
                **common,
            }
            configs[f"{self.disc_prefix}/event/{base}/config"] = {
                "unique_id": f"upb_link_{link.index}_event",
                "name": f"{link.name} event",
                "default_entity_id": f"event.upb_link_{slug}",
                "state_topic": self.link_topic(link, "event"),
                "event_types": EVENT_TYPES,
                **common,
            }
            configs[f"{self.disc_prefix}/button/{base}_deactivate/config"] = {
                "unique_id": f"upb_link_{link.index}_deactivate",
                "name": f"{link.name} deactivate",
                "default_entity_id": f"button.upb_link_{slug}_deactivate",
                "command_topic": self.link_topic(link, "cmd"),
                "payload_press": json.dumps({"command": "deactivate"}),
                "enabled_by_default": False,
                **common,
            }

        return {topic: json.dumps(cfg) for topic, cfg in configs.items()}

    def _signal_components(self, bridge_id: str) -> dict[str, dict[str, Any]]:
        # Raw PIM units (0-255); no HA device_class fits, which would demand dB
        common = {
            "platform": "sensor",
            "entity_category": "diagnostic",
            "state_class": "measurement",
            "state_topic": self.signal_topic,
            **self._availability(),
        }
        return {
            "signal_strength": {
                **common,
                "unique_id": f"{bridge_id}_signal_strength",
                "name": "Signal strength",
                "icon": "mdi:signal",
                "value_template": "{{ value_json.signal_strength }}",
            },
            "noise_floor": {
                **common,
                "unique_id": f"{bridge_id}_noise_floor",
                "name": "Noise floor",
                "icon": "mdi:waveform",
                "value_template": "{{ value_json.noise_floor }}",
                "json_attributes_topic": self.signal_topic,
                "json_attributes_template":
                    "{{ {'noise_counts': value_json.noise_counts} | tojson }}",
            },
        }

    async def _publish_discovery(self, client: aiomqtt.Client) -> None:
        # Remove entities for elements that are no longer in the UPE file
        try:
            previous = set(json.loads(DISCOVERY_STATE_FILE.read_text()))
        except (OSError, ValueError):
            previous = set()
        for topic in previous - set(self.discovery):
            LOG.info("Removing stale discovery %s", topic)
            await client.publish(topic, "", qos=1, retain=True)

        for topic, payload in self.discovery.items():
            await client.publish(topic, payload, qos=1, retain=True)
        with contextlib.suppress(OSError):
            DISCOVERY_STATE_FILE.write_text(json.dumps(sorted(self.discovery)))
        LOG.info("Published %d discovery configs", len(self.discovery))

    # ---- MQTT -> UPB -----------------------------------------------------

    def _on_command(self, topic: str, raw: bytes) -> None:
        parts = topic[len(self.prefix) + 1 :].split("/")
        text = raw.decode("utf-8", "replace").strip()
        try:
            payload = json.loads(text) if text.startswith("{") else {"command": text}
        except ValueError:
            LOG.warning("Ignoring non-JSON payload on %s: %r", topic, text)
            return

        if not self.pim.is_connected():
            LOG.warning("PIM not connected; dropping %s %s", topic, text)
            return

        kind, index, leaf = parts if len(parts) == 3 else ("", "", "")
        device = self.pim.devices.elements.get(index) if kind == "light" else None
        if device and leaf == "set" and self._batch_light(device, payload):
            return
        self._flush_batch()  # anything else goes out after the batched commands

        if parts == ["cmd"]:
            if payload.get("command") == "sync":
                LOG.info("Refreshing all device states")
                self._sync_all()
            return
        if len(parts) != 3:
            return
        if device:
            self._note_fade([device.index], payload)
            if leaf == "set":
                self._light_set(device, payload)
            elif leaf == "cmd":
                self._light_cmd(device, payload)
        elif kind == "link" and (link := self.pim.links.elements.get(index)):
            if leaf == "set" and text.upper() in ("ON", "ACTIVATE"):
                link.activate()
            elif leaf == "cmd":
                self._note_fade([d.device_id for d in link.devices], payload)
                self._link_cmd(link, payload)
        else:
            LOG.warning("Unknown element in topic %s", topic)

    def _note_fade(self, indexes: list[str], payload: dict[str, Any]) -> None:
        """Remember lights we set fading: their recorded level isn't reached yet."""
        seconds = payload.get("transition", payload.get("rate", -1))
        if not isinstance(seconds, (int, float)) or seconds <= 0:
            return
        loop = asyncio.get_running_loop()
        self.fading = {i: end for i, end in self.fading.items() if end > loop.time()}
        for index in indexes:
            self.fading[index] = loop.time() + seconds

    def _batch_light(self, device: UpbDevice, payload: dict[str, Any]) -> bool:
        """Hold a light command briefly, in case a group's other lights follow."""
        if self.opts.batch_window <= 0 or "flash" in payload or "transition" in payload:
            return False
        self.batch.pop(device.index, None)  # a repeat replaces, and moves to the end
        self.batch[device.index] = (device, payload)
        if not self.batch_timer:
            self.batch_timer = asyncio.get_running_loop().call_later(
                self.opts.batch_window, self._flush_batch
            )
        return True

    def _flush_batch(self) -> None:
        """Send held light commands, as link commands where a scene matches."""
        if self.batch_timer:
            self.batch_timer.cancel()
            self.batch_timer = None
        batch, self.batch = self.batch, {}
        if not batch:
            return
        if not self.pim.is_connected():
            LOG.warning("PIM not connected; dropping commands for %s", ", ".join(batch))
            return
        levels = {
            index: device_level(device, 0 if payload.get("state") == "OFF"
                                else payload.get("brightness", 100))
            for index, (device, payload) in batch.items()
        }
        now = asyncio.get_running_loop().time()
        current = {
            index: device_level(device, device.status)
            for index, device in self.pim.devices.elements.items()
            if index not in levels and isinstance(device.status, int) and device.status >= 0
            and self.fading.get(index, 0) <= now  # a scene would cut the fade short
        }
        chosen, rest = plan(levels, self.scenes, current, self.link_keypads.keys())
        for scene, action in chosen:
            link = self.pim.links.elements[f"{self.pim.network_id}_{scene.link_id}"]
            LOG.debug("%d lights -> %s %s (%s)", len(scene.presets), action, link.name, link.index)
            if action == ACTIVATE:
                link.activate()
            else:
                link.deactivate()
        for index, (device, payload) in batch.items():
            if index in rest:
                self._light_set(device, payload)

    @staticmethod
    def _light_set(device: UpbDevice, payload: dict[str, Any]) -> None:
        """HA JSON schema light command, mapped exactly as HA's UpbLight does."""
        if flash := payload.get("flash"):
            device.blink(int(FLASH_SECONDS.get(flash, 0.5) * 60))
            return
        rate = payload.get("transition", -1)
        if payload.get("state") == "OFF":
            device.turn_off(rate)
        else:
            device.turn_on(payload.get("brightness", 100), rate)

    @staticmethod
    def _light_cmd(device: UpbDevice, payload: dict[str, Any]) -> None:
        command = payload.get("command")
        if command == "fade_start":
            device.fade_start(brightness_pct(payload), payload.get("rate", -1))
        elif command == "fade_stop":
            device.fade_stop()
        elif command == "blink":
            device.blink(blink_pulses(payload))
        elif command == "update_status":
            device.update_status()
        else:
            LOG.warning("Unknown light command %r for %s", command, device.index)

    @staticmethod
    def _link_cmd(link: Link, payload: dict[str, Any]) -> None:
        command = payload.get("command")
        if command == "activate":
            link.activate()
        elif command == "deactivate":
            link.deactivate()
        elif command == "goto":
            link.goto(brightness_pct(payload), payload.get("rate", -1))
        elif command == "fade_start":
            link.fade_start(brightness_pct(payload), payload.get("rate", -1))
        elif command == "fade_stop":
            link.fade_stop()
        elif command == "blink":
            link.blink(blink_pulses(payload))
        else:
            LOG.warning("Unknown link command %r for %s", command, link.index)

    # ---- main loops ------------------------------------------------------

    async def run_mqtt(self, broker: dict[str, Any]) -> None:
        will = aiomqtt.Will(self.status_topic, "offline", qos=1, retain=True)
        ha_status = f"{self.disc_prefix}/status"
        retry = 1
        while True:
            try:
                async with aiomqtt.Client(
                    **broker, identifier=f"upb2mqtt-{self.pim.network_id}", will=will
                ) as client:
                    LOG.info("Connected to MQTT broker %s:%s", broker["hostname"], broker["port"])
                    retry = 1
                    for leaf in ("light/+/set", "light/+/cmd", "link/+/set", "link/+/cmd", "cmd"):
                        await client.subscribe(f"{self.prefix}/{leaf}", qos=1)
                    await client.subscribe(ha_status, qos=1)

                    await client.publish(self.status_topic, "online", qos=1, retain=True)
                    await self._publish_discovery(client)
                    await self._replay_retained(client)
                    self.connected_to_broker = True
                    sender = asyncio.create_task(self._sender(client))
                    try:
                        async for message in client.messages:
                            topic = message.topic.value
                            if topic == ha_status:
                                if message.payload == b"online":
                                    LOG.info("Home Assistant came online; republishing")
                                    await self._publish_discovery(client)
                                    await self._replay_retained(client)
                                continue
                            LOG.debug("MQTT <- %s %r", topic, message.payload)
                            try:
                                self._on_command(topic, message.payload)
                            except Exception:  # pylint: disable=broad-except
                                LOG.exception("Error handling %s", topic)
                    finally:
                        self.connected_to_broker = False
                        sender.cancel()
                        await self._shutdown_publish(client)
            except aiomqtt.MqttError as err:
                LOG.warning("MQTT connection error (%s); retry in %ds", err, retry)
                await asyncio.sleep(retry)
                retry = min(60, retry * 2)

    async def _shutdown_publish(self, client: aiomqtt.Client) -> None:
        # The LWT only fires on unclean disconnects; say goodbye explicitly.
        with contextlib.suppress(aiomqtt.MqttError, asyncio.CancelledError):
            await asyncio.shield(client.publish(self.status_topic, "offline", qos=1, retain=True))

    async def _replay_retained(self, client: aiomqtt.Client) -> None:
        while not self.outbox.empty():
            self.outbox.get_nowait()
        for topic, payload in list(self.retained.items()):
            await client.publish(topic, payload, qos=1, retain=True)

    async def _sender(self, client: aiomqtt.Client) -> None:
        while True:
            topic, payload, retain = await self.outbox.get()
            await client.publish(topic, payload, qos=1, retain=retain)

    async def run_refresh(self) -> None:
        """Optional periodic resync, to correct drift from untracked changes."""
        if self.opts.refresh_hours <= 0:
            return
        while True:
            await asyncio.sleep(self.opts.refresh_hours * 3600)
            if self.pim.is_connected():
                LOG.info("Periodic device state refresh")
                self._sync_all()


def supervisor_mqtt() -> dict[str, Any]:
    """Broker credentials from the Supervisor's MQTT service, if available."""
    if not (token := os.environ.get("SUPERVISOR_TOKEN")):
        LOG.warning("SUPERVISOR_TOKEN not set; cannot ask the Supervisor for MQTT")
        return {}
    request = urllib.request.Request(
        "http://supervisor/services/mqtt", headers={"Authorization": f"Bearer {token}"}
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            data = json.load(response)["data"]
    except Exception as err:  # pylint: disable=broad-except
        LOG.warning("No MQTT service from Supervisor (%s)", err)
        return {}
    return {
        "hostname": data["host"],
        "port": int(data["port"]),
        "username": data.get("username"),
        "password": data.get("password"),
    }


def parse_args() -> argparse.Namespace:
    options: dict[str, Any] = {
        "pim_url": "",
        "upe_file": "/homeassistant/network.upe",
        "topic_prefix": "upb",
        "discovery_prefix": "homeassistant",
        "refresh_hours": 0,
        "keypad_sync": True,
        "keypad_scene_match": "all",
        "keypad_delay": 1.5,
        "batch_window": 0.15,
        "tx_count": "auto",
        "mqtt_host": "",
        "mqtt_port": 1883,
        "mqtt_username": "",
        "mqtt_password": "",
        "log_level": "info",
    }
    if OPTIONS_FILE.exists():
        options.update(
            {k: v for k, v in json.loads(OPTIONS_FILE.read_text()).items() if v is not None}
        )
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for key, value in options.items():
        if isinstance(value, bool):
            kind: Any = lambda text: text.lower() in ("1", "true", "yes", "on")
        else:
            kind = type(value) if isinstance(value, (int, float)) else str
        parser.add_argument(f"--{key.replace('_', '-')}", type=kind, default=value)
    return parser.parse_args()


async def main() -> None:
    opts = parse_args()
    logging.basicConfig(
        level=opts.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if not opts.pim_url:
        raise SystemExit("pim_url is not configured")

    broker = supervisor_mqtt()
    if opts.mqtt_host:
        broker = {"hostname": opts.mqtt_host, "port": opts.mqtt_port,
                  "username": opts.mqtt_username or None, "password": opts.mqtt_password or None}
    if not broker:
        raise SystemExit("No MQTT broker: install the Mosquitto add-on or set mqtt_host")

    bridge = Bridge(opts)
    await bridge.load()

    main_task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, main_task.cancel)

    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(bridge.run_mqtt(broker))
            group.create_task(bridge.pim.async_connect())
            group.create_task(bridge.run_refresh())
            group.create_task(bridge.run_signal())
    except asyncio.CancelledError:
        LOG.info("Shutting down")
        bridge.pim.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
