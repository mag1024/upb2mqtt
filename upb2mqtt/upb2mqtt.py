"""UPB to MQTT bridge with Home Assistant discovery.

upb-lib owns the PIM (same library and state model as HA's UPB integration);
this module maps its devices and links onto MQTT. Device state is published
retained, so Home Assistant restarts cost no powerline traffic.

Topics (prefix defaults to "upb"; <idx> is upb-lib's index, e.g. 1_14_0 / 1_17):
  <prefix>/status                      bridge online/offline (LWT, retained)
  <prefix>/pim/status                  PIM online/offline (retained)
  <prefix>/cmd                         {"command": "sync"}
  <prefix>/light/<idx>/state           {"state": "ON", "brightness": 0-100} (retained)
  <prefix>/light/<idx>/attributes      static device info (retained)
  <prefix>/light/<idx>/set             HA JSON light schema command
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
from upb_lib.const import UpbCommand
from upb_lib.devices import UpbAddr, UpbDevice
from upb_lib.links import UPB_COMMAND_TO_ACTION_MAPPING, Link
from upb_lib.message import PIM_ID

from keypads import INDICATE, LEVEL_ON, Keypad, Members, parse_keypads, scene_active

VERSION = "0.2.0"
LOG = logging.getLogger("upb2mqtt")

OPTIONS_FILE = Path("/data/options.json")
DISCOVERY_STATE_FILE = Path("/data/discovery_topics.json")

EVENT_TYPES = sorted(UPB_COMMAND_TO_ACTION_MAPPING.values())
# HA's JSON light sends flash as flash_time_short/long (integer seconds), so
# discovery sets those to 1/2 and they are mapped back to the blink periods
# HA's UPB light used for short/long flash.
FLASH_TIME_SHORT, FLASH_TIME_LONG = 1, 2
FLASH_SECONDS = {FLASH_TIME_SHORT: 0.5, FLASH_TIME_LONG: 1.5, "short": 0.5, "long": 1.5}


def slugify(text: str) -> str:
    """Approximation of homeassistant.util.slugify, for default entity ids."""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_") or "unknown"


def brightness_pct(payload: dict[str, Any]) -> float:
    """Level from a command: brightness (0-255) or brightness_pct, as HA's services."""
    if (brightness := payload.get("brightness")) is not None:
        return round(float(brightness) / 2.55)
    return float(payload.get("brightness_pct", 100))


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

    def keypad_topic(self, keypad: Keypad, leaf: str) -> str:
        return f"{self.prefix}/keypad/{self.pim.network_id}_{keypad.upb_id}/{leaf}"

    # ---- setup -----------------------------------------------------------

    async def load(self) -> None:
        await self.pim.load_upstart_file()
        if not self.pim.config_ok or self.pim.network_id is None:
            raise SystemExit(f"Cannot load UPStart export file {self.opts.upe_file}")
        # Keypads are controllers: their presses arrive as link events, and they
        # have no load to show as a light. Dropping them also keeps upb-lib's
        # sync from querying them.
        devices = self.pim.devices.elements
        keypads = [index for index, device in devices.items() if device.isKeypad()]
        keypad_ids = {devices[index].addr.upb_id for index in keypads}
        for index in keypads:
            del devices[index]
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
        if self.opts.keypad_sync and keypad_ids:
            self._load_keypads(keypad_ids)

        for device in self.pim.devices.elements.values():
            device.add_callback(self._device_changed)
            self._publish(
                self.light_topic(device, "attributes"),
                self._device_attributes(device),
            )
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
        self.discovery = self._build_discovery()

    def _load_keypads(self, keypad_ids: set[int]) -> None:
        self.keypads = parse_keypads(self.opts.upe_file, keypad_ids)
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

    @staticmethod
    def _device_attributes(device: UpbDevice) -> dict[str, Any]:
        return {
            "address": device.index,
            "upb_id": device.addr.upb_id,
            "channel": device.addr.channel,
            "kind": device.kind,
            "manufacturer": device.manufacturer,
            "product": device.product,
            "version": device.version,
            "dimmable": device.dimmable,
        }

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
            LOG.info("Keypad %s: %s LED %s", keypad.name, keypad.label(bit), "on" if desired else "off")
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

        if parts == ["cmd"]:
            if payload.get("command") == "sync":
                LOG.info("Refreshing all device states")
                self._sync_all()
            return
        if len(parts) != 3:
            return
        kind, index, leaf = parts
        if kind == "light" and (device := self.pim.devices.elements.get(index)):
            if leaf == "set":
                self._light_set(device, payload)
            elif leaf == "cmd":
                self._light_cmd(device, payload)
        elif kind == "link" and (link := self.pim.links.elements.get(index)):
            if leaf == "set" and text.upper() in ("ON", "ACTIVATE"):
                link.activate()
            elif leaf == "cmd":
                self._link_cmd(link, payload)
        else:
            LOG.warning("Unknown element in topic %s", topic)

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
    except asyncio.CancelledError:
        LOG.info("Shutting down")
        bridge.pim.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
