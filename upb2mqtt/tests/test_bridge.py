"""End-to-end test: fake PIM <-> bridge (subprocess) <-> in-process MQTT broker.

Checks the exact PIM messages produced for every MQTT command, the state and
event topics that result, light groups sent as scenes, keypad LED sync,
learned per-device transmit counts, per-device stats and their table page,
and discovery republish on HA birth.

    pip install -r requirements.txt amqtt
    python tests/test_bridge.py
"""

import asyncio
import json
import sys
from pathlib import Path

import aiomqtt
from amqtt.broker import Broker
from upb_lib.devices import UpbAddr
from upb_lib.links import LinkAddr
from upb_lib.message import decode
from upb_lib.util import seconds_to_rate

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent))
from keypads import parse_keypads  # noqa: E402
from txcount import TxEncoder  # noqa: E402

UPE = HERE / "test_network.upe"
PIM_PORT, MQTT_PORT, WEB_PORT = 12101, 11884, 18099
NET = 1
KEYPAD = 13
# Transmit counts once learned: 15 doesn't hear single transmissions (the
# tx_count option's default, 2). A link goes out as often as the neediest
# device in it or keypad showing it: 17, 20 and 50 hold 15; the rest hold
# only 51 and/or the keypad.
DEAF_TO_SINGLE = {15}
DEV_TX = {13: 1, 14: 1, 15: 2, 51: 1}
LINK_TX = {1: 1, 3: 1, 4: 1, 17: 2, 20: 2, 50: 2, 52: 1, 99: 1}
ENC = TxEncoder(lambda addr: (LINK_TX if addr.is_link else DEV_TX)[addr.upb_id])


def dev(upb_id: int) -> UpbAddr:
    return UpbAddr(NET, upb_id, 0)


def link(link_id: int) -> LinkAddr:
    return LinkAddr(NET, link_id)


def indicate(channel: int, on: bool) -> str:
    """Indicate (0x26) to the keypad: set LED bit channel-1."""
    # pylint: disable=protected-access
    return ENC._encode_message(-1, dev(KEYPAD), 0xFF, 0x26, bytearray([0x64 if on else 0, 0, channel]))


def query(upb_id: int, tx: int) -> str:
    """Report state sent tx times."""
    return ENC.report_state(dev(upb_id), ctl=(tx - 1) << 2)


class FakePim:
    """Answers like the real PIM: PA, then a device state report if asked."""

    def __init__(self) -> None:
        self.levels = {13: 0, 14: 40, 15: 0, 51: 100}
        self.leds = 0  # keypad LED bitmask
        self.keypad = parse_keypads(str(UPE), {KEYPAD})[KEYPAD]  # its indicator table
        self.received: list[str] = []
        self.indicates: list[str] = []  # keypad LED traffic, kept apart from received
        self.writer: asyncio.StreamWriter | None = None
        self.signal = "5301000200FF00"  # registers F9-FF: signal, noise floor, counts
        self.register_reads: list[str] = []
        self.deaf = set(DEAF_TO_SINGLE)  # devices that miss a message sent once
        self.rejects: set[int] = set()  # devices whose single transmissions get PE

    async def handle(self, reader, writer) -> None:
        self.writer = writer
        buf = b""
        while data := await reader.read(500):
            buf += data
            while b"\r" in buf:
                line, buf = buf.split(b"\r", 1)
                await self.on_line(line.decode())

    def send(self, line: str) -> None:
        self.writer.write(f"{line}\r".encode())

    async def on_line(self, line: str) -> None:
        lead, body = line[0], line[1:]
        if lead == "\x12":
            self.register_reads.append(body)
            self.send("PRF9" + self.signal if body.startswith("F9") else "PR00FF")
            return
        if lead == "\x17":
            self.send("PA")
            return
        _, msg = decode(body)
        if msg.tx_count == 0 and msg.dest_id in self.rejects and not msg.link:
            self.received.append(body)
            self.send("PE")  # no reply follows, and upb-lib sees no timeout
            return
        self.send("PA")
        if msg.tx_count == 0 and msg.dest_id in self.deaf and not msg.link:
            self.received.append(body)
            return  # never heard
        if msg.msg_id == 0x26 and msg.dest_id == KEYPAD:
            self.indicates.append(body)
            bit = 1 << (msg.data[2] - 1)
            self.leds = self.leds | bit if msg.data[0] else self.leds & ~bit
            return
        self.received.append(body)
        self.keypad_hears(msg)
        if msg.msg_id == 0x22 and not msg.link:
            self.levels[msg.dest_id] = msg.data[0]
        if msg.msg_id == 0x30 and msg.dest_id in self.levels:
            await asyncio.sleep(0.05)
            level = self.levels[msg.dest_id]
            data = [self.leds] if msg.dest_id == KEYPAD else [level]  # keypads: LED bitmask
            self.send("PU" + self.report(msg.dest_id, data, msg.tx_count + 1))

    def keypad_hears(self, msg) -> None:
        """The keypad sets its LEDs from link traffic in its indicator table."""
        commands = {0x20: "activated", 0x21: "deactivated", 0x22: "goto"}
        if msg.link and msg.msg_id in commands:
            self.keypad.set_report(self.leds)
            self.keypad.apply_link(msg.dest_id, commands[msg.msg_id], msg.data[0] if msg.data else -1)
            self.leds = sum(1 << bit for bit, on in self.keypad.leds.items() if on)

    @staticmethod
    def report(src: int, data: list[int], tx: int = 2) -> str:
        """Device state report, sent as many times as the query was (last copy)."""
        length = 7 + len(data)
        raw = bytearray([length, (tx - 1) << 2 | (tx - 1), NET, 0xFF, src, 0x86, *data, 0])
        raw[-1] = (256 - sum(raw)) % 256
        return raw.hex().upper()

    def inject(self, hex_msg: str) -> None:
        """A message heard on the powerline (e.g. a keypad press)."""
        self.keypad_hears(decode(hex_msg)[1])
        self.send("PU" + hex_msg)


async def http_get(path: str, method: str = "GET") -> tuple[int, dict[str, str], bytes]:
    """A request to the bridge's device table server: status, headers, body."""
    reader, writer = await asyncio.open_connection("127.0.0.1", WEB_PORT)
    writer.write(f"{method} {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    raw = await reader.read()
    writer.close()
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.decode().split("\r\n")
    headers = dict(line.split(": ", 1) for line in lines[1:])
    return int(lines[0].split()[1]), headers, body


class Harness:
    def __init__(self, client: aiomqtt.Client, pim: FakePim) -> None:
        self.client = client
        self.pim = pim
        self.topics: dict[str, str] = {}
        self.events: list[tuple[str, dict]] = []
        self.failures = 0

    async def collect(self) -> None:
        async for m in self.client.messages:
            payload = m.payload.decode()
            if m.topic.value.endswith("/event"):
                self.events.append((m.topic.value, json.loads(payload)))
            else:
                self.topics[m.topic.value] = payload

    def check(self, name: str, ok: bool, detail="") -> None:
        print(f"{'PASS' if ok else 'FAIL'} {name}{'' if ok else f'  -> {detail}'}")
        self.failures += not ok

    async def command(self, topic: str, payload, expect_pim: list[str]) -> None:
        self.pim.received.clear()
        body = payload if isinstance(payload, str) else json.dumps(payload)
        await self.client.publish(topic, body)
        await asyncio.sleep(0.6)
        self.check(
            f"{topic} {body}",
            self.pim.received == expect_pim,
            f"PIM got {self.pim.received}, expected {expect_pim}",
        )

    async def group(self, name: str, commands: dict[int, dict], expect_pim: list[str]) -> None:
        """Commands to several lights at once, as HA sends for a light group."""
        self.pim.received.clear()
        for unit, payload in commands.items():
            await self.client.publish(f"upb/light/1_{unit}_0/set", json.dumps(payload))
        await asyncio.sleep(0.6)
        self.check(name, self.pim.received == expect_pim,
                   f"PIM got {self.pim.received}, expected {expect_pim}")

    def attr(self, index: str, key: str):
        return json.loads(self.topics.get(f"upb/light/{index}/attributes", "{}")).get(key)

    async def wait_for(self, name: str, test, seconds: float) -> None:
        for _ in range(int(seconds * 10)):
            if test():
                break
            await asyncio.sleep(0.1)
        self.check(name, test())

    def stats(self, unit: int) -> dict:
        return json.loads(self.topics.get(f"upb/stats/1_{unit}", "{}"))

    def state(self, index: str) -> dict | None:
        raw = self.topics.get(f"upb/light/{index}/state")
        return json.loads(raw) if raw else None

    def expect_state(self, index: str, expected: dict) -> None:
        self.check(f"state {index} == {expected}", self.state(index) == expected, self.state(index))


async def main() -> int:
    broker = Broker(
        {
            "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{MQTT_PORT}"}},
            "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}},
        }
    )
    await broker.start()
    pim = FakePim()
    pim_server = await asyncio.start_server(pim.handle, "127.0.0.1", PIM_PORT)

    bridge = await asyncio.create_subprocess_exec(
        sys.executable, str(HERE.parent / "upb2mqtt.py"),
        "--pim-url", f"socket://127.0.0.1:{PIM_PORT}",
        "--upe-file", str(UPE),
        "--mqtt-host", "127.0.0.1", "--mqtt-port", str(MQTT_PORT),
        "--log-level", "warning", "--keypad-delay", "0.2", "--stats-interval", "1",
        "--web-port", str(WEB_PORT),
    )
    try:
        async with aiomqtt.Client("127.0.0.1", MQTT_PORT) as client:
            await client.subscribe("upb/#")
            await client.subscribe("homeassistant/#")
            h = Harness(client, pim)
            collector = asyncio.create_task(h.collect())
            await asyncio.sleep(1)

            # The table shows the startup read's progress while it runs
            progress = []
            for _ in range(40):
                try:
                    status = json.loads((await http_get("/stats.json"))[2])
                except OSError:
                    status = {}
                if status.get("reading"):
                    progress.append(status["reading"])
                elif progress:
                    break
                await asyncio.sleep(0.1)
            h.check("table: read progress shown", bool(progress) and progress[0]["total"] == 4
                    and any(p["unit"] == 15 for p in progress) and progress[-1]["done"] >= 3,
                    progress[:1] + progress[-1:])
            h.check("table: progress gone once read", status.get("reading") is None, status.get("reading"))

            # Startup: every device read with a single transmission first; 15
            # misses it (a 1.5 s timeout) and is read again with two
            await h.wait_for("transmit counts learned",
                             lambda: h.attr("1_15_0", "tx_count") == 2 and h.attr("1_14_0", "tx_count") == 1, 8)
            h.check("vanity takes 1", h.attr("1_51_0", "tx_count") == 1, h.attr("1_51_0", "tx_count"))
            keypad_reads = [m for m in pim.received if m in (query(KEYPAD, 1), query(KEYPAD, 2))]
            h.check("keypad read only by its probe", keypad_reads == [query(KEYPAD, 1)], keypad_reads)
            await asyncio.sleep(0.5)

            # States published retained
            h.check("bridge online", h.topics.get("upb/status") == "online")
            h.check("pim online", h.topics.get("upb/pim/status") == "online")
            h.expect_state("1_14_0", {"state": "ON", "brightness": 40})
            h.expect_state("1_15_0", {"state": "OFF", "brightness": 0})
            h.expect_state("1_51_0", {"state": "ON"})  # relay: no brightness
            h.check("keypad not a light", not any("light/1_13" in t or "upb_1_13" in t for t in h.topics),
                    [t for t in h.topics if "1_13" in t])
            # Keypad LEDs read from its probe reply; B1 follows Vanity (51), which is on
            h.check("keypad LEDs set at startup", pim.indicates == [indicate(1, True)], pim.indicates)
            leds = json.loads(h.topics.get("upb/keypad/1_13/leds", "{}"))
            h.check("keypad LEDs published", leds.get("synced") == ["B1", "B5", "B2"]
                    and leds.get("leds", {}).get("B1") is True, leds)
            h.check("discovery published", "homeassistant/device/upb_1_14/config" in h.topics)

            # PIM signal/noise: read on connect, published for two diagnostic sensors
            h.check("signal registers read on connect", "F90700" in pim.register_reads, pim.register_reads)
            signal = json.loads(h.topics.get("upb/pim/signal", "{}"))
            h.check("signal published", signal == {"signal_strength": 0x53, "noise_floor": 1,
                                                   "noise_counts": [0, 2, 0, 255, 0]}, signal)
            pim_components = json.loads(h.topics["homeassistant/device/upb_1_pim/config"])["components"]
            sensors = {k: v for k, v in pim_components.items() if v["platform"] == "sensor"}
            h.check("signal sensors discovered", sorted(sensors) == ["noise_floor", "signal_strength"]
                    and all(v["entity_category"] == "diagnostic" and v["state_topic"] == "upb/pim/signal"
                            for v in sensors.values()), sensors)
            noise = pim_components.get("noise_problem", {})
            h.check("high-noise problem sensor discovered", noise.get("platform") == "binary_sensor"
                    and noise.get("device_class") == "problem" and "state_class" not in noise
                    and noise.get("state_topic") == "upb/pim/signal"
                    and "value_json.noise_floor > 100" in noise.get("value_template", ""), noise)

            # Per-device stats sensor: startup reads counted, probe misses aren't timeouts
            stats14 = json.loads(h.topics["homeassistant/device/upb_1_14/config"])["components"]["stats"]
            h.check("stats sensor discovered", stats14["state_topic"] == "upb/stats/1_14"
                    and stats14["state_class"] == "total_increasing"
                    and stats14["default_entity_id"] == "sensor.first_floor_office_main_state_changes",
                    stats14)
            await h.wait_for("stats: startup queries answered with a latency",
                             lambda: h.stats(14).get("queries") == 1 and h.stats(14).get("latency_ms"), 3)
            h.check("stats: probe miss neither a query nor a timeout", h.stats(15).get("queries") == 1
                    and h.stats(15).get("timeouts") == 0, h.stats(15))
            h.check("stats: tx_count", (h.stats(14).get("tx_count"), h.stats(15).get("tx_count")) == (1, 2),
                    (h.stats(14), h.stats(15)))
            h.check("stats: first reads aren't changes", h.stats(14).get("state_changes") == 0, h.stats(14))
            h.check("stats: keypads have none", "upb/stats/1_13" not in h.topics)

            # HA JSON light commands (upb-lib default for turn_on is 100%)
            await h.command("upb/light/1_15_0/set", {"state": "ON", "brightness": 50, "transition": 5},
                            [ENC.goto(dev(15), 50, seconds_to_rate(5))])
            h.expect_state("1_15_0", {"state": "ON", "brightness": 50})
            await h.command("upb/light/1_15_0/set", {"state": "ON"}, [ENC.goto(dev(15), 100, -1)])
            await h.command("upb/light/1_14_0/set", {"state": "OFF"}, [ENC.goto(dev(14), 0, -1)])
            h.expect_state("1_14_0", {"state": "OFF", "brightness": 0})
            await h.command("upb/light/1_51_0/set", {"state": "ON", "brightness": 30},
                            [ENC.goto(dev(51), 100, -1)])  # non-dimmable -> 100
            await h.command("upb/light/1_51_0/set", {"state": "ON", "flash": 2}, [ENC.blink(dev(51), 90)])
            await h.command("upb/light/1_51_0/set", {"state": "ON", "flash": 1}, [ENC.blink(dev(51), 30)])

            # Light extras (HA's light_fade_start / fade_stop / blink / update)
            await h.command("upb/light/1_14_0/cmd", {"command": "fade_start", "brightness_pct": 30, "rate": 10},
                            [ENC.fade_start(dev(14), 30, seconds_to_rate(10))])
            await h.command("upb/light/1_14_0/cmd", {"command": "fade_start", "brightness": 255},
                            [ENC.fade_start(dev(14), 100, -1)])
            await h.command("upb/light/1_14_0/cmd", {"command": "fade_stop"},
                            [ENC.fade_stop(dev(14)), ENC.report_state(dev(14))])
            await h.command("upb/light/1_14_0/cmd", {"command": "blink", "blink_rate": 0.1},
                            [ENC.blink(dev(14), 20)])  # upb-lib minimum
            await h.command("upb/light/1_14_0/cmd", "update_status", [ENC.report_state(dev(14))])

            # Discovery details HA needs at runtime, and the refresh button's press
            dev14 = json.loads(h.topics["homeassistant/device/upb_1_14/config"])
            button = dev14["components"]["refresh"]
            h.check("refresh button discovered", button["platform"] == "button"
                    and button["default_entity_id"] == "button.first_floor_office_main_refresh_state", button)
            await h.command(button["command_topic"], button["payload_press"], [ENC.report_state(dev(14))])
            scene = json.loads(h.topics["homeassistant/scene/upb_1_50/config"])
            h.check("scene has payload_on", scene.get("payload_on") == "ON", scene)
            await h.command(scene["command_topic"], scene["payload_on"], [ENC.activate_link(link(50))])

            # Links: scene activation and every link service
            h.events.clear()
            await h.command("upb/link/1_50/set", "ON", [ENC.activate_link(link(50))])
            h.expect_state("1_15_0", {"state": "OFF", "brightness": 0})
            h.check("activated event", ("upb/link/1_50/event", {"event_type": "activated", "address": "1_50",
                    "brightness_pct": -1, "rate": -1}) in h.events, h.events)
            await h.command("upb/link/1_1/cmd", {"command": "activate"}, [ENC.activate_link(link(1))])
            h.expect_state("1_51_0", {"state": "ON"})
            await h.command("upb/link/1_1/cmd", {"command": "deactivate"}, [ENC.deactivate_link(link(1))])
            h.expect_state("1_51_0", {"state": "OFF"})
            await h.command("upb/link/1_17/cmd", {"command": "goto", "brightness_pct": 60, "rate": 3.3},
                            [ENC.goto(link(17), 60, seconds_to_rate(3.3))])
            h.expect_state("1_14_0", {"state": "ON", "brightness": 60})
            await h.command("upb/link/1_17/cmd", {"command": "fade_start", "brightness": 51, "rate": 1},
                            [ENC.fade_start(link(17), 20, seconds_to_rate(1))])
            await h.command("upb/link/1_17/cmd", {"command": "fade_stop"},
                            [ENC.fade_stop(link(17)), ENC.report_state(dev(14)), ENC.report_state(dev(15))])
            await h.command("upb/link/1_17/cmd", {"command": "blink", "blink_rate": 1},
                            [ENC.blink(link(17), 60)])
            h.check("goto event", any(e["event_type"] == "goto" and e["brightness_pct"] == 60
                                      and e["rate"] == 3.3 for _, e in h.events), h.events)

            # Light groups: a scene replaces the commands where it matches exactly
            on, off = {"state": "ON"}, {"state": "OFF"}
            h.events.clear()
            await h.group("group on -> Office On", {14: on, 15: on}, [ENC.activate_link(link(20))])
            h.expect_state("1_14_0", {"state": "ON", "brightness": 100})
            h.expect_state("1_15_0", {"state": "ON", "brightness": 100})
            h.check("scene event fired", [t for t, _ in h.events] == ["upb/link/1_20/event"], h.events)
            await h.group("group off -> Common Off (lowest of two)", {14: off, 15: off},
                          [ENC.activate_link(link(17))])
            h.expect_state("1_15_0", {"state": "OFF", "brightness": 0})
            await h.group("scene plus a leftover light", {51: on, 14: on, 15: on},
                          [ENC.activate_link(link(20)), ENC.goto(dev(51), 100, -1)])
            await h.group("level not the preset -> direct", {14: {"state": "ON", "brightness": 50}, 15: on},
                          [ENC.goto(dev(14), 50, -1), ENC.goto(dev(15), 100, -1)])
            await h.group("transition -> not batched", {14: {"state": "OFF", "transition": 2}, 15: off},
                          [ENC.goto(dev(14), 0, seconds_to_rate(2)), ENC.goto(dev(15), 0, -1)])
            h.pim.received.clear()
            await client.publish("upb/light/1_14_0/set", json.dumps(on))
            await client.publish("upb/light/1_14_0/cmd", "update_status")
            await asyncio.sleep(0.6)
            h.check("batched command goes before a later one",
                    pim.received == [ENC.goto(dev(14), 100, -1), ENC.report_state(dev(14))], pim.received)

            # Powerline traffic we didn't send: keypad activates link 1
            h.events.clear()
            pim.inject(ENC.activate_link(link(1)))
            await asyncio.sleep(0.3)
            h.expect_state("1_51_0", {"state": "ON"})
            h.check("event from keypad", [t for t, _ in h.events] == ["upb/link/1_1/event"], h.events)
            # Someone else's direct device goto
            pim.inject(ENC.goto(dev(15), 75, -1))
            await asyncio.sleep(0.3)
            h.expect_state("1_15_0", {"state": "ON", "brightness": 75})
            await h.wait_for("stats: keypad press and someone else's goto observed",
                             lambda: h.stats(51).get("observed") == 1 and h.stats(15).get("observed") == 1, 2)
            s14 = h.stats(14)
            h.check("stats: our commands and state changes counted", s14.get("sent", 0) > 0
                    and s14.get("sent_scene", 0) > 0 and s14.get("state_changes", 0) > 0, s14)

            # Keypad LEDs. Entry KP: B1 <- Vanity (51); B5 <- Common Off (14 and 15
            # off); B2 <- Office On (14 and 15 on); B3 <- a reset scene, no lights.
            async def leds_after(name: str, action, expect: list[str]) -> None:
                pim.indicates.clear()
                await action()
                await asyncio.sleep(0.8)
                h.check(name, pim.indicates == expect, f"got {pim.indicates}, expected {expect}")

            async def lights(**levels: bool) -> None:
                for unit, on in levels.items():
                    await client.publish(f"upb/light/1_{unit[1:]}_0/set", json.dumps({"state": "ON" if on else "OFF"}))
                await asyncio.sleep(0.1)

            await lights(u51=True, u14=False, u15=False)
            await asyncio.sleep(0.8)
            h.check("LEDs follow lights", pim.leds == 0b011, f"{pim.leds:08b}")
            await leds_after("light off directly -> its LED off",
                             lambda: lights(u51=False), [indicate(1, False)])
            # The keypad lights B2 itself for the Office On scene; only B5 is stale
            await leds_after("scene activation -> only stale LEDs",
                             lambda: client.publish("upb/link/1_20/set", "ON"), [indicate(2, False)])
            await leds_after("scene_match all: one light off -> scene LED off",
                             lambda: lights(u15=False), [indicate(3, False)])
            await lights(u51=True)
            await asyncio.sleep(0.8)

            async def reset_heard() -> None:  # e.g. the old [kp] Cleanup automation
                pim.inject(ENC.activate_link(link(99)))

            await leds_after("reset scene heard -> managed LED restored", reset_heard, [indicate(1, True)])
            await leds_after("unchanged lights -> no traffic", lambda: lights(u51=True), [])

            # Last light of a keypad scene: 15 is off, so turning 14 off alone
            # completes Common Off; the keypad sets B5 itself from the link
            await lights(u14=True, u15=False)
            await asyncio.sleep(0.8)
            pim.received.clear()
            pim.indicates.clear()
            await client.publish("upb/light/1_14_0/set", json.dumps({"state": "OFF"}))
            await asyncio.sleep(0.8)
            h.check("last light off -> keypad's off scene", pim.received == [ENC.activate_link(link(17))],
                    pim.received)
            h.check("... and no LED fix-ups", pim.indicates == [], pim.indicates)
            h.expect_state("1_14_0", {"state": "OFF", "brightness": 0})

            # A single-transmission device that stops answering goes back to 2,
            # starting with upb-lib's retry of the query it missed
            pim.deaf.add(51)
            pim.received.clear()
            await client.publish("upb/light/1_51_0/cmd", "update_status")
            await h.wait_for("timed-out device raised to 2", lambda: h.attr("1_51_0", "tx_count") == 2, 12)
            await h.wait_for("... and its query retried with two",
                             lambda: pim.received == [query(51, 1), query(51, 2)], 2)
            await h.wait_for("stats: query answered on retry isn't a timeout",
                             lambda: h.stats(51).get("queries") == 2 and h.stats(51).get("timeouts") == 0
                             and h.stats(51).get("tx_count") == 2, 2)

            # Device table page (HA ingress) and the JSON it reads
            code, headers, body = await http_get("/")
            h.check("table page served", code == 200 and headers["Content-Type"].startswith("text/html")
                    and b"stats.json" in body, (code, headers))
            code, headers, body = await http_get("/stats.json?t=1")
            table = json.loads(body) if code == 200 else {}
            rows = {r["unit"]: r for r in table.get("devices", [])}
            h.check("table: every light unit, no keypads", sorted(rows) == [14, 15, 51], sorted(rows))
            row = rows.get(51, {})
            h.check("table: stats, tx_count, level", table.get("pim_connected") is True
                    and row.get("name") == "Master Bathroom Vanity" and row.get("timeouts") == 0
                    and row.get("tx_count") == 2 and row.get("level") == [100]
                    and row.get("dimmable") == [False], row)
            h.check("table: dimmers marked", rows.get(14, {}).get("dimmable") == [True], rows.get(14))
            pim.received.clear()
            code = (await http_get("/refresh/14", "POST"))[0]
            await asyncio.sleep(0.3)
            h.check("table: refresh icon reads the device", code == 202
                    and pim.received == [ENC.report_state(dev(14))], (code, pim.received))
            h.check("table: refresh unknown device", (await http_get("/refresh/13", "POST"))[0] == 404)
            h.check("table: refresh needs POST", (await http_get("/refresh/14"))[0] == 405)
            h.check("table: 404", (await http_get("/nope"))[0] == 404)
            h.check("table: HEAD has no body", (await http_get("/", "HEAD"))[2] == b"")
            h.check("table: GET only", (await http_get("/", "POST"))[0] == 405)
            h.check("no panel link outside HA", "configuration_url" not in json.loads(
                h.topics["homeassistant/device/upb_1_14/config"])["device"])
            DEV_TX[51] = 2
            LINK_TX.update({1: 2, 3: 2, 4: 2, 52: 2})
            await h.command("upb/light/1_51_0/set", {"state": "OFF"}, [ENC.goto(dev(51), 0, -1)])
            await h.command("upb/link/1_1/cmd", {"command": "activate"}, [ENC.activate_link(link(1))])

            # Resync button: every device once more with a single transmission
            # first, lights before keypads; the keypad and 51 now miss those, so
            # they're read with two. The PIM rejects 14's (PE): no reply and no
            # timeout, so the bridge gives up on it by itself
            pim.deaf.add(KEYPAD)
            pim.rejects.add(14)
            pim.received.clear()
            await client.publish("upb/cmd", json.dumps({"command": "sync"}))
            expect = [query(14, 1), query(14, 2), query(15, 1), query(15, 2), query(51, 1), query(51, 2),
                      query(KEYPAD, 1), query(KEYPAD, 2)]  # keypads last
            await h.wait_for("resync relearns", lambda: len(pim.received) >= len(expect), 15)
            h.check("resync messages", pim.received == expect, pim.received)
            await h.wait_for("rejected probe -> 2", lambda: h.attr("1_14_0", "tx_count") == 2, 3)

            # HA birth -> discovery republished
            del h.topics["homeassistant/device/upb_1_14/config"]
            await client.publish("homeassistant/status", "online")
            await asyncio.sleep(0.5)
            h.check("discovery republished on HA birth", "homeassistant/device/upb_1_14/config" in h.topics)

            # Clean shutdown -> offline
            bridge.terminate()
            await bridge.wait()
            await asyncio.sleep(0.3)
            h.check("offline on shutdown", h.topics.get("upb/status") == "offline", h.topics.get("upb/status"))
            collector.cancel()
    finally:
        if bridge.returncode is None:
            bridge.kill()
        pim_server.close()
        await broker.shutdown()

    print(f"\n{h.failures} failure(s)")
    return 1 if h.failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
