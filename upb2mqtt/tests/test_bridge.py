"""End-to-end test: fake PIM <-> bridge (subprocess) <-> in-process MQTT broker.

Checks the exact PIM messages produced for every MQTT command, the state and
event topics that result, keypad LED sync, and discovery republish on HA birth.

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
from upb_lib.message import MessageEncode, decode
from upb_lib.util import seconds_to_rate

HERE = Path(__file__).parent
UPE = HERE / "test_network.upe"
PIM_PORT, MQTT_PORT = 12101, 11884
NET = 1
KEYPAD = 13
ENC = MessageEncode(2)  # test_network.upe sets tx_count=2


def dev(upb_id: int) -> UpbAddr:
    return UpbAddr(NET, upb_id, 0)


def link(link_id: int) -> LinkAddr:
    return LinkAddr(NET, link_id)


def indicate(channel: int, on: bool) -> str:
    """Indicate (0x26) to the keypad: set LED bit channel-1."""
    # pylint: disable=protected-access
    return ENC._encode_message(-1, dev(KEYPAD), 0xFF, 0x26, bytearray([0x64 if on else 0, 0, channel]))


class FakePim:
    """Answers like the real PIM: PA, then a device state report if asked."""

    def __init__(self) -> None:
        self.levels = {13: 0, 14: 40, 15: 0, 51: 100}
        self.leds = 0  # keypad LED bitmask
        self.received: list[str] = []
        self.indicates: list[str] = []  # keypad LED traffic, kept apart from received
        self.writer: asyncio.StreamWriter | None = None

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
            self.send("PR00FF")
            return
        if lead == "\x17":
            self.send("PA")
            return
        _, msg = decode(body)
        self.send("PA")
        if msg.msg_id == 0x26 and msg.dest_id == KEYPAD:
            self.indicates.append(body)
            bit = 1 << (msg.data[2] - 1)
            self.leds = self.leds | bit if msg.data[0] else self.leds & ~bit
            return
        self.received.append(body)
        if msg.msg_id == 0x22 and not msg.link:
            self.levels[msg.dest_id] = msg.data[0]
        if msg.msg_id == 0x30 and msg.dest_id in self.levels:
            await asyncio.sleep(0.05)
            level = self.levels[msg.dest_id]
            data = [self.leds] if msg.dest_id == KEYPAD else [level]  # keypads: LED bitmask
            self.send("PU" + self.report(msg.dest_id, data))

    @staticmethod
    def report(src: int, data: list[int]) -> str:
        length = 7 + len(data)
        raw = bytearray([length, 0x05, NET, 0xFF, src, 0x86, *data, 0])
        raw[-1] = (256 - sum(raw)) % 256
        return raw.hex().upper()

    def inject(self, hex_msg: str) -> None:
        """A message heard on the powerline (e.g. a keypad press)."""
        self.send("PU" + hex_msg)


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
        "--log-level", "warning", "--keypad-delay", "0.2",
    )
    try:
        async with aiomqtt.Client("127.0.0.1", MQTT_PORT) as client:
            await client.subscribe("upb/#")
            await client.subscribe("homeassistant/#")
            h = Harness(client, pim)
            collector = asyncio.create_task(h.collect())
            await asyncio.sleep(2.5)

            # Startup: upb-lib syncs every device; states published retained
            h.check("bridge online", h.topics.get("upb/status") == "online")
            h.check("pim online", h.topics.get("upb/pim/status") == "online")
            h.expect_state("1_14_0", {"state": "ON", "brightness": 40})
            h.expect_state("1_15_0", {"state": "OFF", "brightness": 0})
            h.expect_state("1_51_0", {"state": "ON"})  # relay: no brightness
            h.check("keypad not a light", not any("light/1_13" in t or "upb_1_13" in t for t in h.topics),
                    [t for t in h.topics if "1_13" in t])
            # Keypad read at connect; B1 follows Vanity (51), which is on
            h.check("keypad LEDs set at startup", pim.indicates == [indicate(1, True)], pim.indicates)
            leds = json.loads(h.topics.get("upb/keypad/1_13/leds", "{}"))
            h.check("keypad LEDs published", leds.get("synced") == ["B1", "B5", "B2"]
                    and leds.get("leds", {}).get("B1") is True, leds)
            h.check("discovery published", "homeassistant/device/upb_1_14/config" in h.topics)

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

            # Resync button (also re-reads the keypad)
            await h.command("upb/cmd", {"command": "sync"},
                            [ENC.report_state(dev(i)) for i in (14, 15, 51, KEYPAD)])

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
