"""End-to-end test with two PIMs: two fake PIMs on one fake powerline <->
bridge (subprocess) <-> in-process MQTT broker.

The PIMs are on opposite phases. A single transmission from a PIM reaches only
the devices on its phase (A: 51 and keypad 13; B: 14; 15 neither); a repeated
one reaches every device, and the other PIM. Each copy takes COPY seconds on
the line, and the line records every use, so overlapping transmissions (two
PIMs at once, or a PIM over a device's reply) show up as collisions.

Checks the probe order (A once, else B once, else A repeated) with B
connecting late, that nothing goes out repeated where a PIM reaches with one,
that each message goes out on the PIM needing the fewest transmissions, that
only one transmits at a time, that traffic both PIMs hear is handled once,
and failover when PIM B goes away and comes back.

    pip install -r requirements.txt amqtt
    python tests/test_dual_pim.py
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

HERE = Path(__file__).parent
UPE = HERE / "test_network.upe"
PIM_A_PORT, PIM_B_PORT, MQTT_PORT = 12111, 12112, 11885
NET = 1
KEYPAD = 13
ENC = {tx: MessageEncode(tx) for tx in (1, 2)}
INDICATE = 0x26
COPY = 0.1  # seconds per copy of a message on the line


def dev(upb_id: int) -> UpbAddr:
    return UpbAddr(NET, upb_id, 0)


def query(upb_id: int, tx: int) -> str:
    return ENC[tx].report_state(dev(upb_id))


def report(src: int, data: list[int], tx: int) -> str:
    """Device state report sent tx times: its first copy."""
    raw = bytearray([7 + len(data), (tx - 1) << 2, NET, 0xFF, src, 0x86, *data, 0])
    raw[-1] = (256 - sum(raw)) % 256
    return raw.hex().upper()


class Line:
    """The powerline both PIMs are on."""

    def __init__(self) -> None:
        self.levels = {KEYPAD: 0, 14: 40, 15: 0, 51: 100}  # keypad: LED bitmask
        self.pims: list["LegPim"] = []
        self.uses: list[tuple[float, float, str]] = []  # start, end, who

    def use(self, who: str, start: float) -> None:
        self.uses.append((start, asyncio.get_running_loop().time(), who))

    def collisions(self) -> list:
        uses = sorted(self.uses)
        return [(a, b) for a, b in zip(uses, uses[1:]) if b[0] < a[1] - 0.01]


class LegPim:
    """A PIM on one phase; it answers like the real PIM (PA, then any reply)."""

    def __init__(self, name: str, line: Line, near: set[int]) -> None:
        self.name = name
        self.line = line
        self.near = near  # devices that hear it send once
        self.received: list[str] = []  # UPB messages it was given to send, not Indicates
        self.writer: asyncio.StreamWriter | None = None
        self.server: asyncio.Server | None = None
        self.tasks: set[asyncio.Task] = set()
        line.pims.append(self)

    async def start(self, port: int) -> None:
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", port)

    async def stop(self) -> None:
        """Unplug it: drop the connection and refuse new ones."""
        self.server.close()
        if self.writer:
            self.writer.close()
            self.writer = None
        await self.server.wait_closed()

    async def handle(self, reader, writer) -> None:
        self.writer = writer
        buf = b""
        while data := await reader.read(500):
            buf += data
            while b"\r" in buf:
                line, buf = buf.split(b"\r", 1)
                # Concurrently, so that a message sent while one is still on
                # the line overlaps it, as it would on the powerline
                task = asyncio.create_task(self.on_line(line.decode()))
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)

    def send(self, line: str) -> None:
        if self.writer:
            self.writer.write(f"{line}\r".encode())

    def hear(self, hex_msg: str) -> None:
        self.send("PU" + hex_msg)

    async def on_line(self, line: str) -> None:
        lead, body = line[0], line[1:]
        if lead == "\x12":
            self.send("PRF95301000200FF00" if body.startswith("F9") else "PR00FF")
            return
        if lead == "\x17":
            self.send("PA")
            return
        _, msg = decode(body)
        if msg.msg_id != INDICATE:
            self.received.append(body)
        loop = asyncio.get_running_loop()
        tx = msg.tx_count + 1
        self.send("PA")
        start = loop.time()
        await asyncio.sleep(tx * COPY)
        self.line.use(self.name, start)
        if tx > 1:
            for other in self.line.pims:
                if other is not self:
                    other.hear(body)  # a repeated message reaches the other phase
        self.send("PN")  # done transmitting (no ack requested)
        if msg.link or (tx == 1 and msg.dest_id not in self.near):
            return
        if msg.msg_id == 0x22:
            self.line.levels[msg.dest_id] = msg.data[0]
        if msg.msg_id == 0x30 and msg.dest_id in self.line.levels:
            await asyncio.sleep(0.05)
            start = loop.time()
            await asyncio.sleep(COPY)
            reply = report(msg.dest_id, [self.line.levels[msg.dest_id]], tx)
            for pim in self.line.pims:
                if tx > 1 or msg.dest_id in pim.near:
                    pim.hear(reply)  # the first copy; any others follow
            await asyncio.sleep((tx - 1) * COPY)
            self.line.use(f"unit {msg.dest_id}", start)


class Harness:
    def __init__(self, client: aiomqtt.Client, pims: list[LegPim]) -> None:
        self.client = client
        self.pims = pims
        self.topics: dict[str, str] = {}
        self.events: list[str] = []
        self.failures = 0

    async def collect(self) -> None:
        async for m in self.client.messages:
            if m.topic.value.endswith("/event"):
                self.events.append(m.topic.value)
            else:
                self.topics[m.topic.value] = m.payload.decode()

    def check(self, name: str, ok: bool, detail="") -> None:
        print(f"{'PASS' if ok else 'FAIL'} {name}{'' if ok else f'  -> {detail}'}")
        self.failures += not ok

    async def wait_for(self, name: str, test, seconds: float, detail=lambda: "") -> None:
        for _ in range(int(seconds * 10)):
            if test():
                break
            await asyncio.sleep(0.1)
        self.check(name, test(), detail())

    def route(self, unit: int) -> tuple:
        attrs = json.loads(self.topics.get(f"upb/light/1_{unit}_0/attributes", "{}"))
        return attrs.get("pim"), attrs.get("tx_count")

    def stats(self, unit: int) -> dict:
        return json.loads(self.topics.get(f"upb/stats/1_{unit}", "{}"))

    async def command(self, topic: str, payload, expect: tuple[list[str], list[str]]) -> None:
        """A command, and what each PIM is given to send for it."""
        for pim in self.pims:
            pim.received.clear()
        body = payload if isinstance(payload, str) else json.dumps(payload)
        await self.client.publish(topic, body)
        await asyncio.sleep(0.6)
        got = tuple(pim.received for pim in self.pims)
        self.check(f"{topic} {body}", got == expect, f"PIMs got {got}, expected {expect}")


async def main() -> int:
    broker = Broker(
        {
            "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{MQTT_PORT}"}},
            "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}},
        }
    )
    await broker.start()
    line = Line()
    pim_a = LegPim("A", line, {51, KEYPAD})
    pim_b = LegPim("B", line, {14})
    await pim_a.start(PIM_A_PORT)  # B is plugged in after the bridge starts

    bridge = await asyncio.create_subprocess_exec(
        sys.executable, str(HERE.parent / "upb2mqtt.py"),
        "--pim-url", f"socket://127.0.0.1:{PIM_A_PORT}",
        "--pim-b-url", f"socket://127.0.0.1:{PIM_B_PORT}",
        "--upe-file", str(UPE),
        "--mqtt-host", "127.0.0.1", "--mqtt-port", str(MQTT_PORT),
        "--log-level", "warning", "--keypad-delay", "0.2", "--stats-interval", "1",
        "--web-port", "0", "--batch-window", "0",
    )
    try:
        async with aiomqtt.Client("127.0.0.1", MQTT_PORT) as client:
            await client.subscribe("upb/#")
            await client.subscribe("homeassistant/#")
            h = Harness(client, [pim_a, pim_b])
            collector = asyncio.create_task(h.collect())

            await asyncio.sleep(2)
            await pim_b.start(PIM_B_PORT)

            # Startup waits for B, then: A once, else B once, else A repeated
            await h.wait_for("routes learned",
                             lambda: (h.route(14), h.route(15), h.route(51)) == (("B", 1), ("A", 2), ("A", 1)),
                             20, lambda: (h.route(14), h.route(15), h.route(51)))
            await asyncio.sleep(1)
            h.check("A: each device once, 15 repeated after both missed it", pim_a.received
                    == [query(14, 1), query(15, 1), query(15, 2), query(51, 1), query(KEYPAD, 1)],
                    pim_a.received)
            h.check("B: only devices A doesn't reach once, once each",
                    pim_b.received == [query(14, 1), query(15, 1)], pim_b.received)
            h.check("discovery: one transmitter at a time", line.collisions() == [], line.collisions())
            h.check("states read", (json.loads(h.topics.get("upb/light/1_14_0/state", "{}")).get("brightness"),
                                    json.loads(h.topics.get("upb/light/1_51_0/state", "{}")).get("state"))
                    == (40, "ON"), (h.topics.get("upb/light/1_14_0/state"), h.topics.get("upb/light/1_51_0/state")))
            h.check("both PIMs online", (h.topics.get("upb/pim/status"), h.topics.get("upb/pim_b/status"),
                                         h.topics.get("upb/powerline/status")) == ("online",) * 3)
            h.check("stats carry the PIM", (h.stats(14).get("pim"), h.stats(14).get("tx_count")) == ("B", 1),
                    h.stats(14))
            # Every probe is a query: B's devices get A's miss and B's; 15 two misses and A's repeat
            await h.wait_for("queries count every probe",
                             lambda: [h.stats(u).get("queries") for u in (14, 15, 51)] == [2, 3, 1], 3,
                             lambda: [h.stats(u).get("queries") for u in (14, 15, 51)])

            # Discovery: a second PIM device; lights available while either PIM is
            pim_b_config = json.loads(h.topics.get("homeassistant/device/upb_1_pim_b/config", "{}"))
            components = pim_b_config.get("components", {})
            h.check("PIM B discovered", components.get("pim", {}).get("state_topic") == "upb/pim_b/status"
                    and components.get("noise_floor", {}).get("state_topic") == "upb/pim_b/signal"
                    and components.get("noise_problem", {}).get("state_topic") == "upb/pim_b/signal",
                    components)
            h.check("PIM B signal published", json.loads(h.topics.get("upb/pim_b/signal", "{}"))
                    .get("signal_strength") == 0x53)
            light = json.loads(h.topics["homeassistant/device/upb_1_14/config"])["components"]["light_0"]
            h.check("light availability: either PIM", {"topic": "upb/powerline/status"}
                    in light["availability"], light["availability"])

            # Each message on the PIM needing the fewest transmissions
            on = {"state": "ON"}
            await h.command("upb/light/1_14_0/set", on, ([], [ENC[1].goto(dev(14), 100, -1)]))
            await h.command("upb/light/1_51_0/set", {"state": "OFF"}, ([ENC[1].goto(dev(51), 0, -1)], []))
            await h.command("upb/light/1_15_0/set", on, ([ENC[2].goto(dev(15), 100, -1)], []))
            await h.command("upb/light/1_14_0/cmd", "update_status", ([], [query(14, 1)]))
            # Link 1: 51 and the keypad, both on A's phase; 20: 14 and 15
            h.events.clear()
            await h.command("upb/link/1_1/cmd", {"command": "activate"},
                            ([ENC[1].activate_link(LinkAddr(NET, 1))], []))
            await h.command("upb/link/1_20/set", "ON", ([ENC[2].activate_link(LinkAddr(NET, 20))], []))

            # Commands for both PIMs at once still go out one at a time
            line.uses.clear()
            for pim in h.pims:
                pim.received.clear()
            for unit in (14, 51, 15):
                await client.publish(f"upb/light/1_{unit}_0/set", json.dumps({"state": "OFF"}))
            await client.publish("upb/light/1_14_0/cmd", "update_status")
            await asyncio.sleep(1.5)
            h.check("burst routed", (pim_a.received, pim_b.received)
                    == ([ENC[1].goto(dev(51), 0, -1), ENC[2].goto(dev(15), 0, -1)],
                        [ENC[1].goto(dev(14), 0, -1), query(14, 1)]), (pim_a.received, pim_b.received))
            h.check("burst: one transmitter at a time", line.collisions() == [], line.collisions())
            h.check("burst: line use recorded (5 commands, keypad LED fixes)", len(line.uses) >= 5, line.uses)

            # What both PIMs hear is handled once: our own repeated commands
            # echoed by the other PIM, and a keypad press
            h.check("one event per scene sent", h.events == ["upb/link/1_1/event", "upb/link/1_20/event"],
                    h.events)
            observed = h.stats(51).get("observed")
            h.events.clear()
            press = ENC[2].activate_link(LinkAddr(NET, 1))
            pim_a.hear(press)
            await asyncio.sleep(0.2)
            pim_b.hear(press)
            await asyncio.sleep(1.5)
            h.check("keypad press heard by both -> one event", h.events == ["upb/link/1_1/event"], h.events)
            h.check("... observed once", h.stats(51).get("observed") == observed + 1, h.stats(51))
            h.check("our own repeated goto echoed by B isn't someone else's",
                    h.stats(15).get("observed") == 0, h.stats(15))

            # PIM B unplugged: its devices go to A, repeated
            await pim_b.stop()
            await h.wait_for("PIM B offline", lambda: h.topics.get("upb/pim_b/status") == "offline", 3)
            h.check("still reachable", h.topics.get("upb/powerline/status") == "online")
            await h.wait_for("14 moves to A, repeated", lambda: h.route(14) == ("A", 2), 3, lambda: h.route(14))
            await h.command("upb/light/1_14_0/set", {"state": "OFF"}, ([ENC[2].goto(dev(14), 0, -1)], []))

            # ... and back: only B is tried, for the devices A doesn't reach once
            for pim in h.pims:
                pim.received.clear()
            await pim_b.start(PIM_B_PORT)
            await h.wait_for("PIM B reconnects", lambda: h.topics.get("upb/pim_b/status") == "online", 20)
            await h.wait_for("14 back on B", lambda: h.route(14) == ("B", 1), 15, lambda: h.route(14))
            await asyncio.sleep(2)
            h.check("reconnect: no full read, nothing repeated", (pim_a.received, pim_b.received)
                    == ([], [query(14, 1), query(15, 1)]), (pim_a.received, pim_b.received))

            bridge.terminate()
            await bridge.wait()
            collector.cancel()
    finally:
        if bridge.returncode is None:
            bridge.kill()
        for pim in (pim_a, pim_b):
            if pim.server:
                pim.server.close()
        await broker.shutdown()

    print(f"\n{h.failures} failure(s)")
    return 1 if h.failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
