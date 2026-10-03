"""Unit tests for stats.py: per-device traffic counts and latency.

    python tests/test_stats.py
"""

import sys
from pathlib import Path

from upb_lib.devices import UpbAddr
from upb_lib.links import LinkAddr
from upb_lib.message import MessageEncode, decode

sys.path.insert(0, str(Path(__file__).parent.parent))

from stats import Stats  # noqa: E402

FAILURES = 0
ENC = MessageEncode(1)


def check(name: str, got, expected) -> None:
    global FAILURES  # pylint: disable=global-statement
    ok = got == expected
    print(f"{'PASS' if ok else 'FAIL'} {name}{'' if ok else f'  -> got {got!r}, expected {expected!r}'}")
    FAILURES += not ok


def msg(hex_msg: str):
    return decode(hex_msg)[1]


def from_device(src: int, dest: int, msg_id: int, data: list[int], link: bool = False):
    """A message a device sent (the encoder always uses the PIM as source)."""
    raw = bytearray([(0x80 if link else 0) | 7 + len(data), 0, 1, dest, src, msg_id, *data, 0])
    raw[-1] = (256 - sum(raw)) % 256
    return msg(raw.hex().upper())


def main() -> int:
    now = [100.0]
    stats = Stats(clock=lambda: now[0], wall=lambda: 1_700_000_000.0)
    stats.link_members = {17: {14, 15}, 1: {51, 13}}

    # Sent by us
    stats.sent(msg(ENC.goto(UpbAddr(1, 14, 0), 50, -1)))
    stats.sent(msg(ENC.activate_link(LinkAddr(1, 17))))
    stats.sent(msg(ENC.report_state(UpbAddr(1, 14, 0))))
    check("direct command", stats[14].sent, 1)
    check("link command -> each member", (stats[14].sent_scene, stats[15].sent_scene), (1, 1))
    check("query", stats[14].queries, 1)
    check("link to members only", stats[51].sent_scene, 0)

    # Query answered: latency from the PIM's accept to the report
    stats.accepted(14)
    now[0] += 0.8
    stats.heard(from_device(14, 0xFF, 0x86, [40]))
    d = stats[14].as_dict()
    check("latency", d["latency_ms"], 800)
    check("reply isn't observed traffic", d["observed"], 0)
    check("last heard", d["last_heard"], "2023-11-14T22:13:20+00:00")
    check("pending cleared", stats.pending, {})

    stats.accepted(14)
    now[0] += 1.2
    stats.heard(from_device(14, 0xFF, 0x86, [40]))
    d = stats[14].as_dict()
    check("latency is the last query's", d["latency_ms"], 1200)

    # Unsolicited report (no query pending) is observed
    stats.heard(from_device(14, 0, 0x86, [0]))
    check("unsolicited report observed", stats[14].observed, 1)
    check("broadcast dest not counted", 0 in stats.devices, False)

    # A keypad (13) activates link 1: the keypad and the link's lights
    stats.heard(from_device(13, 1, 0x20, [], link=True))
    check("keypad press -> link members", (stats[51].observed, stats[13].observed), (1, 1))
    # Someone else's direct goto
    stats.heard(from_device(40, 15, 0x22, [75]))
    check("direct goto -> target and sender", (stats[15].observed, stats[40].observed), (1, 1))

    # A reply from another device while one is pending doesn't close it
    stats.accepted(15)
    stats.heard(from_device(51, 0xFF, 0x86, [100]))
    check("other device's report observed", stats[51].observed, 2)
    check("still pending", 15 in stats.pending, True)

    # Timeouts
    stats.timed_out(15, retrying=True)
    check("missed attempt that's resent isn't a timeout", stats[15].timeouts, 0)
    check("timeout clears pending", stats.pending, {})
    stats.timed_out(15, retrying=False)
    check("retry missed too -> one timeout", stats[15].timeouts, 1)
    stats.timed_out(15, retrying=False, probe=True)
    check("probe miss not counted", stats[15].timeouts, 1)
    stats.accepted(15)
    stats.disconnected()
    check("disconnect clears pending", stats.pending, {})

    # Queries in flight on both PIMs at once each get their latency
    stats.accepted(14)
    now[0] += 0.1
    stats.accepted(15)
    now[0] += 0.5
    stats.heard(from_device(15, 0xFF, 0x86, [0]))
    stats.heard(from_device(14, 0xFF, 0x86, [0]))
    check("two pending queries", (stats[14].as_dict()["latency_ms"], stats[15].as_dict()["latency_ms"]),
          (600, 500))

    check("no latency yet", stats[51].as_dict()["latency_ms"], None)

    print(f"\n{FAILURES} failure(s)")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
