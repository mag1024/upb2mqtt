"""Unit tests for txcount.py: per-device and per-link transmit counts, and PIM routes.

    python tests/test_txcount.py
"""

import sys
from pathlib import Path

from upb_lib.devices import UpbAddr
from upb_lib.links import LinkAddr
from upb_lib.message import decode

sys.path.insert(0, str(Path(__file__).parent.parent))

from txcount import TxCounts, TxEncoder  # noqa: E402

FAILURES = 0


def check(name: str, got, expected) -> None:
    global FAILURES  # pylint: disable=global-statement
    ok = got == expected
    print(f"{'PASS' if ok else 'FAIL'} {name}{'' if ok else f'  -> got {got!r}, expected {expected!r}'}")
    FAILURES += not ok


def main() -> int:
    tx = TxCounts(2)
    tx.add_listeners(10, [14, 51])  # a device that needs 2 and one that doesn't
    tx.add_listeners(11, [51])
    tx.add_listeners(11, [13])  # a keypad showing link 11
    tx.single[0].update({14: False, 51: True})

    check("single device", tx.device(51), 1)
    check("device needing repeats", tx.device(14), 2)
    check("unprobed device", tx.device(13), 2)
    check("link: max of its listeners", tx.link(10), 2)
    check("link: an unprobed keypad counts", tx.link(11), 2)
    tx.single[0][13] = True
    check("link: all single", tx.link(11), 1)
    check("link nobody listens to", tx.link(99), 2)

    enc = TxEncoder(tx.for_addr)
    check("device message tx bits", decode(enc.goto(UpbAddr(1, 51, 0), 50, -1))[1].tx_count, 0)
    check("repeat device tx bits", decode(enc.goto(UpbAddr(1, 14, 0), 50, -1))[1].tx_count, 1)
    check("link message tx bits", decode(enc.activate_link(LinkAddr(1, 10)))[1].tx_count, 1)
    check("explicit control word kept", decode(enc.report_state(UpbAddr(1, 14, 0), ctl=0))[1].tx_count, 0)
    check("one PIM: always it", tx.route(False, 14), (0, 2))

    # Two PIMs on opposite phases: 51 and 13 hear the first send once, 14 the
    # second; 15 neither
    tx = TxCounts(2, pims=2)
    tx.add_listeners(10, [14, 51])  # one device on each phase
    tx.add_listeners(12, [14])
    tx.add_listeners(13, [51, 13])
    tx.single[0].update({51: True, 13: True, 14: False, 15: False})
    tx.single[1].update({14: True, 15: False})
    check("device on the first PIM's phase", tx.route(False, 51), (0, 1))
    check("device on the second PIM's phase", tx.route(False, 14), (1, 1))
    check("device neither reaches once -> first, repeated", tx.route(False, 15), (0, 2))
    check("unprobed device -> first, repeated", tx.route(False, 77), (0, 2))
    check("link on the second's phase", tx.route(True, 12), (1, 1))
    check("link on the first's phase", tx.route(True, 13), (0, 1))
    check("link across phases -> first, repeated", tx.route(True, 10), (0, 2))
    check("link nobody listens to -> first", tx.route(True, 99), (0, 2))
    check("device(): count of the route", (tx.device(14), tx.device(15)), (1, 2))
    up = {0: False, 1: True}
    tx.online = lambda pim: up[pim]
    check("first PIM down: second, repeated", tx.route(False, 51), (1, 2))
    check("first PIM down: second's own", tx.route(False, 14), (1, 1))
    up.update({0: True, 1: False})
    check("second PIM down: first, repeated", tx.route(False, 14), (0, 2))
    up[0] = False
    check("both down: first", tx.route(False, 14), (0, 2))

    print(f"\n{FAILURES} failure(s)")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
