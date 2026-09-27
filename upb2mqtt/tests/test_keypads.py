"""Unit tests for keypads.py: scene matching, the LED model and UPE parsing.

    python tests/test_keypads.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from keypads import IndicatorRow, Keypad, parse_keypads, scene_active  # noqa: E402

FAILURES = 0


def check(name: str, got, expected) -> None:
    global FAILURES  # pylint: disable=global-statement
    ok = got == expected
    print(f"{'PASS' if ok else 'FAIL'} {name}{'' if ok else f'  -> got {got!r}, expected {expected!r}'}")
    FAILURES += not ok


def main() -> int:
    # Scene matching: (level, preset) per member
    two_on = [(100, 100), (0, 100)]
    check("all: partly on is inactive", scene_active(two_on, "all"), False)
    check("any: partly on is active", scene_active(two_on, "any"), True)
    check("all: dimmed still counts as on", scene_active([(20, 100), (5, 100)], "all"), True)
    check("off-only scene, all off", scene_active([(0, 0), (0, 0)], "any"), True)
    check("off-only scene, one on", scene_active([(0, 0), (40, 0)], "any"), False)
    check("mixed scene needs its off members off", scene_active([(100, 100), (30, 0)], "all"), False)
    check("unknown level", scene_active([(None, 100), (100, 100)], "any"), None)

    # The keypad model follows its indicator table
    keypad = Keypad(1, "KP", rows=[
        IndicatorRow(30, {0: "on", 1: "on"}),
        IndicatorRow(39, {0: "off", 1: "off"}),
        IndicatorRow(40, {2: "opp"}),
    ])
    keypad.apply_link(30, "activated", -1)
    check("ignores traffic until read", keypad.leds, {})
    keypad.set_report(0b100)
    keypad.apply_link(30, "activated", -1)
    check("activate: on -> lit", (keypad.leds[0], keypad.leds[1], keypad.leds[2]), (True, True, True))
    keypad.apply_link(30, "deactivated", -1)
    check("deactivate: on -> off", (keypad.leds[0], keypad.leds[1]), (False, False))
    keypad.apply_link(40, "activated", -1)
    check("activate: opp -> off", keypad.leds[2], False)
    keypad.apply_link(40, "goto", 0)
    check("goto 0 is off-type: opp -> lit", keypad.leds[2], True)
    keypad.apply_link(30, "goto", 50)
    check("goto >0 is on-type", keypad.leds[0], True)
    keypad.apply_link(39, "fade_started", 0)
    check("fade: unknown", (keypad.leds[0], keypad.leds[1]), (None, None))
    check("unlisted scene: no change", (keypad.apply_link(77, "activated", -1), keypad.leds[2]), (None, True))

    # Managed LEDs: on-rows must all be light scenes, no opposite rows
    lights = {30, 39}.__contains__
    check("managed bits", keypad.managed_bits(lights), [0, 1])
    check("no-light scene -> unmanaged", keypad.managed_bits({39}.__contains__), [])

    # UPE parsing (the test network's Entry KP)
    keypads = parse_keypads(str(Path(__file__).parent / "test_network.upe"), {13})
    entry = keypads[13]
    check("labels", entry.labels, {0: "B1", 1: "B5", 2: "B2", 3: "B3"})
    check("rows", [(r.link_id, r.actions) for r in entry.rows], [
        (52, {0: "on"}), (17, {1: "on"}), (20, {2: "on"}), (4, {0: "off"}),
        (99, {0: "off", 1: "off", 2: "off", 3: "on"}),
    ])
    check("entry managed bits", entry.managed_bits({52, 17, 20, 4}.__contains__), [0, 1, 2])

    print(f"\n{FAILURES} failure(s)")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
