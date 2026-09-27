"""Unit tests for scenes.py: which link commands replace a batch of light commands.

    python tests/test_scenes.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scenes import Scene, plan  # noqa: E402

FAILURES = 0


def check(name: str, got, expected) -> None:
    global FAILURES  # pylint: disable=global-statement
    ok = got == expected
    print(f"{'PASS' if ok else 'FAIL'} {name}{'' if ok else f'  -> got {got!r}, expected {expected!r}'}")
    FAILURES += not ok


def result(batch: dict[str, int], scenes: list[Scene], current=None, shown=frozenset()):
    chosen, rest = plan(batch, scenes, current, shown)
    return [(s.name, action) for s, action in chosen], sorted(rest)


def main() -> int:
    # Shaped like the real network's master bath and theater links
    bath_sink = Scene(1, "Bath Sink", {"vanity": 100, "sink": 100, "main": 100})
    bath_shower = Scene(3, "Bath Shower", {"shower": 100, "vanity": 100, "sink": 100, "main": 100})
    bath_off = Scene(4, "Bath Off", {"shower": 0, "vanity": 0, "sink": 0, "main": 0})
    vanity = Scene(52, "Vanity", {"vanity": 100})
    theater_off = Scene(39, "Theater Off", {"t1": 0, "t2": 0, "t3": 0})
    mixed = Scene(60, "Reading", {"lamp": 100, "main": 0})
    dim = Scene(61, "Dim", {"a": 30, "b": 60})
    scenes = [bath_sink, bath_shower, bath_off, vanity, theater_off, mixed, dim]

    check("group on -> exact scene", result({"vanity": 100, "sink": 100, "main": 100}, scenes),
          ([("Bath Sink", "activate")], []))
    check("group off -> off scene preferred over deactivating one as big",
          result(dict.fromkeys(["shower", "vanity", "sink", "main"], 0), scenes),
          ([("Bath Off", "activate")], []))
    check("group off -> deactivate an on scene",
          result(dict.fromkeys(["vanity", "sink", "main"], 0), scenes),
          ([("Bath Sink", "deactivate")], []))
    check("biggest scene wins", result(dict.fromkeys(["shower", "vanity", "sink", "main"], 100), scenes),
          ([("Bath Shower", "activate")], []))
    check("leftovers sent directly", result({"t1": 0, "t2": 0, "t3": 0, "x": 0, "y": 50}, scenes),
          ([("Theater Off", "activate")], ["x", "y"]))
    check("several scenes", result({"t1": 0, "t2": 0, "t3": 0, "vanity": 0, "sink": 0, "main": 0}, scenes),
          ([("Theater Off", "activate"), ("Bath Sink", "deactivate")], []))
    check("scene light missing from batch -> no match", result({"t1": 0, "t2": 0}, scenes),
          ([], ["t1", "t2"]))
    check("wrong level -> no match", result({"vanity": 100, "sink": 50, "main": 100}, scenes),
          ([], ["main", "sink", "vanity"]))
    check("on command doesn't deactivate", result({"t1": 100, "t2": 100, "t3": 100}, scenes),
          ([], ["t1", "t2", "t3"]))
    check("mixed on/off scene", result({"lamp": 100, "main": 0}, scenes), ([("Reading", "activate")], []))
    check("preset levels must match", result({"a": 30, "b": 60}, scenes), ([("Dim", "activate")], []))
    check("one-light scene never used", result({"vanity": 100}, scenes), ([], ["vanity"]))
    check("empty batch", result({}, scenes), ([], []))

    # Lights outside the batch already at a keypad scene's level
    all_shown = {s.link_id for s in scenes}
    bath_dark = {"shower": 0, "vanity": 0, "sink": 0}
    check("last light off -> off scene", result({"main": 0}, scenes, bath_dark, all_shown),
          ([("Bath Off", "activate")], []))
    check("not on a keypad -> direct", result({"main": 0}, scenes, bath_dark, all_shown - {4, 1, 3}),
          ([], ["main"]))
    check("other light on -> direct", result({"main": 0}, scenes, {**bath_dark, "sink": 100}, all_shown),
          ([], ["main"]))
    check("other light unknown -> direct",
          result({"main": 0}, scenes, {"shower": 0, "vanity": 0}, all_shown), ([], ["main"]))
    check("last light on -> on scene",
          result({"main": 100}, scenes, {"vanity": 100, "sink": 100, "shower": 0}, all_shown),
          ([("Bath Sink", "activate")], []))
    check("smallest scene that covers it",
          result({"main": 100}, scenes, {"vanity": 100, "sink": 100, "shower": 100}, all_shown),
          ([("Bath Sink", "activate")], []))
    check("most batch lights covered first",
          result({"main": 0, "sink": 0}, scenes, {"vanity": 0, "shower": 0, "lamp": 100}, all_shown),
          ([("Bath Off", "activate")], []))
    check("exact batch match still first",
          result({"t1": 0, "t2": 0, "t3": 0, "main": 0}, scenes, bath_dark, all_shown),
          ([("Theater Off", "activate"), ("Bath Off", "activate")], []))
    check("light sent by another scene isn't 'already there'",
          result({"vanity": 0, "sink": 0, "main": 0, "shower": 100}, scenes, {}, all_shown),
          ([("Bath Sink", "deactivate")], ["shower"]))
    house = [f"h{i}" for i in range(1, 12)]
    big_off = Scene(70, "House Off", dict.fromkeys(house, 0))
    check("more than 10 lights -> direct",
          result({"h1": 0}, [big_off], dict.fromkeys(house[1:], 0), {70}), ([], ["h1"]))
    check("more than 10 lights, all in the batch -> scene",
          result(dict.fromkeys(house, 0), [big_off], {}, {70}), ([("House Off", "activate")], []))
    ten_off = Scene(71, "Ten Off", dict.fromkeys(house[:10], 0))
    check("10 lights -> scene",
          result({"h1": 0}, [ten_off], dict.fromkeys(house[1:10], 0), {71}), ([("Ten Off", "activate")], []))
    check("batch level must match the preset",
          result({"a": 50}, scenes, {"b": 60}, all_shown), ([], ["a"]))

    print(f"\n{FAILURES} failure(s)")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
