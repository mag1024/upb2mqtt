"""Scene substitution for batches of light commands.

Home Assistant turns a light group on or off by commanding each member at
once, which becomes one direct Goto per light. When a batch of commands does
exactly what a UPB link (scene) would, one link command replaces them: less
powerline traffic, and keypads update their LEDs from the link as they do for
a button press.

A link can stand in for part of a batch when every one of its lights is in
the batch and
  - activate: each light is commanded to its preset level (preset 0 = off), or
  - deactivate: each light is commanded off.
Lights outside the links chosen are sent their own commands.
"""

from __future__ import annotations

from dataclasses import dataclass

ACTIVATE = "activate"
DEACTIVATE = "deactivate"


@dataclass
class Scene:
    link_id: int
    name: str
    presets: dict[str, int]  # device index -> level the link sets (0-100)


def plan(
    batch: dict[str, int], scenes: list[Scene]
) -> tuple[list[tuple[Scene, str]], dict[str, int]]:
    """Link commands covering a batch of device levels, and what's left over.

    Greedy: bigger links first, and at equal size an activation (the way a
    keypad's own "off" scene works) before a deactivation. Links of fewer
    than two lights save nothing and are skipped.
    """
    options = [
        (scene, action)
        for scene in scenes
        if len(scene.presets) >= 2
        for action in (ACTIVATE, DEACTIVATE)
    ]
    options.sort(key=lambda o: (-len(o[0].presets), o[1] != ACTIVATE, o[0].link_id))
    remaining = dict(batch)
    chosen = []
    for scene, action in options:
        if not scene.presets.keys() <= remaining.keys():
            continue
        if action == ACTIVATE:
            ok = all(remaining[d] == level for d, level in scene.presets.items())
        else:
            ok = all(remaining[d] == 0 for d in scene.presets)
        if ok:
            chosen.append((scene, action))
            for device in scene.presets:
                del remaining[device]
    return chosen, remaining
