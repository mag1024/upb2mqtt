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

A link shown on a keypad may also stand in when some of its lights aren't in
the batch but are known to be at its level already, such as turning off the
last light that was on in an "off" scene. That's one link command instead of
a direct command plus Indicates to fix the keypad LEDs, since the keypads
update their LEDs from the link themselves.
"""

from __future__ import annotations

from dataclasses import dataclass

ACTIVATE = "activate"
DEACTIVATE = "deactivate"
# A link relying on current levels re-commands lights nobody asked to change;
# keep that to small links.
MAX_STATE_LINK_LIGHTS = 10


@dataclass
class Scene:
    link_id: int
    name: str
    presets: dict[str, int]  # device index -> level the link sets (0-100)


def plan(
    batch: dict[str, int],
    scenes: list[Scene],
    current: dict[str, int] | None = None,
    shown: frozenset[int] | set[int] = frozenset(),
) -> tuple[list[tuple[Scene, str]], dict[str, int]]:
    """Link commands covering a batch of device levels, and what's left over.

    current: known levels of lights not in the batch (unknown ones left out).
    shown: link ids on some keypad's indicator table; only these (with at most
    MAX_STATE_LINK_LIGHTS lights) may count current levels.

    Links fully in the batch go first: bigger links first, and at equal size
    an activation (the way a keypad's own "off" scene works) before a
    deactivation. Then links shown on keypads that also rely on current
    levels: most batch lights covered first, then an activation, then the
    fewest lights (fewest re-commanded). Links of fewer than two lights save nothing and are
    skipped.
    """
    current = current or {}
    options = [
        (scene, action)
        for scene in scenes
        if len(scene.presets) >= 2
        for action in (ACTIVATE, DEACTIVATE)
    ]
    options.sort(key=lambda o: (-len(o[0].presets), o[1] != ACTIVATE, o[0].link_id))
    remaining = dict(batch)
    chosen = []

    def target(scene: Scene, action: str) -> dict[str, int]:
        return scene.presets if action == ACTIVATE else dict.fromkeys(scene.presets, 0)

    def take(scene: Scene, action: str) -> None:
        chosen.append((scene, action))
        for device in scene.presets:
            remaining.pop(device, None)

    for scene, action in options:
        if scene.presets.keys() <= remaining.keys() and all(
            remaining[d] == level for d, level in target(scene, action).items()
        ):
            take(scene, action)

    def completes(scene: Scene, action: str) -> bool:
        """Every light is in the batch at its level, or outside it and already there."""
        for device, level in target(scene, action).items():
            if device in remaining:
                if remaining[device] != level:
                    return False
            elif device in batch or current.get(device) != level:
                return False  # sent by another link, or not known to be there
        return True

    while candidates := [
        (scene, action)
        for scene, action in options
        if scene.link_id in shown
        and len(scene.presets) <= MAX_STATE_LINK_LIGHTS
        and scene.presets.keys() & remaining.keys()
        and completes(scene, action)
    ]:
        take(*min(candidates, key=lambda o: (
            -len(o[0].presets.keys() & remaining.keys()),
            o[1] != ACTIVATE,
            len(o[0].presets),
            o[0].link_id,
        )))
    return chosen, remaining
