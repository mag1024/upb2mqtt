"""Keypad indicator (LED) sync.

A KPC keypad's toggle buttons send activate or deactivate depending on
whether the button's LED is lit, and the keypad only updates its LEDs from
link (scene) traffic that matches a row of its indicator table. Lights
changed with direct device commands leave the LEDs, and so the toggles,
stale.

This module works out what each LED should show from the current light
levels, using the keypad's own indicator table, and keeps a model of what the
keypad is showing: read with Report State (a one-byte LED bitmask), then
updated from the link traffic the keypad hears. Where the two differ, the
bridge sets the LED with the Indicate command (0x26, direct, channel = bit+1).

An LED is managed only if every indicator row that turns it "on" is a scene
with light members in the UPE file, and no row sets it "opposite". Other LEDs
(gate status, reset-only scenes, ...) are left to the keypad and to whatever
already drives them.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field

LOG = logging.getLogger("upb2mqtt.keypads")

INDICATE = 0x26
LEVEL_ON = 0x64
# Indicator actions, from the two mask bits (mask1, mask2) of each LED
ACTIONS = {0: "opp", 1: "off", 2: "on"}  # 3 = no change
ON_COMMANDS = {"activated"}
OFF_COMMANDS = {"deactivated"}

# (level, preset) pairs of a scene's members; level None = unknown
Members = list[tuple[int | None, int]]


@dataclass
class IndicatorRow:
    link_id: int
    actions: dict[int, str]  # LED bit -> "on" | "off" | "opp"


@dataclass
class Keypad:
    upb_id: int
    name: str
    labels: dict[int, str] = field(default_factory=dict)  # LED bit -> button label
    rows: list[IndicatorRow] = field(default_factory=list)
    # What the keypad shows, per LED bit; None = unknown. Empty until read.
    leds: dict[int, bool | None] = field(default_factory=dict)

    def label(self, bit: int) -> str:
        return self.labels.get(bit, f"LED{bit + 1}")

    @property
    def initialized(self) -> bool:
        return bool(self.leds)

    def links(self) -> set[int]:
        return {row.link_id for row in self.rows}

    def on_rows(self, bit: int) -> list[int]:
        return [r.link_id for r in self.rows if r.actions.get(bit) == "on"]

    def managed_bits(self, has_lights: Callable[[int], bool]) -> list[int]:
        bits = []
        for bit in range(8):
            actions = [r.actions[bit] for r in self.rows if bit in r.actions]
            ons = self.on_rows(bit)
            if ons and "opp" not in actions and all(has_lights(link) for link in ons):
                bits.append(bit)
        return bits

    def set_report(self, bitmask: int) -> None:
        self.leds = {bit: bool(bitmask & (1 << bit)) for bit in range(8)}

    def apply_link(self, link_id: int, command: str, level: float) -> None:
        """Update the model as the keypad reacts to link traffic it hears."""
        if not self.initialized:
            return
        if command == "goto" and level >= 0:
            command = "activated" if level > 0 else "deactivated"
        for row in self.rows:
            if row.link_id != link_id:
                continue
            for bit, action in row.actions.items():
                if command in ON_COMMANDS:
                    self.leds[bit] = action == "on"
                elif command in OFF_COMMANDS:
                    self.leds[bit] = action == "opp"
                else:  # fade, blink: undocumented for indicators
                    self.leds[bit] = None


def parse_keypads(upe_file: str, keypad_ids: set[int]) -> dict[int, Keypad]:
    """Indicator tables (record 13) and button labels (17) from a UPE file."""
    keypads: dict[int, Keypad] = {}
    with open(upe_file, encoding="utf-8") as handle:
        records = [line.strip().split(",") for line in handle]
    for rec in records:
        if rec[0] == "3" and int(rec[1]) in keypad_ids:
            keypads[int(rec[1])] = Keypad(int(rec[1]), f"{rec[11]} {rec[12]}")
    for rec in records:
        if rec[0] not in ("13", "17") or int(rec[3]) not in keypads:
            continue
        keypad = keypads[int(rec[3])]
        if rec[0] == "17":
            keypad.labels[int(rec[2])] = rec[4]
        elif (link_id := int(rec[4])) != 255:
            mask1, mask2 = int(rec[5]), int(rec[6])
            actions = {}
            for bit in range(8):
                code = ((mask1 >> bit) & 1) * 2 + ((mask2 >> bit) & 1)
                if code in ACTIONS:
                    actions[bit] = ACTIONS[code]
            keypad.rows.append(IndicatorRow(link_id, actions))
    return keypads


def scene_active(members: Members, match: str) -> bool | None:
    """Whether lights are in a scene's state; None if a level is unknown.

    "all": every member is at its preset's on/off state (level ignored).
    "any": for a scene that turns lights on, any of those is on. A scene that
    only turns lights off is active when all of them are off, in both modes.
    """
    if any(level is None for level, _ in members):
        return None
    on_members = [level > 0 for level, preset in members if preset > 0]
    if match == "any" and on_members:
        return any(on_members)
    return all((level > 0) == (preset > 0) for level, preset in members)
