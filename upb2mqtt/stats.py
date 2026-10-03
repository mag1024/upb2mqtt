"""Per-device traffic statistics.

Kept per UPB unit (a multi-channel device's channels together) since the
bridge started; nothing is persisted, so a restart starts every count at 0
again (Home Assistant's total_increasing sensors expect exactly that).

- sent / sent_scene / queries: messages the bridge sent to the device
  directly, links it sent that the device hears, and state queries
  (tx_count probes included, answered or not).
- observed: powerline traffic from anyone else that concerns the device: a
  link it hears, a message to it, or a message it sent itself (a local
  press), other than its reply to our query.
- timeouts: queries it never answered, upb-lib's retry included (tx_count
  probes, which aren't retried, don't count).
- latency: of the last query, from the PIM accepting it (PA, so queue waits
  don't count) to the device's state report. Only queries are timed, so a
  reading comes from each read of all devices or a Refresh state press.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from upb_lib.const import UpbCommand
from upb_lib.message import PIM_ID

QUERY = UpbCommand.REPORT_STATE.value
STATE_REPORT = UpbCommand.DEVICE_STATE_REPORT.value


@dataclass
class DeviceStats:
    """Counters for one UPB unit."""

    state_changes: int = 0
    sent: int = 0
    sent_scene: int = 0
    queries: int = 0
    observed: int = 0
    timeouts: int = 0
    latency: float | None = None  # seconds
    last_heard: float | None = None  # wall clock

    def as_dict(self) -> dict[str, Any]:
        return {
            "state_changes": self.state_changes,
            "sent": self.sent,
            "sent_scene": self.sent_scene,
            "queries": self.queries,
            "observed": self.observed,
            "timeouts": self.timeouts,
            "latency_ms": None if self.latency is None else round(self.latency * 1000),
            "last_heard": datetime.fromtimestamp(self.last_heard, timezone.utc)
            .isoformat(timespec="seconds") if self.last_heard else None,
        }


class Stats:
    """Statistics for every UPB unit, fed from PIM traffic."""

    def __init__(self, clock: Callable[[], float] = time.monotonic,
                 wall: Callable[[], float] = time.time) -> None:
        self.clock = clock
        self.wall = wall
        self.devices: dict[int, DeviceStats] = {}
        self.link_members: dict[int, set[int]] = {}  # link id -> upb ids that hear it
        # Queries awaiting a reply (one per PIM at most): upb id -> when accepted
        self.pending: dict[int, float] = {}
        self.started = wall()

    def __getitem__(self, upb_id: int) -> DeviceStats:
        return self.devices.setdefault(upb_id, DeviceStats())

    def sent(self, msg) -> None:
        """A UPB message the bridge queued."""
        if msg.link:
            for upb_id in self.link_members.get(msg.dest_id, ()):
                self[upb_id].sent_scene += 1
        elif msg.msg_id == QUERY:
            self[msg.dest_id].queries += 1
        else:
            self[msg.dest_id].sent += 1

    def accepted(self, upb_id: int) -> None:
        """The PIM accepted a query to the device and is waiting for its reply."""
        self.pending[upb_id] = self.clock()

    def timed_out(self, upb_id: int, retrying: bool, probe: bool = False) -> None:
        """An attempt at a query went unanswered; upb-lib may send it again.
        A tx_count probe that goes unanswered (the device is on the far
        phase from that PIM) isn't a timeout, but stays a query."""
        self.pending.pop(upb_id, None)
        if not retrying and not probe:
            self[upb_id].timeouts += 1

    def disconnected(self) -> None:
        self.pending.clear()

    def heard(self, msg) -> None:
        """A UPB message heard on the powerline (repeats already dropped)."""
        if msg.src_id != PIM_ID:
            self[msg.src_id].last_heard = self.wall()
        if not msg.link and msg.msg_id == STATE_REPORT and msg.src_id in self.pending:
            self[msg.src_id].latency = self.clock() - self.pending.pop(msg.src_id)
            return
        affected = {msg.src_id}
        if msg.link:
            affected |= self.link_members.get(msg.dest_id, set())
        else:
            affected.add(msg.dest_id)
        for upb_id in affected - {0, PIM_ID}:  # broadcast, or us
            self[upb_id].observed += 1
