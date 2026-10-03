"""A second PIM on the same UPB network.

upb-lib drives one PIM. The second gets its own upb-lib Connection (its own
serial link, write queue and retries), and what it hears is handed to the
first PIM's handlers, so one device and link model sees both.

Only one PIM may transmit at a time. Line holds every UPB message until the
powerline is free (nothing in flight on either PIM, the last transmission
finished, no more copies of a message just heard to come), then hands it to
its PIM, so the two take turns as if they shared one queue.

A message sent twice reaches both phases, so both PIMs hear it: a device's
message, and each PIM's own message, heard by the other. Handled twice, a
keypad press would fire its link event twice, and a scene the bridge sent
would fire a second event when the other PIM heard it. Duplicates drops
those copies. A message sent once stays on its phase, so it reaches one PIM.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from upb_lib.connection import Connection
from upb_lib.const import PimCommand
from upb_lib.message import decode
from upb_lib.notify import Notifier

LOG = logging.getLogger("upb2mqtt.pims")

# A message both PIMs hear arrives on each within about half a second (its
# copies are 0.35 s apart); one PIM's own message reaches the other once it
# has been sent, under a second after the PIM accepts it (PA).
HEARD_WINDOW = 1.0
SENT_WINDOW = 3.0

# A PIM reports PK or PN once it has finished transmitting any message,
# queries, direct commands and links alike (measured on a PCS PIM: 0.34-0.40 s
# after it's written for one copy, 0.63-0.72 s for two). If neither comes,
# the line counts as free this long after the message was handed over.
TX_DONE_TIMEOUT = 2.0
# Gap between a repeated message's copies (0.29-0.32 s measured, longer
# messages longer): a message heard with copies still to come keeps the
# line busy.
COPY_TIME = 0.35
POLL_INTERVAL = 0.05  # for what no PIM line reports: timeouts, the times above

Forward = Callable[[str, dict[str, Any]], None]


class _Notifier(Notifier):
    """Connection events stay with this PIM; messages it hears go to forward."""

    def __init__(self, forward: Forward) -> None:
        super().__init__()
        self.forward = forward

    def notify(self, notify_type: str, notify_parameters: dict[str, Any]) -> None:
        if "msg" in notify_parameters:
            self.forward(notify_type, notify_parameters)
        else:
            super().notify(notify_type, notify_parameters)


class SecondPim:
    """Connection to a second PIM, feeding what it hears to forward."""

    def __init__(self, url: str, forward: Forward) -> None:
        self.url = url
        self.notifier = _Notifier(forward)
        self.connection = Connection(url, self.notifier)

    def add_handler(self, event: str, handler: Callable[..., None]) -> None:
        """connected, disconnected or timeout (as upb-lib's UpbPim.add_handler)."""
        self.notifier.attach(event, handler)


def _key(msg) -> tuple:
    """A message apart from its control word (transmit count and sequence)."""
    return (msg.link, msg.network_id, msg.dest_id, msg.src_id, msg.msg_id, bytes(msg.data))


class Duplicates:
    """Recognises a message the other PIM already heard, or sent."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self.recent: dict[tuple, tuple[int, float]] = {}  # key -> PIM, expiry

    def _note(self, pim: int, msg, window: float) -> None:
        now = self.clock()
        self.recent = {k: v for k, v in self.recent.items() if v[1] > now}
        self.recent[_key(msg)] = (pim, now + window)

    def sent(self, pim: int, msg) -> None:
        """A PIM accepted a message to send."""
        if msg.tx_count:  # sent once, it doesn't reach the other phase
            self._note(pim, msg, SENT_WINDOW)

    def heard(self, pim: int, msg) -> bool:
        """Whether a message a PIM heard is new. upb-lib already drops a PIM's
        repeat of a message it heard; this drops the other PIM's copy."""
        if not msg.tx_count:
            return True  # sent once: only one PIM can hear it
        seen = self.recent.get(_key(msg))
        if seen and seen[0] != pim and seen[1] > self.clock():
            return False
        self._note(pim, msg, HEARD_WINDOW)
        return True


class Line:
    """The powerline the PIMs share: UPB messages go out one at a time, in
    the order sent, each from the PIM it's for."""

    def __init__(self, conns: list[Connection], clock: Callable[[], float] = time.monotonic) -> None:
        self.conns = conns
        self.clock = clock
        self.queue: deque[tuple[int, str, bytearray | None, int | None]] = deque()
        self.sending: dict[int, float] = {}  # PIM -> when given a message, until PK/PN
        self.quiet_until = 0.0  # copies of a message heard still to come

    def send(self, pim: int, command: PimCommand, msg: str, rsp: bytearray | None,
             retries: int | None = None) -> None:
        """Connection.send, once the line is free; retries overrides upb-lib's one retry."""
        if command != PimCommand.TX_UPB_MSG:
            self.conns[pim].send(command, msg, rsp)  # PIM registers: nothing goes on the line
            return
        self.queue.append((pim, msg, rsp, retries))
        self.pump()

    def busy(self) -> bool:
        now = self.clock()
        if now < self.quiet_until:
            return True
        for pim, connection in enumerate(self.conns):
            if not connection.is_connected():
                continue
            # pylint: disable=protected-access
            if connection._write_queue:  # being written, or awaiting PA or a reply
                return True
            if now - self.sending.get(pim, -TX_DONE_TIMEOUT) < TX_DONE_TIMEOUT:
                return True
        return False

    def idle(self) -> bool:
        return not self.queue and not self.busy()

    def pump(self) -> None:
        while self.queue and not self.busy():
            pim, msg, rsp, retries = self.queue.popleft()
            connection = self.conns[pim]
            if not connection.is_connected():
                LOG.warning("PIM %s disconnected; dropping %s", "AB"[pim], msg)
                continue
            connection.send(PimCommand.TX_UPB_MSG, msg, rsp)
            if retries is not None:
                # pylint: disable=protected-access
                connection._write_queue[-1].retry_count = retries
            self.sending[pim] = self.clock()

    def pim_line(self, pim: int, command: str, data: str) -> None:
        """A line from a PIM, after upb-lib has handled it."""
        if command in ("PK", "PN", "PE"):  # done transmitting, or rejected
            self.sending.pop(pim, None)
        elif command == "PU":
            try:
                msg = decode(data)[1]
            except ValueError:
                msg = None
            if msg and msg.tx_seq < msg.tx_count:
                self.quiet_until = max(self.quiet_until,
                                       self.clock() + (msg.tx_count - msg.tx_seq) * COPY_TIME)
        self.pump()

    def disconnected(self, pim: int) -> None:
        self.sending.pop(pim, None)
        self.pump()

    async def run(self) -> None:
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            self.pump()
