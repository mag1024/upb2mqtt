"""Unit tests for pims.py: dropping what both PIMs hear, one transmitter at a time.

    python tests/test_pims.py
"""

import sys
from collections import deque
from pathlib import Path

from upb_lib.const import PimCommand
from upb_lib.devices import UpbAddr
from upb_lib.links import LinkAddr
from upb_lib.message import MessageEncode, decode

sys.path.insert(0, str(Path(__file__).parent.parent))

from pims import COPY_TIME, TX_DONE_TIMEOUT, Duplicates, Line  # noqa: E402

FAILURES = 0


def check(name: str, got, expected) -> None:
    global FAILURES  # pylint: disable=global-statement
    ok = got == expected
    print(f"{'PASS' if ok else 'FAIL'} {name}{'' if ok else f'  -> got {got!r}, expected {expected!r}'}")
    FAILURES += not ok


def press(link_id: int, tx: int = 2, seq: int = 0):
    """A keypad's link activation, copy seq of tx."""
    raw = bytearray([0x87, (tx - 1) << 2 | seq, 1, link_id, 13, 0x20, 0])
    raw[-1] = (256 - sum(raw)) % 256
    return decode(raw.hex().upper())[1]


class FakeConnection:
    """upb-lib's Connection, as far as Line uses it."""

    class Entry:
        def __init__(self, command, msg, rsp):
            self.pim_command, self.msg, self.response_cmd = command, msg, rsp
            self.retry_count = 1

    def __init__(self) -> None:
        self._write_queue: deque = deque()
        self.connected = True
        self.sent: list[str] = []

    def is_connected(self) -> bool:
        return self.connected

    def send(self, command, msg, rsp) -> None:
        self._write_queue.append(self.Entry(command, msg, rsp))
        self.sent.append(msg)


def test_line(now: list[float]) -> None:
    conns = [FakeConnection(), FakeConnection()]
    line = Line(conns, clock=lambda: now[0])
    enc = MessageEncode(1)
    goto = [enc.goto(UpbAddr(1, unit, 0), 0, -1) for unit in (14, 51, 15)]
    query = enc.report_state(UpbAddr(1, 14, 0))

    line.send(1, PimCommand.TX_UPB_MSG, goto[0], None)
    line.send(0, PimCommand.TX_UPB_MSG, goto[1], None)
    check("first goes out at once", (conns[0].sent, conns[1].sent), ([], [goto[0]]))
    conns[1]._write_queue.popleft()  # PA
    line.pim_line(1, "PA", "")
    check("accepted but still transmitting: the other waits", conns[0].sent, [])
    line.pim_line(1, "PN", "")
    check("done transmitting (PN): next one", conns[0].sent, [goto[1]])
    conns[0]._write_queue.popleft()
    line.pim_line(0, "PA", "")
    line.send(1, PimCommand.TX_UPB_MSG, query, bytearray([1, 14]), retries=0)
    now[0] += TX_DONE_TIMEOUT
    line.pump()
    check("no PK/PN: line free after a while", conns[1].sent[-1], query)
    check("retries set", conns[1]._write_queue[-1].retry_count, 0)
    line.pim_line(1, "PN", "")
    line.send(0, PimCommand.TX_UPB_MSG, goto[2], None)
    check("query awaiting its reply holds the line", conns[0].sent, [goto[1]])
    conns[1]._write_queue.popleft()  # the reply arrived: first of two copies
    line.pim_line(1, "PU", with_checksum("0804" + "01FF0E8628"))  # 14 at 40%, copy 1 of 2
    check("copies still to come hold the line", conns[0].sent, [goto[1]])
    now[0] += COPY_TIME
    line.pump()
    check("... then it's free", conns[0].sent, [goto[1], goto[2]])
    conns[0]._write_queue.clear()
    line.pim_line(0, "PK", "")
    check("idle", line.idle(), True)
    line.send(0, PimCommand.READ_PIM_REGISTERS, "F90700", None)
    check("register reads don't wait for the line", conns[0].sent[-1], "F90700")
    conns[0]._write_queue.clear()
    conns[1].connected = False
    line.send(1, PimCommand.TX_UPB_MSG, goto[0], None)
    check("message for a disconnected PIM dropped", (line.queue, conns[1].sent[-1]), (deque(), query))


def with_checksum(hex_msg: str) -> str:
    return f"{hex_msg}{-sum(bytes.fromhex(hex_msg)) & 0xFF:02X}"


def main() -> int:
    now = [0.0]
    dup = Duplicates(clock=lambda: now[0])

    check("first hearing", dup.heard(0, press(1)), True)
    now[0] += 0.3
    check("other PIM's copy (another sequence number)", dup.heard(1, press(1, seq=1)), False)
    check("same PIM again: upb-lib's call", dup.heard(0, press(1)), True)
    check("another link", dup.heard(1, press(2)), True)
    now[0] += 1.5
    check("other PIM, later: a new press", dup.heard(1, press(1)), True)

    # Our own message, sent from PIM A (index 0), heard by PIM B
    ours = decode(MessageEncode(2).activate_link(LinkAddr(1, 20)))[1]
    dup.sent(0, ours)
    now[0] += 1.2
    check("other PIM hears what one sent", dup.heard(1, ours._replace(tx_seq=1)), False)
    check("... still dropped", dup.heard(1, ours), False)
    now[0] += 2.0
    check("long after: someone else's", dup.heard(1, ours), True)

    # A PIM hears the other PIM's message only from the other one
    dup.sent(1, ours)
    check("sender hearing it (another controller)", dup.heard(1, ours), True)

    check("expired entries dropped", len(dup.recent) <= 3, True)

    # A message sent once stays on its phase: never the other PIM's copy
    once = decode(MessageEncode(1).activate_link(LinkAddr(1, 21)))[1]
    dup.sent(0, once)
    check("own single transmission not taken for an echo", dup.heard(1, once), True)
    check("single transmission heard again: upb-lib's call", dup.heard(0, press(3, tx=1)), True)
    check("... on either PIM", dup.heard(1, press(3, tx=1)), True)

    test_line(now)

    print(f"\n{FAILURES} failure(s)")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
