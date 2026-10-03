"""Per-destination UPB transmit counts, and the PIM each message goes out on.

Devices on the PIM's own phase hear a message sent once; the rest need it
repeated (tx_count 2). Sending once is about 0.6 s faster per query, since a
device answers with as many copies as it was sent. The bridge learns which
devices answer a single transmission each time it reads all devices; nothing
is kept across restarts, as devices or the PIM may have moved.

With two PIMs, each on its own phase, a device on the second PIM's phase
hears it send once. Each message goes out on the PIM that needs the fewest
transmissions to reach it; the first PIM on a tie.

A link command reaches every device in the link and every keypad that shows
the link on an LED, so it is sent as many times as the neediest of them.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from upb_lib.message import EMPTY_BYTEARRAY, MessageEncode


class TxCounts:
    """Learned transmit counts, per PIM."""

    def __init__(self, default: int, pims: int = 1) -> None:
        self.default = default  # for devices that need repeats, or not yet probed
        # per PIM: upb_id -> answers a single transmission from it
        self.single: list[dict[int, bool]] = [{} for _ in range(pims)]
        self.link_listeners: dict[int, set[int]] = {}  # link id -> upb ids
        self.online: Callable[[int], bool] = lambda pim: True  # set by the bridge

    def add_listeners(self, link_id: int, upb_ids: Iterable[int]) -> None:
        self.link_listeners.setdefault(link_id, set()).update(upb_ids)

    def count(self, pim: int, is_link: bool, upb_id: int) -> int:
        """Transmissions needed for a message from this PIM to reach its destination."""
        if not is_link:
            return 1 if self.single[pim].get(upb_id) else self.default
        listeners = self.link_listeners.get(upb_id)
        if not listeners:
            return self.default  # nothing known to listen: don't guess
        return max(self.count(pim, False, listener) for listener in listeners)

    def route(self, is_link: bool, upb_id: int) -> tuple[int, int]:
        """(PIM, transmit count): the connected PIM needing the fewest."""
        pims = [pim for pim in range(len(self.single)) if self.online(pim)] or [0]
        count, pim = min((self.count(pim, is_link, upb_id), pim) for pim in pims)
        return pim, count

    def device(self, upb_id: int) -> int:
        return self.route(False, upb_id)[1]

    def link(self, link_id: int) -> int:
        return self.route(True, link_id)[1]

    def for_addr(self, addr) -> int:
        return self.route(addr.is_link, addr.upb_id)[1]


class TxEncoder(MessageEncode):
    """upb-lib's encoder, with the transmit count picked per destination."""

    def __init__(self, tx_for: Callable[..., int]) -> None:
        super().__init__(1)
        self.tx_for = tx_for

    def _encode_message(self, ctl, addr, src_id, msg_code, data: bytearray = EMPTY_BYTEARRAY):
        if ctl == -1:  # upb-lib builds the control word from self.tx_count
            self.tx_count = self.tx_for(addr)
        return super()._encode_message(ctl, addr, src_id, msg_code, data)
