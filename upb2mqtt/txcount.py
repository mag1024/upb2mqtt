"""Per-destination UPB transmit counts.

Devices on the PIM's own phase hear a message sent once; the rest need it
repeated (tx_count 2). Sending once is about 0.6 s faster per query, since a
device answers with as many copies as it was sent. The bridge learns which
devices answer a single transmission each time it reads all devices; nothing
is kept across restarts, as devices or the PIM may have moved.

A link command reaches every device in the link and every keypad that shows
the link on an LED, so it is sent as many times as the neediest of them.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from upb_lib.message import EMPTY_BYTEARRAY, MessageEncode


class TxCounts:
    """Learned transmit counts."""

    def __init__(self, default: int) -> None:
        self.default = default  # for devices that need repeats, or not yet probed
        self.single: dict[int, bool] = {}  # upb_id -> answers a single transmission
        self.link_listeners: dict[int, set[int]] = {}  # link id -> upb ids

    def add_listeners(self, link_id: int, upb_ids: Iterable[int]) -> None:
        self.link_listeners.setdefault(link_id, set()).update(upb_ids)

    def device(self, upb_id: int) -> int:
        return 1 if self.single.get(upb_id) else self.default

    def link(self, link_id: int) -> int:
        listeners = self.link_listeners.get(link_id)
        if not listeners:
            return self.default  # nothing known to listen: don't guess
        return max(self.device(upb_id) for upb_id in listeners)

    def for_addr(self, addr) -> int:
        return self.link(addr.upb_id) if addr.is_link else self.device(addr.upb_id)


class TxEncoder(MessageEncode):
    """upb-lib's encoder, with the transmit count picked per destination."""

    def __init__(self, tx_for: Callable[..., int]) -> None:
        super().__init__(1)
        self.tx_for = tx_for

    def _encode_message(self, ctl, addr, src_id, msg_code, data: bytearray = EMPTY_BYTEARRAY):
        if ctl == -1:  # upb-lib builds the control word from self.tx_count
            self.tx_count = self.tx_for(addr)
        return super()._encode_message(ctl, addr, src_id, msg_code, data)
