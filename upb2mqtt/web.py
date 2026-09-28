"""Device table page, served to Home Assistant's ingress.

  GET /                stats.html: a sortable table of every UPB device
  GET /stats.json      what the table shows (see Bridge.status)
  POST /refresh/<unit> read a device's state now (its Refresh state button)

Only Home Assistant's ingress proxy (and localhost, for testing) may connect.
The page uses relative URLs, since ingress serves it under a per-session path.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

LOG = logging.getLogger("upb2mqtt.web")

PAGE = Path(__file__).with_name("stats.html")
INGRESS_PEERS = {"172.30.32.2", "127.0.0.1", "::1"}
REQUEST_TIMEOUT = 10


def _response(status: str, content_type: str, body: bytes, head_only: bool = False) -> bytes:
    headers = (
        f"HTTP/1.1 {status}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Cache-Control: no-store\r\n"
        "Connection: close\r\n\r\n"
    )
    return headers.encode() + (b"" if head_only else body)


async def serve(port: int, status: Callable[[], dict[str, Any]],
                refresh: Callable[[int], bool]) -> None:
    """refresh(unit) queries a device; False if there's no such device or no PIM."""
    page = PAGE.read_bytes()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            peer = (writer.get_extra_info("peername") or ("",))[0].removeprefix("::ffff:")
            if peer not in INGRESS_PEERS:
                LOG.debug("Refused connection from %s", peer)
                return
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), REQUEST_TIMEOUT)
            method, target, _ = head.split(b"\r\n", 1)[0].decode("latin-1").split(" ", 2)
            path = target.split("?", 1)[0]
            head_only = method == "HEAD"
            unit = path.removeprefix("/refresh/") if path.startswith("/refresh/") else None
            if unit is not None:
                if method != "POST":
                    reply = _response("405 Method Not Allowed", "text/plain", b"POST only\n")
                elif unit.isdigit() and refresh(int(unit)):
                    reply = _response("202 Accepted", "text/plain", b"Queued\n")
                else:
                    reply = _response("404 Not Found", "text/plain", b"No such device, or no PIM\n")
            elif method not in ("GET", "HEAD"):
                reply = _response("405 Method Not Allowed", "text/plain", b"GET only\n")
            elif path in ("", "/"):
                reply = _response("200 OK", "text/html; charset=utf-8", page, head_only)
            elif path == "/stats.json":
                body = json.dumps(status()).encode()
                reply = _response("200 OK", "application/json", body, head_only)
            else:
                reply = _response("404 Not Found", "text/plain", b"Not found\n", head_only)
            writer.write(reply)
            await writer.drain()
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError,
                ConnectionError, ValueError):
            pass  # a bad or abandoned request: just close it
        except Exception:  # pylint: disable=broad-except
            LOG.exception("Error serving the device table")
        finally:
            writer.close()

    server = await asyncio.start_server(handle, port=port)
    LOG.info("Device table on port %d", port)
    async with server:
        await server.serve_forever()
