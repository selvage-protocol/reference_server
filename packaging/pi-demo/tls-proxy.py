"""TLS termination for the Selvage demo server (selvaged).

Stdlib only. Listens on the Pi's tailnet IP (never loopback, never 0.0.0.0)
with the Tailscale-provisioned MagicDNS certificate and relays every path,
byte for byte, to the plaintext selvaged on its tailnet bind :8080:

  https://<pi>:8444/<anything>  ->  http://100.64.0.3:8080/<anything>

The front is transparent on purpose. selvaged serves the browser page itself
(`--serve-page`), so the page, `/meta` and `/session` are one origin: the
page's meta read and its WebSocket dial need no CORS headers and no second
origin, and there is nothing here to route, rewrite or answer. This proxy
reads no request at all — it opens the backend connection and pumps both
directions — so the wire each client sees is selvaged's own.

Run under the selvage-tls-proxy.service user unit, which appends stdout to
tls-proxy.log. That file — not the journal — is the record, same as the
selvaged unit (this Pi's journal keeps no user-unit output).

Idle: a quiet socket is not a dead one. The server pings every 30 s and a
browser only answers, so an idle read keeps waiting; only EOF, an I/O error,
or 15 minutes of silence in *both* directions (one shared clock) ends a
relay.
"""

import asyncio
import os
import ssl
import time
from pathlib import Path

BIND = os.environ.get("TLS_PROXY_BIND", "100.64.0.3")
PORT = int(os.environ.get("TLS_PROXY_PORT", "8444"))
BACKEND_HOST = os.environ.get("TLS_PROXY_BACKEND_HOST", "100.64.0.3")
BACKEND_PORT = int(os.environ.get("TLS_PROXY_BACKEND_PORT", "8080"))
CERT = Path(os.environ.get("TLS_PROXY_CERT", "/home/pi/selvage-web/certs/tailnet.crt"))
KEY = Path(os.environ.get("TLS_PROXY_KEY", "/home/pi/selvage-web/certs/tailnet.key"))

CONNECT_TIMEOUT_S = 15.0
# How often an idle relay is re-checked, and how long a relay with no byte in
# either direction is allowed to live. Not a per-direction deadline: a
# WebSocket session is legitimately silent for minutes.
RELAY_POLL_S = 30.0
IDLE_LIMIT_S = 900.0
BUFFER = 64 * 1024

UNREACHABLE_BODY = b"session server unreachable\n"
BACKEND_UNREACHABLE = (
    b"HTTP/1.1 502 Bad Gateway\r\ncontent-type: text/plain; charset=utf-8\r\n"
    + b"content-length: %d\r\nconnection: close\r\n\r\n" % len(UNREACHABLE_BODY)
    + UNREACHABLE_BODY
)


async def pump(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    last_seen: list[float],
) -> None:
    """Copy one direction until EOF, an I/O error, or a genuinely dead relay.

    Both directions share one clock: a read that times out is not an ending,
    and the relay is abandoned only when neither direction has carried a byte
    for `IDLE_LIMIT_S`.
    """
    while True:
        try:
            chunk = await asyncio.wait_for(reader.read(BUFFER), RELAY_POLL_S)
        except TimeoutError:
            idle = time.monotonic() - last_seen[0]
            if idle < IDLE_LIMIT_S:
                continue
            print(
                f"tls-proxy: relay idle {idle:.0f}s in both directions, closing",
                flush=True,
            )
            return
        except (ConnectionError, BrokenPipeError, ssl.SSLError):
            return
        if not chunk:
            return
        last_seen[0] = time.monotonic()
        try:
            writer.write(chunk)
            await writer.drain()
        except (ConnectionError, BrokenPipeError, ssl.SSLError):
            return


async def handle(
    client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
) -> None:
    backend_writer: asyncio.StreamWriter | None = None
    try:
        try:
            backend_reader, backend_writer = await asyncio.wait_for(
                asyncio.open_connection(BACKEND_HOST, BACKEND_PORT),
                CONNECT_TIMEOUT_S,
            )
        except (OSError, TimeoutError) as error:
            print(f"tls-proxy: backend unreachable: {error!r}", flush=True)
            client_writer.write(BACKEND_UNREACHABLE)
            await client_writer.drain()
            return
        last_seen = [time.monotonic()]
        pumps = [
            asyncio.ensure_future(pump(client_reader, backend_writer, last_seen)),
            asyncio.ensure_future(pump(backend_reader, client_writer, last_seen)),
        ]
        try:
            # Either direction ending ends the relay: a half-closed connection
            # has nothing left to carry.
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in pumps:
                task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
    except (ConnectionError, BrokenPipeError):
        pass
    except Exception as error:  # one line in the log, never a dropped traceback
        print(f"tls-proxy: request failed: {error!r}", flush=True)
    finally:
        for stream in (backend_writer, client_writer):
            if stream is not None:
                try:
                    stream.close()
                except Exception:
                    pass


async def main() -> None:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=str(CERT), keyfile=str(KEY))
    server = await asyncio.start_server(handle, BIND, PORT, ssl=ctx)
    print(
        f"tls-proxy listening on https://{BIND}:{PORT} -> "
        f"{BACKEND_HOST}:{BACKEND_PORT} (every path: page, /meta, /session)",
        flush=True,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
