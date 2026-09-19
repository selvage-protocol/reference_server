"""The TLS front's idle logic, without the Pi and without a certificate.

`tls-proxy.py` once treated a quiet half of a tunnel as a dead one: `pump()` returned
on a `RELAY_POLL_S` timeout while `handle()` kept awaiting the *other* pump, so the
sockets stayed open and nothing ever read that direction again — every later frame sat
unforwarded for the rest of the session (the runbook's 2026-09-19 entry). A WebSocket
peer is legitimately silent for minutes, so this is the property the front exists to
get right and the one no test covered.

What is exercised is the real `pump()` and `handle()` from the tracked proxy, and the
real thing on the other side of one direction: a plaintext stub stands in for
`selvaged` on loopback, and only the *client* half is faked. The two clocks are scaled
down — a fifteen-minute relay cannot be held open in a test — so the numbers themselves
are pinned by `test_the_poll_is_far_shorter_than_the_limit` and the behaviour by the
tests around it. Every wait is a deadline that reports what it saw, never a sleep
followed by a hope.

Run it directly:

    python3 packaging/pi-demo/test_tls_proxy.py

or as the flake check the workflows run: `nix build .#checks.<system>.tls-proxy`.
"""

import asyncio
import importlib.util
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROXY_PATH = HERE / "tls-proxy.py"

# The clocks, scaled: the poll is what a quiet read waits, the limit is how long a relay
# with no byte in *either* direction is allowed to live. The proxy reads both from its
# own module, so a test replaces them there.
POLL = 0.05
LIMIT = 0.6
# Four polls of quiet: longer than any single read timeout, far shorter than the limit.
# Under the defect this is the window in which the relay ended and stopped reading.
QUIET = 4 * POLL
# Generous, and a real deadline: a wait that expires reports the state it saw.
DEADLINE = 5.0


def load_proxy():
    """The tracked `tls-proxy.py` as a module, hyphen and all.

    Importing it runs no listener: `main()` is behind `__main__`, and the module body
    only reads environment variables.
    """
    spec = importlib.util.spec_from_file_location("tls_proxy_under_test", PROXY_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {PROXY_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeReader:
    """One half of a tunnel as the proxy sees it: data when the test feeds it, silence
    otherwise. `read()` blocks on an empty queue, which is exactly the quiet the old
    `pump()` read as an ending."""

    def __init__(self):
        self._queue = asyncio.Queue()
        self.reads = 0

    def feed(self, data):
        self._queue.put_nowait(data)

    async def read(self, size):
        self.reads += 1
        return await self._queue.get()


class FakeWriter:
    """The other half, recording what was written to it and whether it was closed."""

    def __init__(self):
        self.received = bytearray()
        self.closed = False

    def write(self, data):
        self.received += data

    async def drain(self):
        pass

    def close(self):
        self.closed = True


async def stub_backend(send_first, quiet_for):
    """A plaintext stub for `selvaged` on loopback.

    It greets with `send_first`, stays quiet for `quiet_for`, and then sends the frame
    that must still be forwarded. Returns the server and the port it listens on.
    """

    async def handle(reader, writer):
        try:
            writer.write(send_first)
            await writer.drain()
            await asyncio.sleep(quiet_for)
            writer.write(b"later\n")
            await writer.drain()
            while await reader.read(1024):
                pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port


async def wait_for_bytes(writer, wanted, deadline, what):
    """Waits for `wanted` on a writer, and on expiry says what was there instead."""
    try:
        async with asyncio.timeout(deadline):
            while wanted not in writer.received:
                await asyncio.sleep(0.005)
    except TimeoutError:
        raise AssertionError(
            f"{what}: expected {wanted!r} within {deadline}s, saw "
            f"{bytes(writer.received)!r} (relay closed: {writer.closed})"
        ) from None


class QuietTransportTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.proxy = load_proxy()
        self.limits = (self.proxy.RELAY_POLL_S, self.proxy.IDLE_LIMIT_S)
        self.proxy.RELAY_POLL_S = POLL
        self.proxy.IDLE_LIMIT_S = LIMIT
        self.feeding = False
        self.feeder = None

    async def asyncTearDown(self):
        self.proxy.RELAY_POLL_S, self.proxy.IDLE_LIMIT_S = self.limits

    async def relay_to(self, *, client_feeds=False):
        """Starts a stub backend and one relay through the proxy's own `handle()`.

        The client half is faked and the backend half is a real socket, so the direction
        under test is read by the real `pump()`. Returns the relay task, the fake client
        reader and writer, and the stub server.
        """
        server, port = await stub_backend(b"first\n", QUIET)
        self.addAsyncCleanup(self._close, server)
        self.proxy.BACKEND_HOST = "127.0.0.1"
        self.proxy.BACKEND_PORT = port
        client_reader = FakeReader()
        client_writer = FakeWriter()
        if client_feeds:
            self.feeding = True
            self.feeder = asyncio.ensure_future(self._feed(client_reader))
            self.addAsyncCleanup(self._stop_feeding)
        relay = asyncio.ensure_future(self.proxy.handle(client_reader, client_writer))
        self.addAsyncCleanup(self._cancel, relay)
        return relay, client_reader, client_writer, server

    async def _feed(self, reader):
        while self.feeding:
            reader.feed(b"ping\n")
            await asyncio.sleep(POLL / 2)

    async def _stop_feeding(self):
        self.feeding = False
        if self.feeder is not None:
            self.feeder.cancel()
            await asyncio.gather(self.feeder, return_exceptions=True)

    async def _cancel(self, relay):
        if not relay.done():
            relay.cancel()
        await asyncio.gather(relay, return_exceptions=True)

    async def _close(self, server):
        server.close()
        await server.wait_closed()

    def test_the_poll_is_far_shorter_than_the_limit(self):
        """The numbers the relay is built on, not the scaled test ones.

        A poll as long as the limit would make every idle check a coin toss, and the
        fifteen minutes is the window the front's docstring and the Pi's unit document.
        """
        self.assertEqual(self.limits, (30.0, 900.0))
        self.assertLess(self.limits[0], self.limits[1])

    async def test_a_quiet_half_is_not_a_dead_one(self):
        """The regression: a direction quiet for longer than a poll still forwards.

        The client half says nothing at all. The backend half greets, is quiet for four
        polls, and then speaks again — and that later frame has to arrive. Under the
        defect the backend's pump returned on the first timeout, `handle()` tore the
        tunnel down behind it, and this frame was never read by anyone.
        """
        relay, client_reader, client_writer, _server = await self.relay_to()
        await wait_for_bytes(
            client_writer,
            b"first\n",
            DEADLINE,
            "the greeting of a tunnel that has not been quiet yet",
        )
        await wait_for_bytes(
            client_writer,
            b"later\n",
            DEADLINE,
            f"a frame sent after {QUIET}s of quiet, with a {POLL}s poll",
        )
        self.assertFalse(
            client_writer.closed,
            "the tunnel is still open after a quiet shorter than the limit",
        )
        self.assertFalse(
            relay.done(),
            "the relay runs on rather than ending at the first quiet read",
        )

    async def test_one_direction_carrying_bytes_keeps_the_relay_open(self):
        """Both directions share one clock: the backend's long silence is not an ending.

        The client half sends a byte between polls while the backend half is quiet for
        longer than the limit. A per-direction deadline would end the backend's pump —
        the defect's own shape — and a clock that is not shared would end the relay.
        """
        relay, _client_reader, client_writer, _server = await self.relay_to(
            client_feeds=True
        )
        await wait_for_bytes(
            client_writer,
            b"first\n",
            DEADLINE,
            "the greeting before the backend goes quiet",
        )
        await asyncio.sleep(LIMIT * 1.5)
        self.assertFalse(
            relay.done(),
            "a byte in either direction is what the idle clock measures",
        )
        await wait_for_bytes(
            client_writer,
            b"later\n",
            DEADLINE,
            "the frame sent after the backend's own silence exceeded the limit",
        )

    async def test_silence_in_both_directions_ends_the_relay(self):
        """The other half of the rule: with no byte in either direction, it does end.

        The relay lives for the whole window — more than one poll — and then closes,
        which is what keeps a parked connection from being held open forever.
        """
        loop = asyncio.get_running_loop()
        started = loop.time()
        relay, _client_reader, client_writer, _server = await self.relay_to()
        await asyncio.wait_for(relay, DEADLINE)
        lived = loop.time() - started
        self.assertGreaterEqual(
            lived,
            POLL * 3,
            "the relay outlives a single poll: the limit, not the poll, is the deadline",
        )
        self.assertTrue(
            client_writer.closed,
            "the client's half is closed when the relay ends",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
