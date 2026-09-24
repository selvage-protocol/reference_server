"""The guard around the command line each packaging shape gives `selvaged`.

A command line this server refuses is a deployment that never starts. The box's
copy of `compose.yaml` is installed from the file tracked here, so `docker
compose up` with a refused line is a container that exits 2 and restart-loops
under `restart: unless-stopped`, and a front that upstreams to it is down with
it. The defect this file was written for was exactly that and it reached
production: `--outbound-queue-bytes 8388608` was below the floor, which is
`net::MAX_FRAME_BYTES` plus the envelope headroom the queue needs, and the value
had been argued from the frame bound in prose rather than run.

The authority on what the binary accepts is the binary. This file restates no
rule and no number: it reads the `command:` each tracked shape declares, hands
it to `selvaged`, and fails if the server does not come up.

Both compose shapes are read, because the rule is one binary's and both files
are deployed: `packaging/prod/compose.yaml` (the public demo) and
`packaging/pi/compose.yaml` (the Pi demo, being retired, whose tracked shape
still has to run). The shapes are listed rather than globbed, and a shape that
declares no `command:` fails instead of being skipped, so one cannot drop out of
this guard without a reader seeing it.

The one flag replaced is the bind address: `--listen` is the container's own
boundary rather than a limit, and a guard that took `0.0.0.0:8080` would fight
whatever is on the host. It is appended, and the last `--listen` is the one the
server takes. Every flag that carries a limit is the shape's own.

Run it directly, with a `selvaged` on `PATH` or one built into `target/`:

    python3 packaging/prod/test_compose_args.py

or as the flake check the workflows run:
`nix build .#checks.<system>.packaging-args`.
"""

import json
import os
import re
import select
import shutil
import subprocess
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent

# The tracked shapes that hand `selvaged` a command line, in the order the tests
# below name them, and the flag the defect that wrote this guard was carried on.
SHAPES = (HERE / "compose.yaml", HERE.parent / "pi" / "compose.yaml")
QUEUE_FLAG = "--outbound-queue-bytes"

# The banner `selvaged` prints once it is bound and serving. The address is a free
# loopback port rather than the shape's own, for the reason the docstring gives.
SERVING = "selvaged listening on"
BIND = "127.0.0.1:0"

# A process that either refuses the command line and exits on the spot or binds
# and stays up is not waited on for long; the bound is here so that a hang is
# reported as one instead of stalling the check.
STARTUP_TIMEOUT = 20.0
STOP_TIMEOUT = 10.0

_SERVICE = re.compile(r"^  ([a-z][a-z0-9_-]*):$")


def compose_body(text: str) -> str:
    """The file's directives, with its comment lines dropped.

    Every read below is a scan, and a scan that walks the prose too reports what
    the prose says rather than what the file declares.
    """
    return "\n".join(
        line for line in text.split("\n") if not line.lstrip().startswith("#")
    )


def service_blocks(text: str) -> dict:
    """Each service the file defines, as the lines that follow its name."""
    blocks = {}
    in_services = False
    name = None
    for line in compose_body(text).split("\n"):
        if line.startswith("services:"):
            in_services = True
            continue
        if not in_services:
            continue
        if line and not line.startswith(" "):
            break
        if match := _SERVICE.fullmatch(line):
            name = match.group(1)
            blocks[name] = []
        elif name is not None:
            blocks[name].append(line)
    return blocks


def scalar(token: str) -> str:
    """One YAML scalar, as the argument the server would see.

    A value the shapes quote (`"30000"`) is JSON and parsed as one; a plain
    token is taken as it stands.
    """
    if token.startswith('"'):
        return json.loads(token)
    if token.startswith("'") and token.endswith("'"):
        return token[1:-1]
    return token


def command_block(block: list) -> list | None:
    """The `command:` of one service, as argv, in either YAML spelling."""
    for index, line in enumerate(block):
        stripped = line.strip()
        if not stripped.startswith("command:"):
            continue
        rest = stripped[len("command:") :].strip()
        if rest.startswith("["):
            # A flow sequence. The shapes write it JSON-compatible, which is what
            # `json.loads` requires; anything else is refused here rather than
            # silently read as one argument.
            return json.loads(rest)
        argv = []
        for following in block[index + 1 :]:
            token = following.strip()
            if not token.startswith("- "):
                break
            argv.append(scalar(token[2:].strip()))
        return argv
    return None


def selvaged_command(path: Path) -> list | None:
    """The command line `path` gives its `selvaged` service."""
    blocks = service_blocks(path.read_text(encoding="utf-8"))
    if "selvaged" not in blocks:
        return None
    return command_block(blocks["selvaged"])


def binary() -> str:
    """The `selvaged` to run: `SELVAGED`, then `PATH`, then a checkout's build."""
    for candidate in (
        os.environ.get("SELVAGED"),
        shutil.which("selvaged"),
        str(REPO / "target" / "debug" / "selvaged"),
        str(REPO / "target" / "release" / "selvaged"),
    ):
        if candidate and Path(candidate).is_file():
            return candidate
    raise AssertionError(
        "no `selvaged` to run: build one (`cargo build -p selvaged`) or run this "
        "as the flake check (`nix build .#checks.<system>.packaging-args`), which "
        "puts the built binary on `PATH`"
    )


def serve(command: list) -> tuple:
    """Runs one shape's command line and reports what the server did.

    Returns `(serving, status, stdout, stderr)`: `serving` is true when the
    banner was printed, `status` the exit status once the process stopped, and
    the two streams as they were read.
    """
    proc = subprocess.Popen(
        [binary(), *command, "--listen", BIND],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        return observe(proc)
    finally:
        stop(proc)


def observe(proc) -> tuple:
    """Waits for the process to print its banner or to stop, whichever it does.

    Neither is a sleep in front of an assertion: a refused command line exits
    while `select` is still returning, and an accepted one prints the banner.
    The deadline only decides when a process that does neither is reported as
    the hang it is.
    """
    deadline = time.monotonic() + STARTUP_TIMEOUT
    out, err = "", ""
    watched = [proc.stdout, proc.stderr]
    while watched:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False, proc.poll(), out, err
        for stream in select.select(watched, [], [], remaining)[0]:
            line = stream.readline()
            if not line:
                watched.remove(stream)
                continue
            if stream is proc.stdout:
                out += line
                if SERVING in line:
                    return True, None, out, err
            else:
                err += line
    # Both streams are at EOF, so the process is on its way out; the status is
    # what the guard reports it as.
    try:
        proc.wait(timeout=STOP_TIMEOUT)
    except subprocess.TimeoutExpired:
        pass
    return False, proc.poll(), out, err


def stop(proc) -> None:
    """Ends the serving process, and leaves nothing behind if it does not go."""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=STOP_TIMEOUT)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=STOP_TIMEOUT)
    for stream in (proc.stdout, proc.stderr):
        stream.close()


class ComposeCommandTest(unittest.TestCase):
    """The command line each shape gives the server is one the server accepts."""

    def test_the_scan_reads_a_command_out_of_every_shape_it_guards(self):
        """A scan that matches nothing reports a clean tree, so say what it read."""
        for path in SHAPES:
            with self.subTest(shape=path.name):
                self.assertTrue(path.is_file(), f"{path} is not there to guard")
                command = selvaged_command(path)
                self.assertTrue(
                    command,
                    f"no `command:` was read out of {path}'s `selvaged` service, "
                    "so this shape is not being guarded",
                )

    def test_a_guarded_shape_still_names_the_flag_this_guard_is_for(self):
        """The queue is the flag the defect was on; dropping it is a decision.

        A shape that stops naming it takes the server's own default, which is
        above the floor, so nothing here is broken by that — but a guard whose
        subject has quietly gone is a guard that reports clean. Whoever removes
        it removes this test with it and says so.
        """
        naming = [
            path.name
            for path in SHAPES
            if QUEUE_FLAG in (selvaged_command(path) or [])
        ]
        self.assertTrue(
            naming,
            f"none of {[path.name for path in SHAPES]} names {QUEUE_FLAG} any "
            "more, which is the flag this guard exists for",
        )

    def test_the_public_demo_command_line_starts_the_server(self):
        self.assert_starts(SHAPES[0])

    def test_the_pi_command_line_starts_the_server(self):
        self.assert_starts(SHAPES[1])

    def assert_starts(self, path: Path) -> None:
        """`path`'s command line, run: the server comes up, or this says why not."""
        command = selvaged_command(path)
        self.assertTrue(command, f"no command to run in {path}")
        serving, status, out, err = serve(command)
        if serving:
            return
        how = (
            f"neither started the server nor stopped it within {STARTUP_TIMEOUT}s"
            if status is None
            else f"did not start the server: exit {status}"
        )
        self.fail(
            f"{path.name}'s command line {how}, stdout {out!r}, stderr {err!r}\n"
            f"the command line was: {' '.join(command)}"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
