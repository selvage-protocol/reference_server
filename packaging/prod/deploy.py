#!/usr/bin/env python3
"""Point the public demo at published image tags, and bring it up to them.

Installed by hand as `/usr/local/sbin/selvage-deploy`, root-owned and mode 0755,
and the only command the `deployci` user may run through sudo
(`packaging/prod/deployci.sudoers`, whose line ends in `""` so that sudo permits
no argument at all rather than any). That confinement is why this takes **no
arguments**: an argument to a script that reaches `docker compose` is a root shell
with extra steps. The request arrives on stdin instead — one or both image
references — and every value it may carry is matched against a fixed pattern
before anything else happens. The argument refusal below is the second lock on
that door, not the first.

What a request can and cannot do, because that is the whole security argument:

* it names a tag of one of the two published `ghcr.io/selvage-protocol/` images
  and nothing else — no path, no flag, no shell word, no other repository;
* it may omit an image, which leaves that line of `.env` exactly as it is;
* it rewrites those lines of `/srv/selvage/.env` and nothing else. The compose
  file, `proxy/` and `tls/` are the owner's to edit by hand and are never written
  here, so a request cannot add a port, drop a capability or mount a file.

A run that fails leaves `.env` as it found it. The new file is staged as
`.env.deploy`, which `pull`, `up` and the convergence check read, and `.env` is
written only once the containers run what was asked for. All of it happens under
the lock `selvage-update.service` takes, so a timer tick cannot run compose in the
middle of a deploy, and the first tick after a failed one brings the box back to
what `.env` still names.

The residual, plainly: a deploy request is a deployment. Whoever can send one
chooses which published tag runs, ends every live room by replacing the server,
and can move the deployment to an older release. The content behind a tag is the
publisher's; following tags rather than digests is the owner's choice.

`README.md` beside this file owns the box's layout and its hand-install.
"""

import fcntl
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SRV = Path("/srv/selvage")
ENV_FILE = SRV / ".env"
ENV_PREV = SRV / ".env.prev"
ENV_STAGED = SRV / ".env.deploy"
# Shared with `selvage-update.service`, which names the same path.
LOCK = SRV / ".update.lock"
# A timer tick holds the lock for as long as its pull takes.
LOCK_WAIT_SECONDS = 600.0

# The two variables the compose file interpolates, and the image repository each
# may name, which is also the compose service it lands on.
PINS = {
    "SELVAGED_IMAGE": "selvaged",
    "SELVAGE_WEB_IMAGE": "selvage-web",
}
SERVICES = ("proxy", "selvaged", "selvage-web")

# A request is two hundred bytes; anything past this is not one, and the bound
# is here so a caller cannot make a root process read for as long as it likes.
MAX_REQUEST_BYTES = 4096

# The OCI tag grammar: a version, `latest`, or a commit-stamped `<version>-<sha>`.
_TAG = r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}"
IMAGE_PATTERN = {
    key: re.compile(rf"^ghcr\.io/selvage-protocol/{name}:{_TAG}$") for key, name in PINS.items()
}


class Refused(Exception):
    """The request is not one this box acts on, or the deploy did not converge."""


def parse_request(data: bytes) -> dict:
    """The request, or `Refused` naming what about it is not allowed.

    The grammar is deliberately narrow: `KEY=VALUE` lines, one of the `PINS`
    keys each, no blank line, no control character, no duplicate, and each value
    matched against the one fixed shape its key admits. Nothing here is a path, and
    nothing here reaches a shell.
    """
    if len(data) > MAX_REQUEST_BYTES:
        raise Refused(f"request is longer than {MAX_REQUEST_BYTES} bytes")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise Refused(f"request is not UTF-8: {error}") from error

    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()

    request = {}
    for line in lines:
        if not line:
            raise Refused("request has a blank line")
        if any(ord(character) < 0x20 for character in line):
            raise Refused("request has a control character")
        key, separator, value = line.partition("=")
        if not separator:
            raise Refused(f"request line is not KEY=VALUE: {line!r}")
        if key not in IMAGE_PATTERN:
            raise Refused(f"{key!r} is not a key this box acts on")
        if key in request:
            raise Refused(f"{key} appears twice")
        if not IMAGE_PATTERN[key].match(value):
            raise Refused(f"{key} is not a ghcr.io/selvage-protocol/{PINS[key]}:<tag> reference: {value!r}")
        request[key] = value

    if not request:
        raise Refused("no image named: send SELVAGED_IMAGE, SELVAGE_WEB_IMAGE or both")
    return request


def rewrite_env(text: str, requested: dict) -> str:
    """`text` with each requested key's line replaced, and every other line kept.

    A key the file does not have is appended. Comments, blank lines and anything
    the owner added by hand come through byte for byte.
    """
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    seen = set()
    for index, line in enumerate(lines):
        key = line.strip().partition("=")[0].strip()
        if key in requested:
            lines[index] = f"{key}={requested[key]}"
            seen.add(key)
    lines.extend(f"{key}={value}" for key, value in requested.items() if key not in seen)
    return "".join(f"{line}\n" for line in lines)


def read_env() -> str:
    """The current `.env`, never through a link."""
    try:
        descriptor = os.open(ENV_FILE, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return ""
    with os.fdopen(descriptor, encoding="utf-8") as handle:
        try:
            return handle.read()
        except UnicodeDecodeError as error:
            raise Refused(f"{ENV_FILE} is not UTF-8, so it is not rewritten: {error}") from error


def atomic_write(path: Path, text: str) -> None:
    """Replace `path` with `text`, mode 0600, owned by whoever owns the directory.

    The file is created fresh beside its target and renamed over it, so an
    existing link at `path` is replaced rather than followed. The directory's
    owner is `selvage`, whose compose and timer read these files.
    """
    owner = os.stat(path.parent)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o600)
            os.fchown(handle.fileno(), owner.st_uid, owner.st_gid)
            handle.write(text)
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def record(request: dict) -> bool:
    """Write the requested lines into `.env`, keeping the file it replaces as `.env.prev`.

    Called only once the containers have converged. `.env` is read again here
    rather than reused from the start of the deploy, because hand commands do not
    take the lock: an edit made to it while `pull` and `up` ran survives, and
    `.env.prev` is the file actually replaced. A no-op leaves both files alone, so
    `.env.prev` keeps naming the last real previous state.
    """
    current = read_env()
    text = rewrite_env(current, request)
    if text == current:
        return False
    atomic_write(ENV_PREV, current)
    atomic_write(ENV_FILE, text)
    return True


def take_lock() -> int:
    """Hold the lock the update timer takes, waiting out a tick in progress."""
    descriptor = os.open(LOCK, os.O_RDONLY | os.O_CREAT | os.O_NOFOLLOW, 0o644)
    deadline = time.monotonic() + LOCK_WAIT_SECONDS
    while True:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return descriptor
        except BlockingIOError:
            if time.monotonic() >= deadline:
                os.close(descriptor)
                raise Refused(
                    f"{LOCK} is still held after {LOCK_WAIT_SECONDS:.0f}s; "
                    f"`systemctl status selvage-update.service` shows what holds it"
                ) from None
            time.sleep(1)


def compose_argv(env_file: Path, *arguments: str) -> list[str]:
    """The project's command line, run in `SRV` as the timer and every hand command are.

    No `-f`: Compose finds `compose.yaml` in the working directory, and a
    `compose.override.yaml` beside it too, so a deploy runs the same shape the
    next tick does. `--env-file` replaces `.env`, which is how the staged file is
    read instead.
    """
    return ["docker", "compose", "--env-file", str(env_file), *arguments]


def compose(env_file: Path, *arguments: str) -> subprocess.CompletedProcess:
    return subprocess.run(compose_argv(env_file, *arguments), cwd=SRV, text=True, check=False)


def run(argv: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(argv, cwd=SRV, text=True, capture_output=True, check=False)


def image_id(reference: str) -> str:
    return run(["docker", "image", "inspect", "--format", "{{.Id}}", reference]).stdout.strip()


def repo_digest(reference: str) -> str:
    """What the tag named when this box pulled it: the record of what was deployed."""
    listed = run(
        ["docker", "image", "inspect", "--format", '{{join .RepoDigests " "}}', reference]
    )
    return listed.stdout.strip() or "-"


def service_state(service: str, env_file: Path) -> tuple[str, str]:
    """The image and status of the container this service runs, or `("", "")`."""
    listed = run(compose_argv(env_file, "ps", "-q", "--all", service))
    container = listed.stdout.strip().split("\n")[0].strip()
    if not container:
        return "", ""
    inspected = run(["docker", "inspect", "--format", "{{.Image}}|{{.State.Status}}", container])
    if inspected.returncode != 0:
        return "", ""
    image, _, status = inspected.stdout.strip().partition("|")
    return image, status


def assert_converged(request: dict, env_file: Path) -> None:
    """Every service runs, and each requested one runs the image its tag pulled."""
    for service in SERVICES:
        _image, status = service_state(service, env_file)
        if status != "running":
            raise Refused(f"{service} is {status or 'missing'} after `up -d`, not running")
    for key, reference in request.items():
        pulled = image_id(reference)
        if not pulled:
            raise Refused(f"{reference} is not in the local image store after `pull`")
        image, _status = service_state(PINS[key], env_file)
        if image != pulled:
            raise Refused(f"{PINS[key]} runs {image} but {reference} is {pulled}")


def deploy(request: dict) -> None:
    named = [PINS[key] for key in PINS if key in request]

    print("=== request ===")
    for key in PINS:
        if key in request:
            print(f"{key}={request[key]}")

    atomic_write(ENV_STAGED, rewrite_env(read_env(), request))

    print(f"=== pull {' '.join(named)} ===")
    if compose(ENV_STAGED, "pull", *named).returncode != 0:
        raise Refused(f"`docker compose pull {' '.join(named)}` failed; {ENV_FILE} was not written")

    print("=== up -d ===")
    if compose(ENV_STAGED, "up", "-d").returncode != 0:
        raise Refused(
            f"`docker compose up -d` failed; {ENV_FILE} was not written, and the next "
            f"update tick goes back to what it names"
        )

    assert_converged(request, ENV_STAGED)

    if record(request):
        print(f"wrote {ENV_FILE}; the file it replaced is {ENV_PREV}")
    else:
        print(f"{ENV_FILE} already named these")
    ENV_STAGED.unlink(missing_ok=True)

    print("=== deployed ===")
    for key in PINS:
        if key in request:
            print(f"{request[key]} = {repo_digest(request[key])}")
    compose(ENV_FILE, "ps")


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("selvage-deploy takes no arguments; the request arrives on stdin", file=sys.stderr)
        return 2
    if os.geteuid() != 0:
        print("selvage-deploy must run as root", file=sys.stderr)
        return 2

    try:
        request = parse_request(sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1))
        lock = take_lock()
        try:
            deploy(request)
        finally:
            os.close(lock)
        return 0
    except Refused as refusal:
        print(f"refused: {refusal}", file=sys.stderr)
        return 1
    except OSError as error:
        print(f"refused: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
