#!/usr/bin/env python3
"""Deploy the public demo to two digest-pinned images.

Installed by hand as `/usr/local/sbin/selvage-deploy`, root-owned and mode 0755,
and the only command the `deployci` user may run through sudo
(`packaging/prod/deployci.sudoers`). That confinement is why this takes **no
arguments**: a sudoers rule that permits an argument is a wildcard, and a
wildcard around a script that reaches `docker compose` is a root shell with extra
steps. The request arrives on stdin instead — `COMPOSE_SHA256` plus one or both
image references — and every value it may carry is matched against a fixed
pattern before anything else happens.

What a request can and cannot do, because that is the whole security argument:

* it names published `ghcr.io/selvage-protocol/` images **by digest** and nothing
  else — no path, no flag, no shell word, no other repository;
* it may omit an image, which leaves that service exactly as it is;
* it cannot edit the shape: the compose file is *verified* against the hash the
  caller names and a mismatch stops the deploy before a container is touched. So
  a caller cannot add a port, drop a capability, mount a certificate it should
  not have or raise a memory limit.

The residual, plainly: a deploy request is a deployment. Whoever can send one
chooses which of the published releases runs, ends every live room by replacing
the server, and can downgrade the deployment to an older published release. That
influence is the point of the thing and is the largest thing it can do.

`README.md` beside this file owns the box's shape and its hand-install.
"""

import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path

ETC = Path("/etc/selvage")
COMPOSE = ETC / "compose.yaml"
ENV_FILE = ETC / ".env"
ENV_PREV = ETC / ".env.prev"

# The two variables the compose file interpolates: which image repository each
# may name, and which compose service it lands on.
PINS = {
    "SELVAGED_IMAGE": ("selvaged", "selvaged"),
    "SELVAGE_WEB_IMAGE": ("selvage-web", "selvage-web"),
}
SERVICES = ("proxy", "selvaged", "selvage-web")

# A request is two hundred bytes; anything past this is not one, and the bound
# is here so a caller cannot make a root process read for as long as it likes.
MAX_REQUEST_BYTES = 4096

_HEX64 = "[0-9a-f]{64}"
IMAGE_PATTERN = {
    key: re.compile(rf"^ghcr\.io/selvage-protocol/{repo}@sha256:{_HEX64}$")
    for key, (repo, _service) in PINS.items()
}
SHA256_PATTERN = re.compile(rf"^{_HEX64}$")
KEY_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")


class Refused(Exception):
    """The request is not one this box acts on. Nothing has been changed."""


def parse_request(data: bytes) -> dict:
    """The request, or `Refused` naming what about it is not allowed.

    The grammar is deliberately narrow: `KEY=VALUE` lines, upper-case keys, no
    blank line, no control character, no duplicate, and each value matched
    against the one fixed shape its key admits. Nothing here is a path, and
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
        if not KEY_PATTERN.match(key):
            raise Refused(f"request key is not an upper-case word: {key!r}")
        if key in request:
            raise Refused(f"{key} appears twice")
        if key == "COMPOSE_SHA256":
            if not SHA256_PATTERN.match(value):
                raise Refused(f"COMPOSE_SHA256 is not a lowercase sha256 digest: {value!r}")
        elif key in IMAGE_PATTERN:
            if not IMAGE_PATTERN[key].match(value):
                raise Refused(
                    f"{key} is not a digest-pinned "
                    f"ghcr.io/selvage-protocol/{PINS[key][0]} reference: {value!r}"
                )
        else:
            raise Refused(f"{key} is not a key this box acts on")
        request[key] = value

    if "COMPOSE_SHA256" not in request:
        raise Refused(
            "COMPOSE_SHA256 is required: the deploy verifies the shape rather than writing it"
        )
    if not any(key in request for key in PINS):
        raise Refused("no image named: send SELVAGED_IMAGE, SELVAGE_WEB_IMAGE or both")
    return request


def read_env(text: str) -> dict:
    """The pins in an `.env` file. Anything else in the file is not this box's business."""
    pins = {}
    for line in text.split("\n"):
        key, separator, value = line.strip().partition("=")
        if separator and key in PINS:
            pins[key] = value.strip()
    return pins


def resolve_pins(current: dict, requested: dict) -> dict:
    """The two references the next `up` runs: what was asked for, else what is there."""
    pins = {}
    for key in PINS:
        value = requested.get(key, current.get(key))
        if value is None:
            raise Refused(f"{key} is in neither the request nor {ENV_FILE}")
        if not IMAGE_PATTERN[key].match(value):
            raise Refused(
                f"{key} in {ENV_FILE} is not a reference this box deploys, and a request that "
                f"leaves it alone cannot fix that: {value!r}"
            )
        pins[key] = value
    return pins


def render_env(pins: dict) -> str:
    return (
        "# Generated by /usr/local/sbin/selvage-deploy: the two published images the next\n"
        "# `docker compose up -d` runs. Digests, not tags, because a tag is a name the\n"
        "# publisher can repoint. The file this replaced is .env.prev; the explained copy is\n"
        "# .env.example, and README.md beside it owns the shape.\n"
        + "".join(f"{key}={pins[key]}\n" for key in PINS)
    )


def compose_environment() -> dict:
    """`docker compose`'s environment, and the one variable that must be in it.

    Compose rebuilds `proxy` on every `up` — its `pull_policy` is `build`, which
    is what makes a change to `proxy/` reach the running container. With
    BuildKit's default attestations the resulting manifest, and so the image ID,
    is stamped per build even when the content is byte-identical, and compose
    recreates any container whose image ID moved. Without this, every deploy
    replaces the front and drops the WebSockets through it, a deploy that
    changed nothing included. The front's image is local only and nothing is
    published from it, so the attestations are worth nothing here.
    """
    environment = dict(os.environ)
    environment["BUILDX_NO_DEFAULT_ATTESTATIONS"] = "1"
    return environment


def compose_argv(*arguments: str) -> list[str]:
    """The project's own command line, spelled once.

    `--project-directory /etc/selvage` is what makes the front's relative build
    context resolve, and the compose file's `name:` is what names the project, so
    nothing here needs `-p`.
    """
    return [
        "docker",
        "compose",
        "--project-directory",
        str(ETC),
        "-f",
        str(COMPOSE),
        "--env-file",
        str(ENV_FILE),
        *arguments,
    ]


def compose(*arguments: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        compose_argv(*arguments), env=compose_environment(), text=True, check=False
    )


def run(argv: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(argv, text=True, capture_output=True, check=False)


def image_id(reference: str) -> str:
    return run(["docker", "image", "inspect", "--format", "{{.Id}}", reference]).stdout.strip()


def service_state(service: str) -> dict:
    """The container this service is running, or `{}` when there is none."""
    listed = run(compose_argv("ps", "-q", "--all", service))
    container = listed.stdout.strip().split("\n")[0].strip()
    if not container:
        return {}
    inspected = run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.Id}}|{{.State.StartedAt}}|{{.Image}}|{{.State.Status}}",
            container,
        ]
    )
    if inspected.returncode != 0:
        return {}
    identifier, started, image, status = inspected.stdout.strip().split("|")
    return {"id": identifier, "started": started, "image": image, "status": status}


def verdict(before: dict, after: dict) -> str:
    """What `up` did to one service, which is the thing the run has to report."""
    if not before:
        return "created" if after else "absent"
    if not after:
        return "removed"
    if before["id"] != after["id"]:
        return "recreated"
    if before["started"] != after["started"]:
        return "restarted"
    return "unchanged"


def atomic_write(path: Path, text: str) -> None:
    temporary = path.with_name(path.name + ".new")
    temporary.write_text(text, encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def record(pins: dict, written: str) -> bool:
    """Write the references into `.env`, keeping the file it replaces as `.env.prev`.

    The previous file is the box's own record of what the deploy moved off, so
    that a rollback is a hand edit against a file that exists rather than a
    reconstruction from a log. It is written only when the content actually
    changes, so a no-op deploy does not overwrite the last real previous state.
    """
    rendered = render_env(pins)
    if rendered == written:
        return False
    ENV_PREV.write_bytes(written.encode("utf-8"))
    os.chmod(ENV_PREV, 0o600)
    atomic_write(ENV_FILE, rendered)
    return True


def short(identifier: str) -> str:
    """Enough of a container or image id to read; the comparison is on the whole thing."""
    return identifier.replace("sha256:", "")[:12] or "-"


def assert_converged(pins: dict) -> None:
    for key, (repository, service) in PINS.items():
        wanted = image_id(pins[key])
        state = service_state(service)
        if not wanted:
            raise Refused(f"{pins[key]} is not in the local image store after `pull`")
        if not state:
            raise Refused(f"{service} has no container after `up -d`")
        if state["image"] != wanted:
            raise Refused(
                f"{service} runs {state['image']} but {repository} {pins[key]} is {wanted}"
            )
        if state["status"] != "running":
            raise Refused(f"{service} is {state['status']} after `up -d`, not running")


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("selvage-deploy takes no arguments; the request arrives on stdin", file=sys.stderr)
        return 2
    if os.geteuid() != 0:
        print("selvage-deploy must run as root", file=sys.stderr)
        return 2

    try:
        request = parse_request(sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1))
        written = ENV_FILE.read_text(encoding="utf-8") if ENV_FILE.exists() else ""
        current = read_env(written)
        pins = resolve_pins(current, request)

        actual = hashlib.sha256(COMPOSE.read_bytes()).hexdigest()
        if actual != request["COMPOSE_SHA256"]:
            raise Refused(
                f"{COMPOSE} is not the shape the request names: it is {actual}, the request "
                f"says {request['COMPOSE_SHA256']}. The deploy verifies the shape and never "
                f"writes it; a shape change is a hand install"
            )

        named = [PINS[key][1] for key in PINS if key in request]
        print("=== request ===")
        print(f"COMPOSE_SHA256={request['COMPOSE_SHA256']}  ({COMPOSE})")
        for key in PINS:
            change = "requested" if key in request else "left as it is"
            print(f"{key}={pins[key]}  ({change})")

        before = {service: service_state(service) for service in SERVICES}

        print(f"=== pull {' '.join(named)} ===")
        pulled = compose("pull", *named)
        if pulled.returncode != 0:
            raise Refused(f"`docker compose pull {' '.join(named)}` failed")

        if record(pins, written):
            print(f"wrote {ENV_FILE}; the file it replaced is {ENV_PREV}")
        else:
            print(f"{ENV_FILE} already records both references")

        print("=== up -d ===")
        if compose("up", "-d").returncode != 0:
            raise Refused("`docker compose up -d` failed")

        assert_converged(pins)

        print("=== containers ===")
        for service in SERVICES:
            was, now = before[service], service_state(service)
            print(
                f"{service:12} {verdict(was, now):10} "
                f"status={now.get('status', 'absent'):9} "
                f"started {was.get('started', '-')} -> {now.get('started', '-')}  "
                f"image {short(was.get('image', ''))} -> {short(now.get('image', ''))}"
            )
        print("=== deployed ===")
        return 0
    except Refused as refusal:
        print(f"refused: {refusal}", file=sys.stderr)
        return 1
    except OSError as error:
        print(f"refused: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
