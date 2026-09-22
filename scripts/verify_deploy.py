#!/usr/bin/env python3
"""Verify that the box a deploy just reached serves what was asked for.

Two reads, and only one of them is an assertion.

* **The origin**, over the Tailscale SSH path the deploy itself used, through the
  front on the box's own loopback: `https://127.0.0.1/meta` and the page at `/`.
  That is the honest source — it is what is running, with no edge between the
  reader and it — so it **fails the run** when the version asked for is not the
  version served, and when the page stops answering 200.
* **The public origin**, over Cloudflare, from this runner. Cloudflare answers a
  programmatic client on a datacenter address with a managed challenge, so this
  read is a **report**: the challenge is named in plain words, the attempt ends at
  the first challenge response rather than polling a deadline it cannot pass, and
  a challenge never turns a healthy deploy red. When the edge does let a client
  like this one through the read is compared, so the assertion comes back on its
  own and no flag has to be remembered to turn it back on.

The origin read needs `-k`: the certificate is the Cloudflare Origin CA pair
issued for the public name, which is in no trust store and does not match
`127.0.0.1` in any case. The front is reached directly, not through the edge, and
nothing from the dispatch reaches the remote shell.

    scripts/verify_deploy.py --expect-version 0.2.1
    scripts/verify_deploy.py                      # a page-only deploy: /meta is read, not compared
"""

import argparse
import json
import subprocess
import sys
import tempfile
import time
from functools import partial
from pathlib import Path
from typing import NamedTuple

SSH_TARGET = "deployci@selvage-protocol-prod"
ORIGIN_URL = "https://127.0.0.1"
PUBLIC_URL = "https://selvage.dontblameme.dev"

# Long enough for a front that a deploy has just recreated to answer again, and
# short enough that a step which cannot pass says so instead of looking hung.
ORIGIN_DEADLINE_SECONDS = 60.0
PUBLIC_DEADLINE_SECONDS = 60.0
POLL_INTERVAL_SECONDS = 5.0
CURL_MAX_SECONDS = 10
SPAWN_TIMEOUT_SECONDS = 60

PAGE_MARKER = "origin-page:"
META_MARKER = "origin-meta:"

# The whole of what `deployci` runs on the box, in one line each so the answer is
# parsed without depending on a JSON parser being installed there. Neither the
# version nor anything else out of the dispatch is in here: it is a comparison on
# this side and never a word in a remote shell.
REMOTE_READ = """\
printf 'origin-page:%s\\n' "$(curl -sk -o /dev/null -w '%{http_code}' --max-time @MAX@ @URL@/)"
printf 'origin-meta:%s\\n' "$(curl -sk --max-time @MAX@ @URL@/meta | tr -d '\\n')"
"""

CHALLENGE = "challenge"
REPORTED = "reported"
UNREADABLE = "unreadable"

# The header is the reliable half of the observation — Cloudflare's managed
# challenge came back as a 403 carrying `cf-mitigated: challenge` — and the body
# markers are the belt to its braces, because an interstitial served without the
# header would otherwise read as a real failure and turn a healthy deploy red.
MITIGATION_HEADER = "cf-mitigated"
INTERSTITIAL_BODY_MARKERS = (
    "just a moment",
    "cdn-cgi/challenge-platform",
    "attention required! | cloudflare",
)


class Origin(NamedTuple):
    """What one read of the box's front produced."""

    page: str
    meta: str
    error: str = ""


class Public(NamedTuple):
    """What one read of the public origin produced."""

    status: str
    headers: str
    body: str
    error: str = ""


class OriginAssertion(Exception):
    """The origin did not serve what was asked for before the deadline."""


class PublicAssertion(Exception):
    """The edge answered without a challenge and still did not report the version."""


def json_server(body: str) -> str:
    """The `server` field of a `/meta` body, or "" when it does not carry one."""
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        return ""
    if not isinstance(parsed, dict):
        return ""
    served = parsed.get("server")
    return served if isinstance(served, str) else ""


def header_value(headers: str, name: str) -> str:
    """The first value of `name` in a raw header block, or ""."""
    for line in headers.splitlines():
        field, _, value = line.partition(":")
        if field.strip().lower() == name.lower():
            return value.strip()
    return ""


def origin_problem(reading: Origin, expect_version: str | None) -> str | None:
    """Why this origin read is not yet the read that was asked for, or None."""
    if reading.error:
        return reading.error
    if reading.page != "200":
        return f"the page answered {reading.page or 'nothing'}"
    served = json_server(reading.meta)
    if not served:
        return f"/meta carried no server field: {(reading.meta or 'nothing')[:200]!r}"
    if expect_version is not None and served != f"selvaged/{expect_version}":
        return f"/meta reports {served}"
    return None


def observed(reading: Origin) -> str:
    """The state a failed origin poll reports rather than guessing at."""
    if reading.error:
        return reading.error
    return f"the page answered {reading.page or 'nothing'} and /meta says {json_server(reading.meta) or 'nothing'}"


def wait_for_origin(
    read, expect_version: str | None, deadline: float, interval: float
) -> Origin:
    """Poll the origin until it serves what was asked for, or the deadline passes.

    A dead line with a stated observation, not a sleep and a hope: every failure
    carries what the last read actually said.
    """
    started = time.monotonic()
    while True:
        reading = read()
        problem = origin_problem(reading, expect_version)
        if problem is None:
            return reading
        if time.monotonic() - started >= deadline:
            raise OriginAssertion(f"{problem}; last read: {observed(reading)}")
        time.sleep(interval)


def classify_public(reading: Public, expect_version: str | None) -> tuple[str, str]:
    """What one public read says, in the three shapes this run cares about."""
    if reading.error:
        return UNREADABLE, reading.error
    mitigated = header_value(reading.headers, MITIGATION_HEADER)
    lowered = reading.body.lower()
    marker = next((m for m in INTERSTITIAL_BODY_MARKERS if m in lowered), "")
    if mitigated or marker:
        detail = f"{reading.status or 'no status'} with "
        detail += f"{MITIGATION_HEADER}: {mitigated}" if mitigated else f"a Cloudflare interstitial body ({marker})"
        return CHALLENGE, detail
    if reading.status == "200":
        served = json_server(reading.body)
        if served and (expect_version is None or served == f"selvaged/{expect_version}"):
            return REPORTED, served
        return UNREADABLE, f"200 reporting {(served or 'no server field')!r}"
    return UNREADABLE, f"{reading.status or 'no status'}"


def wait_for_public(read, expect_version: str | None, deadline: float, interval: float) -> tuple[str, str]:
    """Read the public origin, ending at the first challenge rather than polling it.

    A challenge is an answer about the edge, not about the deployment, and it
    cannot be passed by waiting: the attempt is over the moment it arrives.
    """
    started = time.monotonic()
    while True:
        verdict, detail = classify_public(read(), expect_version)
        if verdict in (CHALLENGE, REPORTED):
            return verdict, detail
        if time.monotonic() - started >= deadline:
            raise PublicAssertion(detail)
        time.sleep(interval)


def read_origin(ssh_target: str, origin_url: str) -> Origin:
    """One read of the front on the box, as `deployci` over Tailscale SSH."""
    command = REMOTE_READ.replace("@MAX@", str(CURL_MAX_SECONDS)).replace("@URL@", origin_url)
    argv = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=accept-new",
        ssh_target,
        command,
    ]
    try:
        done = subprocess.run(
            argv, capture_output=True, text=True, timeout=SPAWN_TIMEOUT_SECONDS, check=False
        )
    except subprocess.TimeoutExpired:
        return Origin("", "", f"ssh to {ssh_target} did not answer within {SPAWN_TIMEOUT_SECONDS}s")
    if done.returncode != 0:
        return Origin(
            "", "", f"ssh to {ssh_target} exited {done.returncode}: {done.stderr.strip()}"
        )
    page = ""
    meta = ""
    for line in done.stdout.splitlines():
        if line.startswith(PAGE_MARKER):
            page = line[len(PAGE_MARKER) :].strip()
        elif line.startswith(META_MARKER):
            meta = line[len(META_MARKER) :].strip()
    if not page:
        return Origin("", "", f"the read produced no page status: {done.stdout.strip()!r}")
    return Origin(page, meta)


def read_public(public_url: str) -> Public:
    """One read of the public origin from here, headers and body kept.

    `-f` is deliberately absent: the body and the headers of a refusal are the
    whole of what this read is for.
    """
    with tempfile.TemporaryDirectory() as workdir:
        headers = Path(workdir) / "headers"
        body = Path(workdir) / "body"
        argv = [
            "curl",
            "-sS",
            "--max-time",
            str(CURL_MAX_SECONDS),
            "-D",
            str(headers),
            "-o",
            str(body),
            "-w",
            "%{http_code}",
            f"{public_url}/meta",
        ]
        try:
            done = subprocess.run(
                argv, capture_output=True, text=True, timeout=SPAWN_TIMEOUT_SECONDS, check=False
            )
        except subprocess.TimeoutExpired:
            return Public("", "", "", f"curl did not answer within {SPAWN_TIMEOUT_SECONDS}s")
        status = done.stdout.strip()
        header_text = headers.read_text(encoding="utf-8", errors="replace") if headers.exists() else ""
        body_text = body.read_text(encoding="utf-8", errors="replace") if body.exists() else ""
        if done.returncode != 0 and status in ("", "000"):
            return Public(status, header_text, body_text, f"curl exited {done.returncode}: {done.stderr.strip()}")
    return Public(status, header_text, body_text)


def origin_sentence(reading: Origin, origin_url: str, ssh_target: str, expect_version: str | None) -> str:
    """The line a passing origin assertion prints."""
    sentence = (
        f"the origin: {origin_url} through the front on {ssh_target} reports "
        f"{json_server(reading.meta)}, and the page answers {reading.page}"
    )
    if expect_version is None:
        sentence += " (no server version was dispatched, so /meta is read and not compared)"
    return sentence


CHALLENGE_REPORT = """\
the public read: {url}/meta was answered with a Cloudflare challenge, so it is not asserted this run.
  Cloudflare answered {detail}. A managed challenge is answered at Cloudflare's edge, before the
  request reaches this origin, and no programmatic client on a datacenter address can pass it:
  not this runner, and not the Azure box itself. The same read from a residential address does
  answer, which is how the edge was told apart from the deployment. What is asserted above is the
  origin's own answer, read on the box through the front. This read is a report; it is compared,
  and does fail the run, whenever the edge does let a client like this one through."""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ssh-target", default=SSH_TARGET)
    parser.add_argument("--origin-url", default=ORIGIN_URL)
    parser.add_argument("--public-url", default=PUBLIC_URL)
    parser.add_argument(
        "--expect-version",
        default=None,
        help="the selvaged version this run deployed; without it /meta is read and not compared",
    )
    parser.add_argument("--origin-deadline", type=float, default=ORIGIN_DEADLINE_SECONDS)
    parser.add_argument("--public-deadline", type=float, default=PUBLIC_DEADLINE_SECONDS)
    parser.add_argument("--poll-interval", type=float, default=POLL_INTERVAL_SECONDS)
    args = parser.parse_args(argv)

    try:
        origin = wait_for_origin(
            partial(read_origin, args.ssh_target, args.origin_url),
            args.expect_version,
            args.origin_deadline,
            args.poll_interval,
        )
    except OriginAssertion as failed:
        print(f"the origin did not serve what was deployed: {failed}", file=sys.stderr)
        return 1
    print(origin_sentence(origin, args.origin_url, args.ssh_target, args.expect_version))

    try:
        verdict, detail = wait_for_public(
            partial(read_public, args.public_url),
            args.expect_version,
            args.public_deadline,
            args.poll_interval,
        )
    except PublicAssertion as failed:
        print(
            "the public origin answered without a challenge and did not report what was "
            f"deployed: {failed}. The origin on the box above did answer, so this is the "
            "public path from this runner rather than the deployment.",
            file=sys.stderr,
        )
        return 1
    if verdict == CHALLENGE:
        print(CHALLENGE_REPORT.format(url=args.public_url, detail=detail))
    else:
        print(f"the public read: {args.public_url}/meta reports {detail}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
