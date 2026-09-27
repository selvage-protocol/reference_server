#!/usr/bin/env python3
"""Dispatch this repository's deploy workflow, then wait for the run it creates.

`gh workflow run` prints no run id, so a release that dispatches `deploy-prod.yml`
and looks away cannot tell a deploy that succeeded from one that never started or
one that failed — which is how a green release has stood beside a red deploy and a
demo still on the previous version. This dispatches and then watches the run the
dispatch created, and exits non-zero unless that run concludes `success`.

The run to watch is the newest run of the workflow that is not the newest one seen
before the dispatch; `gh run list` is read before the dispatch and polled after it.
The run is named with its id and URL before it is waited on, so the log says what
was watched, and each failure names itself: no run appeared, the run did not finish
inside the deadline, or the run finished with a conclusion that is not `success`.

    scripts/wait_for_deploy.py --dispatch --workflow deploy-prod.yml --ref main \
        -f server_version=0.6.0 [-f install_shape=true]

Watching a run that already exists, and dispatching nothing — how the wait above is
proved without cutting a release (any repository `gh` can read works, hence
`--repo`):

    scripts/wait_for_deploy.py --run-id 36327799545 --repo selvage-protocol/web_client

Or waiting for the next run of a workflow to appear and finish, on its own, for a
dispatch whose id was lost:

    scripts/wait_for_deploy.py --watch-next --workflow deploy-prod.yml

A deadline that passes names the last read and what remains rather than looking
hung. The completion deadline outlasts `deploy-prod.yml`'s own `timeout-minutes: 20`
plus the queue time ahead of it, so a deploy that hangs is reported by the deploy's
own timeout before it is reported here.

`gh` is reached through `PATH` (or `SELVAGE_GH`); on a host that has it only under
Nix, run this under `nix shell nixpkgs#gh -c ...`.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import textwrap
import time
from functools import partial
from typing import Callable, NamedTuple

GH = os.environ.get("SELVAGE_GH", "gh")

DEFAULT_WORKFLOW = "deploy-prod.yml"
DEFAULT_REF = "main"
# `deploy-prod.yml`'s own job carries `timeout-minutes: 20`; the extra ten minutes
# are for the queue ahead of it and the Tailscale join, so a deploy that hangs is
# reported by the deploy's own timeout rather than by this.
DEFAULT_DEADLINE_SECONDS = 1800.0
# A run `gh workflow run` accepted reaches `gh run list` in seconds; this bounds
# the wait for that read-after-write lag rather than the deploy itself.
DEFAULT_APPEAR_DEADLINE_SECONDS = 300.0
DEFAULT_POLL_INTERVAL_SECONDS = 10.0
GH_TIMEOUT_SECONDS = 60.0
FIELDS = "databaseId,url,status,conclusion"

SUCCESS = "success"
COMPLETED = "completed"


class GhError(Exception):
    """`gh` could not be run, exited non-zero, or did not answer in time."""


class Run(NamedTuple):
    """One run of a workflow, as `gh` reports it."""

    id: int
    url: str
    status: str
    conclusion: str


class NeverAppeared(Exception):
    """No run of the workflow was newer than the one seen before the dispatch."""

    def __init__(self, before: int | None, last: Run | None, deadline: float):
        super().__init__("no run appeared")
        self.before = before
        self.last = last
        self.deadline = deadline


class DidNotFinish(Exception):
    """The run was still going when the deadline passed."""

    def __init__(self, last: Run, deadline: float):
        super().__init__("the run did not finish")
        self.last = last
        self.deadline = deadline


def run_gh(argv: list[str], repository: str | None = None) -> str:
    """Run one `gh` command and return its stdout, or raise `GhError`."""
    command = [GH, *argv]
    if repository:
        command += ["--repo", repository]
    try:
        done = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=GH_TIMEOUT_SECONDS,
        )
    except OSError as failed:
        raise GhError(f"{GH} could not be run: {failed}") from failed
    except subprocess.TimeoutExpired as failed:
        raise GhError(
            f"{GH} did not answer within {GH_TIMEOUT_SECONDS:.0f}s: {' '.join(command)}"
        ) from failed
    if done.returncode != 0:
        detail = (done.stderr or done.stdout).strip()
        raise GhError(f"{GH} exited {done.returncode}: {' '.join(command)}\n{detail}")
    return done.stdout


def to_run(payload: dict) -> Run:
    """One entry of `gh run list`/`gh run view`, normalized.

    `conclusion` is absent or null until the run completes, which is not the same
    as any of the conclusions, so it becomes the empty string rather than None.
    """
    conclusion = payload.get("conclusion")
    return Run(
        int(payload["databaseId"]),
        payload.get("url") or "",
        payload.get("status") or "",
        conclusion if isinstance(conclusion, str) else "",
    )


def newest_run(repository: str | None, workflow: str) -> Run | None:
    """The newest run of the workflow, or None when it has no runs at all."""
    listed = json.loads(
        run_gh(
            ["run", "list", "--workflow", workflow, "--limit", "1", "--json", FIELDS],
            repository,
        )
    )
    return to_run(listed[0]) if listed else None


def read_run(repository: str | None, run_id: int) -> Run:
    """One read of a run by id."""
    return to_run(
        json.loads(run_gh(["run", "view", str(run_id), "--json", FIELDS], repository))
    )


def dispatch_run(
    repository: str | None, workflow: str, ref: str, inputs: list[str]
) -> None:
    """`gh workflow run <workflow> --ref <ref> -f ...`, whose run id it does not print."""
    argv = ["workflow", "run", workflow, "--ref", ref]
    for item in inputs:
        argv += ["-f", item]
    run_gh(argv, repository)


def wait_for_new_run(
    before_id: int | None,
    read_newest: Callable[[], Run | None],
    deadline: float,
    interval: float,
) -> Run:
    """Poll until a run other than the one seen before the dispatch appears.

    The deadline is for the appearance alone: a dispatch `gh` accepted should appear
    in the run list within seconds, and a workflow that never produces one is a
    different failure from a deploy that runs long.
    """
    started = time.monotonic()
    last: Run | None = None
    while True:
        last = read_newest()
        if last is not None and last.id != before_id:
            return last
        if time.monotonic() - started >= deadline:
            raise NeverAppeared(before_id, last, deadline)
        time.sleep(interval)


def wait_for_conclusion(
    read_run: Callable[[], Run], deadline: float, interval: float
) -> Run:
    """Poll the run until it completes, or the deadline passes with the last read."""
    started = time.monotonic()
    while True:
        last = read_run()
        if last.status == COMPLETED:
            return last
        if time.monotonic() - started >= deadline:
            raise DidNotFinish(last, deadline)
        time.sleep(interval)


def input_value(inputs: list[str], key: str) -> str:
    """The value of one `KEY=VALUE` dispatch input, or "" when it is absent."""
    for item in inputs:
        name, _, value = item.partition("=")
        if name == key:
            return value
    return ""


def gh_command(parts: list[str], repository: str | None = None) -> str:
    """A `gh` command line a reader can paste, with `--repo` when one was named."""
    command = " ".join(["gh", *parts])
    if repository:
        command += f" --repo {repository}"
    return command


def redispatch_command(
    workflow: str, ref: str, inputs: list[str], repository: str | None = None
) -> str:
    """The `gh workflow run` that repeats the dispatch, with the same inputs."""
    parts = ["workflow", "run", workflow, "--ref", ref]
    for item in inputs:
        parts += ["-f", item]
    return gh_command(parts, repository)


def log_command(run_id: int, repository: str | None = None) -> str:
    """The `gh run view` that reads a failed run's log."""
    return gh_command(["run", "view", str(run_id), "--log-failed"], repository)


def watches(run: Run) -> str:
    """The line that names the run being watched."""
    return f"watching run {run.id}: {run.url}"


def conclusion_report(final: Run, repository: str | None = None) -> str:
    return textwrap.dedent(f"""\
        run {final.id} concluded {final.conclusion or 'without a conclusion'}:
          {final.url}
        Read its log with:
          {log_command(final.id, repository)}""")


def never_appeared_report(
    workflow: str, before: int | None, last: Run | None, deadline: float, dispatched: bool
) -> str:
    if last is None:
        saw = f"no runs of {workflow} are visible"
    elif last.id == before:
        since = "the dispatch" if dispatched else "the watch began"
        saw = f"the newest run is still {last.id} ({last.url}), the one seen before {since}"
    else:
        saw = f"the newest run is {last.id} ({last.url})"
    return textwrap.dedent(f"""\
        no new run of {workflow} appeared within {deadline:.0f}s{(' of the dispatch') if dispatched else ''}.
        The last read: {saw}.""")


def did_not_finish_report(label: str, last: Run, deadline: float) -> str:
    return textwrap.dedent(f"""\
        {label} did not finish within {deadline:.0f}s:
          {last.url}
        The last read said status={last.status or 'unknown'}.""")


def remains(workflow: str, ref: str, inputs: list[str], repository: str | None) -> str:
    """What a reader has left when this step fails after the release is out."""
    version = input_value(inputs, "server_version")
    released = (
        f"v{version} is tagged and published, and its image exists"
        if version
        else "it is tagged and published"
    )
    return textwrap.dedent(f"""\
        What remains:
          the release is already out — {released}. The public demo did not take it, and
          this step cannot move it further. The dispatch above is the button that moves
          the demo, and its run is the next thing to read; re-dispatch it with:
            {redispatch_command(workflow, ref, inputs, repository)}""")


def dispatch_mode(args: argparse.Namespace) -> int:
    repository = args.repo
    workflow = args.workflow
    read_newest = partial(newest_run, repository, workflow)

    try:
        before = read_newest()
    except GhError as failed:
        print(f"cannot read {workflow}'s runs before dispatching: {failed}", file=sys.stderr)
        return 1
    try:
        dispatch_run(repository, workflow, args.ref, args.inputs)
    except GhError as failed:
        print(f"could not dispatch {workflow}: {failed}", file=sys.stderr)
        return 1

    try:
        run = wait_for_new_run(
            before.id if before else None,
            read_newest,
            args.appear_deadline,
            args.poll_interval,
        )
    except NeverAppeared as failed:
        print(
            never_appeared_report(
                workflow, failed.before, failed.last, failed.deadline, dispatched=True
            ),
            file=sys.stderr,
        )
        print(remains(workflow, args.ref, args.inputs, repository), file=sys.stderr)
        return 1
    except GhError as failed:
        print(f"lost contact with gh after dispatching {workflow}: {failed}", file=sys.stderr)
        return 1
    return watch_to_conclusion(args, run)


def watch_to_conclusion(args: argparse.Namespace, run: Run) -> int:
    repository = args.repo
    workflow = args.workflow
    # A run named by `--run-id` is not necessarily one of `--workflow`'s — the
    # default names this repository's deploy — so only a dispatch or a watch of a
    # workflow gets to name it.
    label = f"{workflow} run {run.id}" if (args.dispatch or args.watch_next) else f"run {run.id}"
    print(watches(run), flush=True)
    try:
        final = wait_for_conclusion(
            partial(read_run, repository, run.id), args.deadline, args.poll_interval
        )
    except DidNotFinish as failed:
        print(
            did_not_finish_report(label, failed.last, failed.deadline),
            file=sys.stderr,
        )
        if args.dispatch:
            print(remains(workflow, args.ref, args.inputs, repository), file=sys.stderr)
        return 1
    except GhError as failed:
        print(f"lost contact with gh while watching run {run.id}: {failed}", file=sys.stderr)
        return 1

    if final.conclusion != SUCCESS:
        print(conclusion_report(final, repository), file=sys.stderr)
        if args.dispatch:
            print(remains(workflow, args.ref, args.inputs, repository), file=sys.stderr)
        return 1
    print(f"{label} concluded {SUCCESS}: {final.url}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--repo", default=None, help="OWNER/NAME; without it, gh resolves the checkout"
    )
    parser.add_argument("--workflow", default=DEFAULT_WORKFLOW)
    parser.add_argument("--ref", default=DEFAULT_REF)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--run-id", type=int, help="watch this run; dispatch nothing")
    mode.add_argument(
        "--dispatch", action="store_true", help="dispatch, then watch the run it creates"
    )
    mode.add_argument(
        "--watch-next",
        action="store_true",
        help="wait for the next run of --workflow; dispatch nothing",
    )
    parser.add_argument(
        "--deadline", type=float, default=DEFAULT_DEADLINE_SECONDS,
        help="seconds to wait for the run to conclude",
    )
    parser.add_argument(
        "--appear-deadline", type=float, default=DEFAULT_APPEAR_DEADLINE_SECONDS,
        help="seconds to wait for a dispatched run to appear",
    )
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL_SECONDS)
    parser.add_argument(
        "-f", "--input", dest="inputs", action="append", default=[], metavar="KEY=VALUE"
    )
    args = parser.parse_args(argv)

    if args.dispatch:
        return dispatch_mode(args)

    repository = args.repo
    workflow = args.workflow
    if args.run_id is not None:
        try:
            run = read_run(repository, args.run_id)
        except GhError as failed:
            print(f"cannot read run {args.run_id}: {failed}", file=sys.stderr)
            return 1
        return watch_to_conclusion(args, run)

    read_newest = partial(newest_run, repository, workflow)
    try:
        before = read_newest()
    except GhError as failed:
        print(f"cannot read {workflow}'s runs: {failed}", file=sys.stderr)
        return 1
    try:
        run = wait_for_new_run(
            before.id if before else None,
            read_newest,
            args.appear_deadline,
            args.poll_interval,
        )
    except NeverAppeared as failed:
        print(
            never_appeared_report(
                workflow, failed.before, failed.last, failed.deadline, dispatched=False
            ),
            file=sys.stderr,
        )
        return 1
    except GhError as failed:
        print(f"lost contact with gh while waiting for {workflow}: {failed}", file=sys.stderr)
        return 1
    return watch_to_conclusion(args, run)


if __name__ == "__main__":
    sys.exit(main())
