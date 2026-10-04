#!/usr/bin/env python3
"""The guard around what a workflow's `dry_run` input promises.

A workflow that declares a `dry_run` input prints a plan, and the flag is only
worth having if that plan cannot become a lie about the run it is part of: a
`dry_run: true` dispatch must not write, commit, push or dispatch anything.

`actionlint` cannot see this. Every step in such a workflow is syntactically valid, every
`if:` is a well-formed expression and the file lints clean; the defect is *which
steps do not carry one*. So the rule here is positional, and it judges a step by
where it stands rather than by its name:

  * a workflow that declares a `dry_run` input has a step conditioned **positively**
    on it — the rehearsal, which prints what a real run would do;
  * every step after that one, in its job or in a job below it, must carry a
    condition that excludes a dry run, or be in a job that does.

A mutating step added later is caught by its position after the plan, whatever it
is called and whatever it runs, which is the point of not keeping a list of
commands here.

Run it directly, against this repository's workflows or a copy of them:

    python3 -B scripts/check_dry_run_gating.py [workflows-dir ...]

or as the flake check the workflows run:
`nix build .#checks.<system>.dry-run-gating`.

## What it cannot catch, and does not claim to

  * **A step before the plan step, or the plan step itself, that changes something
    outside the runner.** The prefix is what a dry run is meant to exercise — the
    ref check and the two version guards — and nothing structural tells a read from
    a write there. `dry_run_gating_check_does_not_read_the_prefix` in
    `scripts/test_check_dry_run_gating.py` pins that residual rather than leaving it
    implied.
  * **A spelling of the same test this does not know.** A condition that names
    `dry_run` in a form not in `EXCLUDED`/`RUN_ON_DRY_RUN` is reported rather than
    assumed safe, which covers the `||` forms and anything unrecognised, but an
    equivalent test written some third way is a false alarm rather than a pass.
  * **A mutation performed indirectly** — a reusable workflow, a composite action,
    or a `run:` body that shells out to a script which pushes. What a gated step
    runs is not read.
  * **Workflows without a `dry_run` input**, which is most of them: there is nothing
    to rehearse there, so there is nothing to check. A repository where *no*
    workflow declares the input is refused instead, so a scan that reaches nothing
    cannot report success.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

DRY_RUN = r"(?:inputs|github\.event\.inputs)\.dry_run\b"

# `if: inputs.dry_run != true` and its siblings. Each is a conjunction-free test of
# the input, so a step carrying one cannot run on a dry run.
EXCLUDED = (
    re.compile(rf"!\s*{DRY_RUN}"),
    re.compile(rf"{DRY_RUN}\s*!=\s*(?:true|'true'|\"true\")"),
    re.compile(rf"{DRY_RUN}\s*==\s*(?:false|'false'|\"false\")"),
    re.compile(rf"{DRY_RUN}\s+is\s+not\s+true\b"),
)

# The rehearsal itself: the step that prints the plan. Exactly one is expected, and
# it is the boundary everything below has to be gated against.
RUN_ON_DRY_RUN = (
    re.compile(rf"{DRY_RUN}\s*$"),
    re.compile(rf"{DRY_RUN}\s*==\s*(?:true|'true'|\"true\")"),
)

GATED = "gated"
PLAN = "plan"
UNGATED = "ungated"
UNRECOGNISED = "unrecognised"


def condition(block: dict):
    """The `if:` of a step or a job, as this reads it."""
    value = block.get("if")
    if value is None:
        return None
    text = " ".join(str(value).split())
    inline = re.fullmatch(r"\$\{\{(.*)\}\}", text, re.DOTALL)
    if inline:
        text = " ".join(inline.group(1).split())
    return text


def classify(expression):
    if expression is None:
        return UNGATED
    mentions = re.search(DRY_RUN, expression)
    # `a || b` is true when either half is: a dry run excluded by one disjunct is
    # still run by the other, so no disjunction over `dry_run` is read as a gate.
    if mentions and "||" in expression:
        return UNRECOGNISED
    if any(pattern.search(expression) for pattern in EXCLUDED):
        return GATED
    if any(pattern.search(expression) for pattern in RUN_ON_DRY_RUN):
        return PLAN
    if mentions:
        return UNRECOGNISED
    return UNGATED


class Step:
    def __init__(self, job, number, name, expression, state):
        self.job = job
        self.number = number
        self.name = name
        self.expression = expression
        self.state = state

    def where(self):
        if self.number == 0:
            return f"job `{self.job}`"
        return f"job `{self.job}`, step {self.number} (`{self.name}`)"


def label(block):
    for key in ("name", "uses", "run"):
        value = block.get(key)
        if isinstance(value, str) and value.strip():
            return " ".join(value.split())[:80]
    return "<unnamed>"


def load(path):
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if document is None:
        return {}
    if not isinstance(document, dict):
        raise SystemExit(f"{path}: not a workflow document")
    return document


def dispatch_inputs(document):
    """`on.workflow_dispatch.inputs`, with `on` read the way PyYAML resolves it.

    YAML 1.1 has `on` for a boolean, so an unquoted `on:` key arrives as `True`.
    """
    triggers = document.get("on")
    if triggers is None:
        triggers = document.get(True)
    if not isinstance(triggers, dict):
        return None
    dispatch = triggers.get("workflow_dispatch")
    if not isinstance(dispatch, dict):
        return None
    inputs = dispatch.get("inputs")
    return inputs if isinstance(inputs, dict) else None


def steps_of(document):
    """Every step of every job, in the order the workflow runs them."""
    collected = []
    for job_name, job in (document.get("jobs") or {}).items():
        if not isinstance(job, dict):
            continue
        job_expression = condition(job)
        job_state = classify(job_expression)
        for number, step in enumerate(job.get("steps") or [], start=1):
            if not isinstance(step, dict):
                continue
            expression = condition(step)
            state = classify(expression)
            if job_state == GATED:
                state = GATED
            elif job_state == PLAN:
                state = PLAN
            elif job_state == UNRECOGNISED:
                state = UNRECOGNISED
            collected.append(Step(job_name, number, label(step), expression, state))
        if job_state == UNRECOGNISED:
            collected.append(Step(job_name, 0, "<the job's own condition>", job_expression, UNRECOGNISED))
    return collected


def check(path):
    """Returns the problems in one workflow and a line describing what was read."""
    problems = []
    document = load(path)
    inputs = dispatch_inputs(document)
    if inputs is None or "dry_run" not in inputs:
        return problems, None

    declared = inputs["dry_run"]
    if not isinstance(declared, dict) or declared.get("type") != "boolean":
        problems.append(
            f"{path}: the `dry_run` input is not declared `type: boolean`, so a condition "
            f"comparing it to `true` does not mean what it says"
        )

    steps = steps_of(document)
    for step in steps:
        if step.state == UNRECOGNISED:
            problems.append(
                f"{path}: {step.where()} has `if: {step.expression}`, which names dry_run in a "
                f"form this check cannot read; write `inputs.dry_run` or `inputs.dry_run != true`"
            )

    plan = next((step for step in steps if step.state == PLAN), None)
    if plan is None:
        problems.append(
            f"{path}: declares a `dry_run` input and no step is conditioned `inputs.dry_run`, so "
            f"there is no plan for this check to gate the rest of the workflow against"
        )
        return problems, f"{path.name}: declares dry_run, no plan step"

    ungated = [step for step in steps[steps.index(plan) + 1 :] if step.state != GATED]
    for step in ungated:
        problems.append(
            f"{path}: {step.where()} runs after the plan step (step {steps.index(plan) + 1}) and "
            f"{'carries no condition' if step.expression is None else 'carries `if: ' + step.expression + '`'}, "
            f"which does not exclude a dry run; a dispatch with `dry_run: true` would print the plan "
            f"and then run this. Add `if: inputs.dry_run != true`"
        )

    after = len(steps) - steps.index(plan) - 1
    if ungated:
        tail = f"{len(ungated)} of the {after} steps after it are ungated"
    else:
        tail = f"all {after} steps after it are gated"
    read = f"{path.name}: the plan is step {steps.index(plan) + 1} of {len(steps)}, and {tail}"
    return problems, read


def main(argv):
    roots = [Path(argument) for argument in argv[1:]] or [Path(".github/workflows")]
    files = sorted({path for root in roots for path in list(root.glob("*.yml")) + list(root.glob("*.yaml"))})
    if not files:
        print(f"refusing: no workflow file under {', '.join(str(root) for root in roots)}", file=sys.stderr)
        return 1

    problems = []
    read = []
    for path in files:
        found, line = check(path)
        problems += found
        if line:
            read.append(line)

    for line in read:
        print(line)
    if not read:
        print(
            f"refusing: none of the {len(files)} workflow files read declares a `dry_run` input, so "
            f"this check reached nothing; if the flag is gone, remove the check with it",
            file=sys.stderr,
        )
        return 1

    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
