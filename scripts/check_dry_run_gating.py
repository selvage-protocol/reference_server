#!/usr/bin/env python3
"""The guard around what a workflow's `dry_run` input promises.

A workflow that declares a `dry_run` input has a plan step conditioned positively on it, which
prints what the dispatch would have done, and every step after it carries a condition that
excludes a dry run. The flag is worth exactly as much as that second half: the plan step is a
`run:` block printing a promise, and what keeps the promise is the conditions on the steps below
it. A step added later with no condition does what the plan said it would not.

This file is one file in six repositories — `reference_server`, `specification`,
`vscode_client`, `nvim_client`, `web_client` and `jetbrains_client` — kept byte-identical with
its suite, `scripts/test_check_dry_run_gating.py`, so nothing in it names one repository's
workflows. Each runs it over its own `.github/workflows` from its own `scripts/ci-local.sh` and
its own CI, because a regression there is otherwise found by a dispatch rather than by CI.

`actionlint` cannot see it. Every step in a file like these is syntactically valid, every `if:`
is a well-formed expression and the file lints clean; the defect is *which steps do not carry
one*, or carry one that does not mean what it looks like. So the rule here is positional, and it
judges a step by where it stands rather than by its name:

  * a workflow that declares a `dry_run` input has a step conditioned **positively** on it —
    the rehearsal, which prints what a real run would do;
  * every step after that one, in its job or in a job below it, must carry a condition that
    excludes a dry run, or be in a job that does.

A mutating step added later is caught by its position after the plan, whatever it is called
and whatever it runs, which is the point of not keeping a list of commands here.

A condition is judged by what it evaluates to, not by what it contains. Each `if:` is parsed as
a GitHub Actions expression and evaluated twice, once with the input set and once with it
clear, with every other context and every function call standing for a value that is not known.
A step is gated when its condition is false on a dry run whatever those unknowns are; it is the
plan when its condition is false on a real run and names the input. Reading it that way is what
refuses `!(inputs.dry_run != true)`, which contains a gate and means its opposite, and the forms
beside it: a double negation, `!inputs.dry_run != true` (the `!` binds first), a disjunction
whose other half runs the step anyway, and a type mismatch. `inputs.dry_run` is a boolean and
`github.event.inputs.dry_run` the same input as a string, and the expression language compares
mismatched types as numbers, so `inputs.dry_run != 'true'` and
`github.event.inputs.dry_run != true` are true whatever the input is.

Run it directly, against a repository's workflows or a copy of them:

    python3 -B scripts/check_dry_run_gating.py [workflows-dir ...]

## What it cannot catch, and does not claim to

  * **A step before the plan step, or the plan step itself, that changes something outside
    the runner.** The prefix is what a dry run is meant to exercise — reads, refusals and the
    version computation — and nothing structural tells a read from a write there.
    `test_the_prefix_is_not_read` in `scripts/test_check_dry_run_gating.py` pins that residual
    rather than leaving it implied.
  * **A gate that depends on what a function returns or another context holds.** Those are
    unknown here, so a condition that excludes a dry run only through one of them is reported
    rather than assumed safe: a false alarm rather than a pass.
  * **A gate carried by `needs:`** — a job skipped because the job it needs was skipped. Only a
    job's own `if:` and its steps' are read.
  * **A mutation performed indirectly** — a reusable workflow, a composite action, or a `run:`
    body that shells out to a script which pushes. What a gated step runs is not read.
  * **Workflows without a `dry_run` input**, which is most of them: there is nothing to
    rehearse there, so there is nothing to check. A directory where *no* workflow declares the
    input is refused instead, so a scan that reaches nothing cannot report success.
"""

from __future__ import annotations

import math
import re
import sys
from pathlib import Path

import yaml

# Any mention of the input, inside a string literal or not: a condition that names it and is
# neither a gate nor the plan is reported rather than read as ungated.
DRY_RUN = re.compile(r"(?:inputs|github\.event\.inputs)\.dry_run\b", re.IGNORECASE)

# Where the input lives, and what it holds on each kind of run: `inputs` keeps a boolean input's
# type, and `github.event.inputs` carries it as a string.
CONTEXTS = {
    ("inputs", "dry_run"): {True: True, False: False},
    ("github", "event", "inputs", "dry_run"): {True: "true", False: "false"},
}

# The spellings the refusal below names.
GATE = "`if: inputs.dry_run != true`, or `if: ${{ !inputs.dry_run }}`"

GATED = "gated"
PLAN = "plan"
UNGATED = "ungated"
UNRECOGNISED = "unrecognised"


class Unknown:
    """A value this check does not know: another context, or what a function returns."""

    def __repr__(self):
        return "<unknown>"


UNKNOWN = Unknown()


class Unparsed(ValueError):
    pass


TOKEN = re.compile(
    r"""
      (?P<space>\s+)
    | (?P<number>-?(?:0x[0-9a-fA-F]+|\d+(?:\.\d+)?(?:[eE][+-]?\d+)?))
    | (?P<string>'(?:[^']|'')*')
    | (?P<operator>&&|\|\||==|!=|<=|>=|[!<>()\[\],.*])
    | (?P<name>[A-Za-z_][A-Za-z0-9_-]*)
    """,
    re.VERBOSE,
)


def tokens(text):
    found = []
    position = 0
    while position < len(text):
        match = TOKEN.match(text, position)
        if match is None:
            raise Unparsed(f"cannot read {text[position:]!r}")
        position = match.end()
        if match.lastgroup != "space":
            found.append((match.lastgroup, match.group()))
    return found


class Parser:
    """GitHub's expression grammar, by precedence from loosest: `||`, `&&`, `==` and `!=`,
    the orderings, `!`, then property access, indexing and calls."""

    def __init__(self, text):
        self.tokens = tokens(text)
        self.position = 0

    def peek(self):
        if self.position < len(self.tokens):
            return self.tokens[self.position]
        return (None, None)

    def take(self, *values):
        kind, value = self.peek()
        if kind == "operator" and value in values:
            self.position += 1
            return value
        return None

    def expect(self, value):
        if self.take(value) is None:
            raise Unparsed(f"expected {value!r}")

    def parse(self):
        tree = self.disjunction()
        if self.position != len(self.tokens):
            raise Unparsed(f"unexpected {self.peek()[1]!r}")
        return tree

    def disjunction(self):
        tree = self.conjunction()
        while self.take("||"):
            tree = ("or", tree, self.conjunction())
        return tree

    def conjunction(self):
        tree = self.equality()
        while self.take("&&"):
            tree = ("and", tree, self.equality())
        return tree

    def equality(self):
        tree = self.ordering()
        while operator := self.take("==", "!="):
            tree = ("compare", operator, tree, self.ordering())
        return tree

    def ordering(self):
        tree = self.negation()
        while operator := self.take("<", "<=", ">", ">="):
            tree = ("compare", operator, tree, self.negation())
        return tree

    def negation(self):
        if self.take("!"):
            return ("not", self.negation())
        return self.access()

    def access(self):
        tree = self.primary()
        while True:
            if self.take("."):
                kind, value = self.peek()
                if kind == "name" or (kind == "operator" and value == "*"):
                    self.position += 1
                    tree = ("property", tree, value.lower())
                else:
                    raise Unparsed("expected a property name")
            elif self.take("["):
                index = self.disjunction()
                self.expect("]")
                tree = ("index", tree, index)
            else:
                return tree

    def primary(self):
        kind, value = self.peek()
        if kind is None:
            raise Unparsed("the expression ends early")
        if self.take("("):
            tree = self.disjunction()
            self.expect(")")
            return tree
        self.position += 1
        if kind == "number":
            return ("literal", number(value))
        if kind == "string":
            return ("literal", value[1:-1].replace("''", "'"))
        if kind == "name":
            if value in ("true", "false"):
                return ("literal", value == "true")
            if value == "null":
                return ("literal", None)
            if self.take("("):
                arguments = []
                if not self.take(")"):
                    arguments.append(self.disjunction())
                    while self.take(","):
                        arguments.append(self.disjunction())
                    self.expect(")")
                return ("call", value.lower(), arguments)
            return ("context", value.lower())
        raise Unparsed(f"unexpected {value!r}")


def path_of(tree):
    """`inputs.dry_run` as `("inputs", "dry_run")`, or None when the tree is not a plain path."""
    if tree[0] == "context":
        return (tree[1],)
    if tree[0] == "property":
        head = path_of(tree[1])
        return None if head is None else head + (tree[2],)
    if tree[0] == "index" and tree[2][0] == "literal" and isinstance(tree[2][1], str):
        head = path_of(tree[1])
        return None if head is None else head + (tree[2][1].lower(),)
    return None


def number(value):
    """The expression language's coercion, used whenever two operands' types differ."""
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return 0.0
        if re.fullmatch(r"-?0x[0-9a-fA-F]+", text):
            return float(int(text, 16))
        if re.fullmatch(r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?", text):
            return float(text)
    return math.nan


def truthy(value):
    if isinstance(value, Unknown):
        return UNKNOWN
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        return value != 0 and not math.isnan(value)
    return value != ""


def kind_of(value):
    return type(None) if value is None else type(value)


def compare(operator, left, right):
    if isinstance(left, Unknown) or isinstance(right, Unknown):
        return UNKNOWN
    if kind_of(left) is not kind_of(right):
        left, right = number(left), number(right)
    elif isinstance(left, str):
        left, right = left.casefold(), right.casefold()
    elif not isinstance(left, float):
        left, right = number(left), number(right)
    if operator == "==":
        return left == right
    if operator == "!=":
        return not left == right
    return {"<": left < right, "<=": left <= right, ">": left > right, ">=": left >= right}[operator]


def evaluate(tree, dry_run):
    """The value of an expression on a run whose `dry_run` is `dry_run`, or UNKNOWN."""
    shape = tree[0]
    if shape == "literal":
        return tree[1]
    if shape in ("context", "property", "index"):
        path = path_of(tree)
        if path in CONTEXTS:
            return CONTEXTS[path][dry_run]
        return UNKNOWN
    if shape == "call":
        return UNKNOWN
    if shape == "not":
        value = truthy(evaluate(tree[1], dry_run))
        return UNKNOWN if isinstance(value, Unknown) else not value
    if shape == "compare":
        return compare(tree[1], evaluate(tree[2], dry_run), evaluate(tree[3], dry_run))
    left = evaluate(tree[1], dry_run)
    decided = truthy(left)
    stops = shape == "or"
    if decided is stops:
        return left
    right = evaluate(tree[2], dry_run)
    if decided is UNKNOWN:
        # Unknown on the left: the whole is decided only when the right half decides it the
        # same way the left could have.
        return right if truthy(right) is stops else UNKNOWN
    return right


def condition(block: dict):
    """The `if:` of a step or a job, as this reads it."""
    value = block.get("if")
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    text = " ".join(str(value).split())
    inline = re.fullmatch(r"\$\{\{(.*)\}\}", text, re.DOTALL)
    if inline:
        text = " ".join(inline.group(1).split())
    return text


def parses(expression):
    try:
        Parser(expression).parse()
    except Unparsed:
        return False
    return True


def classify(expression):
    if expression is None:
        return UNGATED
    mentions = DRY_RUN.search(expression) is not None
    try:
        tree = Parser(expression).parse()
    except Unparsed:
        return UNRECOGNISED if mentions else UNGATED
    if truthy(evaluate(tree, True)) is False:
        return GATED
    if mentions and truthy(evaluate(tree, False)) is False:
        return PLAN
    return UNRECOGNISED if mentions else UNGATED


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
        if step.state != UNRECOGNISED:
            continue
        if step.number != 0 and classify(step.expression) != UNRECOGNISED:
            # A step inside a job whose own condition is the one at fault, which is named once.
            continue
        if parses(step.expression):
            why = (
                "which names dry_run, but a dispatch with `dry_run: true` can still run it, and "
                "it is not a plan either, since a real run can run it too"
            )
        else:
            why = "which names dry_run in an expression this check cannot parse"
        problems.append(f"{path}: {step.where()} has `if: {step.expression}`, {why}; write {GATE}")

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
            f"and then run this. Add {GATE}"
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
