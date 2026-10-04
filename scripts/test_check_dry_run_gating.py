#!/usr/bin/env python3
"""The guard around `scripts/check_dry_run_gating.py`, which reads a workflow back and refuses
one whose `dry_run` flag is a lie.

The rule it enforces is positional: within a workflow that declares a `dry_run` input, the
first step conditioned positively on it is the rehearsal, and every step after it — in its job
or in a job below it — has to carry a condition that excludes a dry run. So what the tests here
have to pin is that this reads *position* and not a list of commands, and that it reads what a
condition *means* rather than what it contains:

  * a plan step and steps after it with no condition is refused, and the refusal names the
    step and says what to add, and so is a step appended to a job that is one long ordered
    step list;
  * each of the spellings a gating condition is written in across these repositories is
    accepted: `inputs.dry_run != true` on a step or on a job, next to another test,
    `${{ !inputs.dry_run }}`, `inputs.dry_run == false`, and the string comparison the
    `github.event.inputs` context needs;
  * a condition that contains one of those spellings and means something else is refused: a
    negation of it, a double negation, a comparison of the boolean `inputs.dry_run` with a
    string or of the string `github.event.inputs.dry_run` with a boolean, a dry-run test inside
    a string literal, and a disjunction whose other half runs the step anyway;
  * a step after the plan gated on some *other* input is refused, because
    `if: inputs.install_shape` is not a condition that excludes a dry run;
  * a workflow that declares the input and never prints a plan is refused, and so is a
    directory where nothing declares the input at all — a scan that reaches nothing must not
    report success;
  * a step *before* the plan is not read, which is the residual the checker's own docstring
    states; the test that pins it is here so the claim cannot drift.

The checker needs `PyYAML`. Each repository that carries it runs this suite from its own
`scripts/ci-local.sh` and its own CI; run it with any Python that has PyYAML:

    python3 -B scripts/test_check_dry_run_gating.py
"""

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
CHECK = HERE / "check_dry_run_gating.py"
# Nothing is built in `/tmp`: it is a RAM-backed tmpfs on some hosts, where a build has
# taken a machine down. `scripts/ci-local.sh` points `TMPDIR` at `.tmp/` inside the
# checkout and a nix build at its own `/build`; with neither, the scratch comes from the
# same place the gate's does.
WORKDIR = Path(os.environ.get("TMPDIR") or (HERE.parent / ".tmp")) / "dry-run-gating"

PLAN = "the plan, and nothing done"


def document(jobs, dry_run="type: boolean"):
    """A workflow with a `dry_run` input, and the jobs given."""
    head = textwrap.dedent(
        f"""\
        name: release
        on:
          workflow_dispatch:
            inputs:
              dry_run:
                description: rehearse this release
                required: false
                default: false
                {dry_run}
        jobs:
        """
    )
    return head + textwrap.indent(textwrap.dedent(jobs), "  ")


def job(name, *steps, condition=None):
    head = [f"{name}:", "  runs-on: ubuntu-24.04"]
    if condition is not None:
        head.append(f'  if: "{condition}"')
    head.append("  steps:")
    body = textwrap.indent(textwrap.dedent("".join(steps)), "    ")
    return "\n".join(head) + "\n" + body


def step(name, condition=None, run="true"):
    lines = [f"- name: {name}"]
    if condition is not None:
        lines.append(f'  if: "{condition}"')
    lines.append(f"  run: {run}")
    return "\n".join(lines) + "\n"


def plan(condition="inputs.dry_run"):
    return step(PLAN, condition=condition)


def push():
    return step("push the bump and its tag", run="git push --atomic origin HEAD:main")


# Conditions that contain an accepted spelling, or look like one, and run the step on a dry run
# all the same. Each is refused wherever it stands after the plan.
RUN_ON_A_DRY_RUN_ALL_THE_SAME = {
    # The negation of a gate is the plan's own condition.
    "a negated `!= true`": "!(inputs.dry_run != true)",
    "a negated `== false`": "!(inputs.dry_run == false)",
    "a negated `!`": "!(!inputs.dry_run)",
    "a double negation": "!!inputs.dry_run",
    # `!` binds tighter than `!=`, so this is `(!inputs.dry_run) != true`, which is `dry_run`.
    "a negation compared with `!= true`": "!inputs.dry_run != true",
    "a negated gate beside another test": "github.event_name == 'workflow_dispatch' && !(inputs.dry_run != true)",
    # `inputs.dry_run` is a boolean, and a boolean compared with a string is compared as numbers:
    # `'true'` is not one, so `!=` holds whatever the input is.
    "the boolean compared with the string `'true'`": "inputs.dry_run != 'true'",
    # `github.event.inputs` carries the same input as a string, and the string `'true'` is not
    # the number the boolean `true` becomes, so `!=` holds there too.
    "the string compared with the boolean `true`": "github.event.inputs.dry_run != true",
    "the string, negated": "!(github.event.inputs.dry_run != 'true')",
    # A spelling inside a string literal is text, not a test of the input.
    "a gate inside a string literal": "contains('inputs.dry_run != true', github.ref)",
    # Either half of a disjunction runs the step.
    "a disjunction after `!= true`": "inputs.dry_run != true || github.event_name == 'push'",
    "a disjunction after `== false`": "inputs.dry_run == false || always()",
    "a disjunction before `!`": "github.event_name == 'push' || !inputs.dry_run",
    "a gate that is not a whole expression": "${{ inputs.dry_run != true }} && true",
}


class DryRunGatingTest(unittest.TestCase):
    def setUp(self):
        WORKDIR.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix="check-", dir=WORKDIR))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def read_back(self, **workflows):
        for path in self.root.glob("*.yml"):
            path.unlink()
        for name, text in workflows.items():
            (self.root / f"{name.removesuffix('_yml')}.yml").write_text(text, encoding="utf-8")
        return subprocess.run(
            [sys.executable, "-B", str(CHECK), str(self.root)],
            capture_output=True,
            text=True,
            check=False,
        )

    def assert_refused(self, result, *says):
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        for saying in says:
            self.assertIn(saying, result.stderr)

    def assert_accepted(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def gated_by(self, condition):
        return self.read_back(release_yml=document(job("release", plan(), step("push the bump and its tag", condition=condition))))

    def test_an_ungated_step_after_the_plan_is_refused(self):
        result = self.read_back(
            release_yml=document(
                job("release", plan(), step("resolve the page revision"), push())
            )
        )
        self.assert_refused(
            result,
            "job `release`, step 2 (`resolve the page revision`)",
            "step 3 (`push the bump and its tag`) runs after the plan step",
            "Add `if: inputs.dry_run != true`",
        )

    def test_a_step_appended_to_a_long_single_job_list_is_refused(self):
        # A workflow that is one job with one ordered step list is where a regression takes
        # this shape: nothing names the appended step, and the plan step it would run through
        # is several steps above it.
        result = self.read_back(
            release_yml=document(
                job(
                    "release",
                    step("refuse a version that is not a published tag"),
                    step("build the request"),
                    plan(),
                    step("deploy", condition="${{ !inputs.dry_run }}"),
                    step("verify what can be verified", condition="${{ !inputs.dry_run }}"),
                    step("pin the image in .env", run="scripts/deploy.sh"),
                )
            )
        )
        self.assert_refused(
            result,
            "job `release`, step 6 (`pin the image in .env`)",
            "carries no condition",
            "Add `if: inputs.dry_run != true`, or `if: ${{ !inputs.dry_run }}`",
        )

    def test_step_level_gating_is_accepted(self):
        result = self.read_back(
            release_yml=document(
                job(
                    "release",
                    plan(),
                    step("resolve the page revision", condition="inputs.dry_run != true"),
                    step("push the bump and its tag", condition="inputs.dry_run != true"),
                )
            )
        )
        self.assert_accepted(result)
        self.assertIn("the plan is step 1 of 3, and all 2 steps after it are gated", result.stdout)

    def test_job_level_gating_is_accepted(self):
        result = self.read_back(
            release_yml=document(
                job("rehearse", plan())
                + job("release", step("bump", run="scripts/bump-version.sh"), condition="inputs.dry_run != true")
            )
        )
        self.assert_accepted(result)

    def test_inline_bang_gating_is_accepted(self):
        self.assert_accepted(self.gated_by("${{ !inputs.dry_run }}"))

    def test_equals_false_gating_is_accepted(self):
        self.assert_accepted(self.gated_by("inputs.dry_run == false"))

    def test_gating_beside_another_test_is_accepted(self):
        self.assert_accepted(self.gated_by("github.event_name == 'workflow_dispatch' && inputs.dry_run != true"))

    def test_gating_beside_a_disjunction_of_other_tests_is_accepted(self):
        # The `||` here is between two other tests; the conjunction with the gate still excludes
        # a dry run, whichever of them holds.
        self.assert_accepted(
            self.gated_by(
                "(github.event_name == 'push' || github.event_name == 'workflow_dispatch') && inputs.dry_run != true"
            )
        )

    def test_the_string_context_is_read_as_a_string(self):
        # `github.event.inputs` carries a boolean input as the string `'true'` or `'false'`, so
        # its plan and its gates are written as string comparisons.
        result = self.read_back(
            release_yml=document(
                job(
                    "release",
                    plan(condition="github.event.inputs.dry_run == 'true'"),
                    step("push the bump and its tag", condition="github.event.inputs.dry_run != 'true'"),
                    step("publish", condition="github.event.inputs.dry_run == 'false'"),
                )
            )
        )
        self.assert_accepted(result)

    def test_a_condition_that_still_runs_on_a_dry_run_is_refused(self):
        for shape, condition in RUN_ON_A_DRY_RUN_ALL_THE_SAME.items():
            with self.subTest(shape, condition=condition):
                result = self.gated_by(condition)
                self.assert_refused(result, "step 2 (`push the bump and its tag`)")
                self.assertNotIn("all 1 steps after it are gated", result.stdout)

    def test_a_job_whose_condition_still_runs_on_a_dry_run_is_refused(self):
        result = self.read_back(
            release_yml=document(
                job("rehearse", plan())
                + job("release", step("bump", run="scripts/bump-version.sh"), condition="!(inputs.dry_run != true)")
            )
        )
        self.assert_refused(result, "job `release`, step 1 (`bump`)")

    def test_a_negated_gate_is_named_as_not_excluding_a_dry_run(self):
        self.assert_refused(
            self.gated_by("!(inputs.dry_run != true)"),
            "carries `if: !(inputs.dry_run != true)`, which does not exclude a dry run",
        )

    def test_a_disjunction_over_dry_run_is_refused(self):
        self.assert_refused(
            self.gated_by("inputs.dry_run != true || github.event_name == 'push'"),
            "names dry_run, but a dispatch with `dry_run: true` can still run it",
        )

    def test_an_expression_that_does_not_parse_is_refused(self):
        self.assert_refused(
            self.gated_by("inputs.dry_run != true &&"),
            "names dry_run in an expression this check cannot parse",
        )

    def test_a_step_after_the_plan_gated_on_another_input_is_refused(self):
        result = self.read_back(
            release_yml=document(
                job(
                    "release",
                    plan(),
                    step("install the box's shape", condition="inputs.install_shape"),
                )
            )
        )
        self.assert_refused(
            result,
            "step 2 (`install the box's shape`)",
            "which does not exclude a dry run",
        )

    def test_a_workflow_with_no_plan_step_is_refused(self):
        result = self.read_back(
            release_yml=document(job("release", push())),
        )
        self.assert_refused(result, "no step is conditioned `inputs.dry_run`")

    def test_a_workflow_without_the_input_is_not_read(self):
        result = self.read_back(
            release_yml=document(
                job("release", plan(), step("push the bump and its tag", condition="inputs.dry_run != true"))
            ),
            deploy_yml=textwrap.dedent(
                """\
                name: deploy
                on:
                  workflow_dispatch:
                jobs:
                  deploy:
                    runs-on: ubuntu-24.04
                    steps:
                      - run: scripts/deploy.sh
                """
            ),
        )
        self.assert_accepted(result)
        self.assertNotIn("deploy.yml", result.stdout + result.stderr)

    def test_a_directory_that_reaches_no_input_is_refused(self):
        result = self.read_back(
            deploy_yml=textwrap.dedent(
                """\
                name: deploy
                on:
                  workflow_dispatch:
                jobs:
                  deploy:
                    runs-on: ubuntu-24.04
                    steps:
                      - run: scripts/deploy.sh
                """
            )
        )
        self.assert_refused(result, "none of the 1 workflow files read declares a `dry_run` input")

    def test_a_dry_run_input_that_is_not_boolean_is_refused(self):
        result = self.read_back(
            release_yml=document(
                job("release", plan(), step("push the bump and its tag", condition="inputs.dry_run != true")),
                dry_run="type: string",
            )
        )
        self.assert_refused(result, "is not declared `type: boolean`")

    def test_the_prefix_is_not_read(self):
        # The residual the checker states: everything before the plan is assumed to be
        # what a dry run is for, so a mutating step standing there is not caught.
        result = self.read_back(
            release_yml=document(
                job("release", push(), plan(), step("publish", condition="inputs.dry_run != true"))
            )
        )
        self.assert_accepted(result)


if __name__ == "__main__":
    unittest.main()
