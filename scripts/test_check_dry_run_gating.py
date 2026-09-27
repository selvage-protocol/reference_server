#!/usr/bin/env python3
"""The guard around `scripts/check_dry_run_gating.py`, which reads a workflow back and
refuses one whose `dry_run` flag is a lie.

The rule it enforces is positional: within a workflow that declares a `dry_run`
input, the first step conditioned positively on it is the rehearsal, and every step
after it has to carry a condition that excludes a dry run. So what the tests here
have to pin is that this reads *position* and not a list of commands:

  * the shape that shipped — a plan step and steps after it with no condition — is
    refused, and the refusal names the step and says what to add;
  * each of the spellings a gating condition is written in across these
    repositories is accepted: `inputs.dry_run != true` on a step, on a job, next to
    another test, and `${{ !inputs.dry_run }}`;
  * a step after the plan gated on some *other* input is refused, because
    `if: inputs.install_shape` is not a condition that excludes a dry run;
  * a step after the plan joined to a dry-run test by `||` is refused, because the
    other disjunct runs it anyway;
  * a workflow that declares the input and never prints a plan is refused, and so is
    a directory where nothing declares the input at all — a scan that reaches
    nothing must not report success;
  * a step *before* the plan is not read, which is the residual the checker's own
    docstring states; the test that pins it is here so the claim cannot drift.

`check_dry_run_gating.py` needs `PyYAML`, which the flake check hands it
(`python3.withPackages (ps: [ ps.pyyaml ])`). Run the suite through that, or with
any Python that has it:

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
# Nothing is built in `/tmp`: it is a RAM-backed tmpfs on some hosts, where a build
# has taken a machine down. The flake check leaves this unset and gets `/build`.
WORKDIR = Path(os.environ.get("TMPDIR") or "/build") / "dry-run-gating"

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


def plan():
    return step(PLAN, condition="inputs.dry_run")


def push():
    return step("push the bump and its tag", run="git push --atomic origin HEAD:main")


class DryRunGatingTest(unittest.TestCase):
    def setUp(self):
        WORKDIR.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix="check-", dir=WORKDIR))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def read_back(self, **workflows):
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

    def test_the_shape_that_shipped_is_refused(self):
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
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("the plan is step 1 of 3, and all 2 steps after it are gated", result.stdout)

    def test_job_level_gating_is_accepted(self):
        result = self.read_back(
            release_yml=document(
                job("rehearse", plan())
                + job("release", step("bump", run="scripts/bump-version.sh"), condition="inputs.dry_run != true")
            )
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_inline_bang_gating_is_accepted(self):
        result = self.read_back(
            release_yml=document(
                job(
                    "release",
                    plan(),
                    step("push the bump and its tag", condition="${{ !inputs.dry_run }}"),
                )
            )
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_gating_beside_another_test_is_accepted(self):
        result = self.read_back(
            release_yml=document(
                job(
                    "release",
                    plan(),
                    step(
                        "push the bump and its tag",
                        condition="github.event_name == 'workflow_dispatch' && inputs.dry_run != true",
                    ),
                )
            )
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

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

    def test_a_disjunction_over_dry_run_is_refused(self):
        result = self.read_back(
            release_yml=document(
                job(
                    "release",
                    plan(),
                    step(
                        "push the bump and its tag",
                        condition="inputs.dry_run != true || github.event_name == 'push'",
                    ),
                )
            )
        )
        self.assert_refused(result, "names dry_run in a form this check cannot read")

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
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
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
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
