#!/usr/bin/env python3
"""What `scripts/wait_for_deploy.py` watches, and what it does when it cannot.

`gh workflow run` prints no run id, so the release has to read the workflow's runs
to find the one its dispatch created. The choices that decide a release's colour
are all here:

  * **the run to watch is the newest one that is not the newest seen before the
    dispatch**, read from `gh run list` on both sides of the dispatch;
  * **the completion deadline outlasts the deploy's own `timeout-minutes`**, and a
    deadline that passes reports the last read rather than looking hung;
  * **three failures, each named as itself** and each saying what remains: the
    release is out, the demo did not take it, and the deploy's own run is the next
    thing to read, with the `gh workflow run` that repeats the dispatch;
  * **watching a run that already exists** (`--run-id`) needs no dispatch, which is
    how the wait is proved without cutting a release.

The poll loops are driven with real, tiny deadlines and injected reads, and a fake
read stops its own loop rather than racing the wall clock. The `gh`-facing edges
run against a stand-in on `PATH`, where a mistake in an argument, a missing binary
or a `gh` that never answers would otherwise only show up on a runner.

Run it directly:

    python3 -B scripts/test_wait_for_deploy.py
"""

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import wait_for_deploy as wait  # noqa: E402  (the path above makes this importable)

GREEN = wait.Run(36327799545, "https://github.com/selvage-protocol/web_client/actions/runs/36327799545", "completed", "success")
RED = wait.Run(36326818510, "https://github.com/selvage-protocol/web_client/actions/runs/36326818510", "completed", "failure")


def run_url(run_id: int) -> str:
    """`GREEN`'s run URL with this id in place of its own.

    `GREEN` and `RED` are runs that exist, so their links are spelled out above and
    the link check reads them. The two ids below name no run at all, so a URL
    spelled in full for one would be an address that check fetches and gets a 404
    from.
    """
    return GREEN.url.replace(str(GREEN.id), str(run_id))


BEFORE = wait.Run(36100000000, run_url(36100000000), "completed", "success")
RUNNING = wait.Run(36330000000, run_url(36330000000), "in_progress", "")

LIST_FIELDS = ["run", "list", "--workflow", "deploy-prod.yml", "--limit", "1", "--json", wait.FIELDS]

# A stand-in `gh`: records the argv it was handed, answers stdout/stderr/status from
# the environment, so the tests read the argument list rather than a network. The
# interpreter is named by its absolute path because a build sandbox has no
# `/usr/bin/env`.
STUB_GH = f'''\
#!{sys.executable}
import os
import sys

with open(os.environ["STUB_LOG"], "a", encoding="utf-8") as handle:
    handle.write(" ".join(sys.argv[1:]) + "\\n")
sys.stdout.write(os.environ.get("STUB_STDOUT", ""))
sys.stderr.write(os.environ.get("STUB_STDERR", ""))
sys.exit(int(os.environ.get("STUB_STATUS", "0")))
'''


class TheRunToWatchTest(unittest.TestCase):
    def test_the_run_created_is_the_newest_one_not_seen_before(self):
        readings = [BEFORE, BEFORE, GREEN]
        calls = []

        def read():
            calls.append(1)
            return readings[min(len(calls), len(readings)) - 1]

        found = wait.wait_for_new_run(BEFORE.id, read, deadline=5.0, interval=0.01)
        self.assertEqual(found, GREEN)
        self.assertEqual(len(calls), 3, "the two reads that matched the snapshot are not the run")

    def test_a_workflow_with_no_runs_before_takes_the_first_run_it_sees(self):
        found = wait.wait_for_new_run(None, lambda: GREEN, deadline=1.0, interval=0.01)
        self.assertEqual(found, GREEN)

    def test_a_run_that_never_appears_is_its_own_failure(self):
        with self.assertRaises(wait.NeverAppeared) as raised:
            wait.wait_for_new_run(BEFORE.id, lambda: BEFORE, deadline=0.03, interval=0.01)
        self.assertEqual(raised.exception.before, BEFORE.id)
        self.assertEqual(raised.exception.last, BEFORE)
        self.assertEqual(raised.exception.deadline, 0.03)

    def test_a_workflow_that_has_runs_at_all_is_worth_retrying(self):
        readings = [None, None, GREEN]
        calls = []

        def read():
            calls.append(1)
            return readings[min(len(calls), len(readings)) - 1]

        found = wait.wait_for_new_run(None, read, deadline=5.0, interval=0.01)
        self.assertEqual(found, GREEN)
        self.assertEqual(len(calls), 3)


class TheConclusionTest(unittest.TestCase):
    def test_a_completed_run_is_returned_with_its_conclusion(self):
        final = wait.wait_for_conclusion(lambda: GREEN, deadline=1.0, interval=0.01)
        self.assertEqual(final, GREEN)

    def test_a_run_that_does_not_finish_is_its_own_failure(self):
        with self.assertRaises(wait.DidNotFinish) as raised:
            wait.wait_for_conclusion(lambda: RUNNING, deadline=0.03, interval=0.01)
        self.assertEqual(raised.exception.last, RUNNING)
        self.assertEqual(raised.exception.deadline, 0.03)

    def test_a_run_still_going_is_worth_retrying_before_the_deadline(self):
        readings = [RUNNING, RUNNING, RED]
        calls = []

        def read():
            calls.append(1)
            return readings[min(len(calls), len(readings)) - 1]

        final = wait.wait_for_conclusion(read, deadline=5.0, interval=0.01)
        self.assertEqual(final, RED)
        self.assertEqual(len(calls), 3)

    def test_an_unfinished_run_carries_no_conclusion(self):
        self.assertEqual(wait.to_run({"databaseId": 7, "status": "queued"}), wait.Run(7, "", "queued", ""))


class TheReadingsTest(unittest.TestCase):
    """`gh`'s JSON, and the command lines that produce it."""

    @contextmanager
    def with_stub(self, stdout="", status=0, stderr=""):
        with tempfile.TemporaryDirectory() as workdir:
            bin_dir = Path(workdir) / "bin"
            bin_dir.mkdir()
            stub = bin_dir / "gh"
            stub.write_text(STUB_GH, encoding="utf-8")
            stub.chmod(0o755)
            log = Path(workdir) / "argv"
            with mock.patch.dict(
                os.environ,
                {
                    "PATH": f"{bin_dir}:{os.environ['PATH']}",
                    "STUB_LOG": str(log),
                    "STUB_STDOUT": stdout,
                    "STUB_STDERR": stderr,
                    "STUB_STATUS": str(status),
                },
            ):
                yield log

    def argv(self, log):
        return log.read_text(encoding="utf-8").strip()

    def test_the_newest_run_is_parsed_from_the_run_list(self):
        payload = json.dumps(
            [{"databaseId": GREEN.id, "url": GREEN.url, "status": "completed", "conclusion": "success"}]
        )
        with self.with_stub(stdout=payload) as log:
            found = wait.newest_run(None, "deploy-prod.yml")
            sent = self.argv(log)
        self.assertEqual(found, GREEN)
        self.assertEqual(sent, " ".join(LIST_FIELDS))

    def test_a_workflow_with_no_runs_reads_as_none(self):
        with self.with_stub(stdout="[]"):
            self.assertIsNone(wait.newest_run(None, "deploy-prod.yml"))

    def test_one_run_is_read_by_id(self):
        payload = json.dumps(
            {"databaseId": RED.id, "url": RED.url, "status": "completed", "conclusion": "failure"}
        )
        with self.with_stub(stdout=payload) as log:
            found = wait.read_run(None, RED.id)
            sent = self.argv(log)
        self.assertEqual(found, RED)
        self.assertEqual(sent, f"run view {RED.id} --json {wait.FIELDS}")

    def test_the_dispatch_sends_the_inputs_and_the_ref(self):
        with self.with_stub() as log:
            wait.dispatch_run(
                None, "deploy-prod.yml", "main", ["server_version=0.6.0", "install_shape=true"]
            )
            sent = self.argv(log)
        self.assertEqual(
            sent,
            "workflow run deploy-prod.yml --ref main -f server_version=0.6.0 -f install_shape=true",
        )

    def test_a_named_repository_reaches_gh_as_repo(self):
        with self.with_stub(stdout="[]") as log:
            wait.newest_run("selvage-protocol/web_client", "deploy-prod.yml")
            sent = self.argv(log)
        self.assertTrue(sent.endswith("--repo selvage-protocol/web_client"), sent)

    def test_a_gh_that_exits_nonzero_is_an_error_that_carries_its_stderr(self):
        with self.with_stub(status=1, stderr="HTTP 404: not found"):
            with self.assertRaises(wait.GhError) as raised:
                wait.newest_run(None, "deploy-prod.yml")
        self.assertIn("HTTP 404", str(raised.exception))

    def test_a_missing_gh_is_an_error_rather_than_a_traceback(self):
        with mock.patch.object(wait, "GH", "/nonexistent/gh-for-the-test"):
            with self.assertRaises(wait.GhError) as raised:
                wait.newest_run(None, "deploy-prod.yml")
        self.assertIn("could not be run", str(raised.exception))

    def test_a_gh_that_never_answers_is_an_error(self):
        with tempfile.TemporaryDirectory() as workdir:
            bin_dir = Path(workdir) / "bin"
            bin_dir.mkdir()
            stub = bin_dir / "gh"
            stub.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(5)\n", encoding="utf-8")
            stub.chmod(0o755)
            with mock.patch.dict(os.environ, {"PATH": f"{bin_dir}:{os.environ['PATH']}"}):
                with mock.patch.object(wait, "GH_TIMEOUT_SECONDS", 0.05):
                    with self.assertRaises(wait.GhError) as raised:
                        wait.newest_run(None, "deploy-prod.yml")
        self.assertIn("did not answer", str(raised.exception))


class TheWordsTest(unittest.TestCase):
    def test_an_input_is_read_by_name(self):
        inputs = ["server_version=0.6.0", "install_shape=true"]
        self.assertEqual(wait.input_value(inputs, "server_version"), "0.6.0")
        self.assertEqual(wait.input_value(inputs, "install_shape"), "true")
        self.assertEqual(wait.input_value(inputs, "web_version"), "")

    def test_the_redispatch_is_the_dispatch_it_repeats(self):
        self.assertEqual(
            wait.redispatch_command("deploy-prod.yml", "main", ["server_version=0.6.0"]),
            "gh workflow run deploy-prod.yml --ref main -f server_version=0.6.0",
        )
        self.assertEqual(
            wait.redispatch_command(
                "deploy-prod.yml", "main", ["server_version=0.6.0", "install_shape=true"]
            ),
            "gh workflow run deploy-prod.yml --ref main -f server_version=0.6.0 -f install_shape=true",
        )

    def test_a_finished_run_names_its_conclusion_its_url_and_its_log(self):
        report = wait.conclusion_report(RED)
        self.assertIn("concluded failure", report)
        self.assertIn(RED.url, report)
        self.assertIn(f"gh run view {RED.id} --log-failed", report)

    def test_a_deadline_names_the_last_read(self):
        report = wait.did_not_finish_report("deploy-prod.yml run 36330000000", RUNNING, 1800.0)
        self.assertIn("did not finish within 1800s", report)
        self.assertIn(RUNNING.url, report)
        self.assertIn("status=in_progress", report)

    def test_what_remains_names_the_release_the_demo_and_the_redispatch(self):
        report = wait.remains(
            "deploy-prod.yml", "main", ["server_version=0.6.0", "install_shape=true"], None
        )
        self.assertIn("the release is already out", report)
        self.assertIn("v0.6.0 is tagged and published", report)
        self.assertIn("The public demo did not take it", report)
        self.assertIn("the next thing to read", report)
        self.assertIn(
            "gh workflow run deploy-prod.yml --ref main -f server_version=0.6.0 -f install_shape=true",
            report,
        )


class TheEndingTest(unittest.TestCase):
    """`main`'s endings, with the `gh`-facing reads injected."""

    def run_main(self, reads, dispatch=None, argv=None):
        argv = argv if argv is not None else ["--dispatch", "-f", "server_version=0.6.0"]
        argv += ["--deadline", "0.05", "--appear-deadline", "0.05", "--poll-interval", "0.01"]
        patches = [
            mock.patch.object(wait, "read_run", reads["read_run"]),
            mock.patch.object(wait, "newest_run", reads["newest_run"]),
        ]
        if dispatch is not None:
            patches.append(mock.patch.object(wait, "dispatch_run", dispatch))
        for patch in patches:
            patch.start()
        try:
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                code = wait.main(argv)
        finally:
            for patch in reversed(patches):
                patch.stop()
        return code, out.getvalue(), err.getvalue()

    @staticmethod
    def sequence(*runs):
        reads = list(runs)

        def read(*_):
            return reads.pop(0) if len(reads) > 1 else reads[0]

        return read

    def test_a_successful_dispatch_is_green_and_names_the_run(self):
        reads = {"newest_run": self.sequence(BEFORE, GREEN), "read_run": self.sequence(GREEN)}
        code, out, err = self.run_main(reads, dispatch=lambda *a: None)
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertIn(f"watching run {GREEN.id}: {GREEN.url}", out)
        self.assertIn(f"deploy-prod.yml run {GREEN.id} concluded success", out)

    def test_a_failed_deploy_fails_the_release_and_says_what_remains(self):
        reads = {"newest_run": self.sequence(BEFORE, RED), "read_run": self.sequence(RED)}
        code, out, err = self.run_main(
            reads, dispatch=lambda *a: None, argv=["--dispatch", "-f", "server_version=0.6.0", "-f", "install_shape=true"]
        )
        self.assertEqual(code, 1)
        self.assertIn(f"watching run {RED.id}", out)
        self.assertIn("concluded failure", err)
        self.assertIn(RED.url, err)
        self.assertIn(f"gh run view {RED.id} --log-failed", err)
        self.assertIn("the release is already out", err)
        self.assertIn("v0.6.0 is tagged and published", err)
        self.assertIn(
            "gh workflow run deploy-prod.yml --ref main -f server_version=0.6.0 -f install_shape=true",
            err,
        )

    def test_a_conclusion_that_is_not_success_names_itself(self):
        skipped = wait.Run(RED.id, RED.url, "completed", "skipped")
        reads = {"newest_run": self.sequence(BEFORE, skipped), "read_run": self.sequence(skipped)}
        code, _, err = self.run_main(reads, dispatch=lambda *a: None)
        self.assertEqual(code, 1)
        self.assertIn("concluded skipped", err)

    def test_a_run_that_does_not_finish_names_the_last_read(self):
        reads = {"newest_run": self.sequence(BEFORE, RUNNING), "read_run": self.sequence(RUNNING)}
        code, _, err = self.run_main(reads, dispatch=lambda *a: None)
        self.assertEqual(code, 1)
        self.assertIn("did not finish within 0s", err)
        self.assertIn(RUNNING.url, err)
        self.assertIn("status=in_progress", err)
        self.assertIn("What remains", err)

    def test_a_run_that_never_appears_names_what_remains(self):
        reads = {"newest_run": self.sequence(BEFORE, BEFORE), "read_run": self.sequence(BEFORE)}
        code, _, err = self.run_main(reads, dispatch=lambda *a: None)
        self.assertEqual(code, 1)
        self.assertIn("no new run of deploy-prod.yml appeared within 0s of the dispatch", err)
        self.assertIn("the one seen before the dispatch", err)
        self.assertIn("the release is already out", err)
        self.assertIn(
            "gh workflow run deploy-prod.yml --ref main -f server_version=0.6.0", err
        )

    def test_a_dispatch_that_gh_refuses_is_named_where_it_failed(self):
        reads = {"newest_run": self.sequence(BEFORE), "read_run": self.sequence(BEFORE)}

        def refuse(*_):
            raise wait.GhError("gh exited 1: workflow not found")

        code, _, err = self.run_main(reads, dispatch=refuse)
        self.assertEqual(code, 1)
        self.assertIn("could not dispatch deploy-prod.yml", err)
        self.assertIn("workflow not found", err)

    def test_watching_an_existing_run_needs_no_dispatch(self):
        reads = {"newest_run": self.sequence(BEFORE), "read_run": self.sequence(GREEN)}
        code, out, err = self.run_main(
            reads,
            argv=["--run-id", str(GREEN.id), "--repo", "selvage-protocol/web_client"],
        )
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertIn(f"watching run {GREEN.id}", out)

    def test_watching_an_existing_failed_run_names_it_and_its_log(self):
        reads = {"newest_run": self.sequence(BEFORE), "read_run": self.sequence(RED)}
        code, _, err = self.run_main(
            reads,
            argv=["--run-id", str(RED.id), "--repo", "selvage-protocol/web_client"],
        )
        self.assertEqual(code, 1)
        self.assertIn(f"run {RED.id} concluded failure", err)
        self.assertIn(RED.url, err)
        self.assertIn(
            f"gh run view {RED.id} --log-failed --repo selvage-protocol/web_client", err
        )
        self.assertNotIn("the release is already out", err)

    def test_watching_an_existing_run_that_is_not_finished_names_the_last_read(self):
        reads = {"newest_run": self.sequence(BEFORE), "read_run": self.sequence(RUNNING)}
        code, _, err = self.run_main(reads, argv=["--run-id", str(RUNNING.id)])
        self.assertEqual(code, 1)
        self.assertIn(f"run {RUNNING.id} did not finish within 0s", err)
        self.assertNotIn("deploy-prod.yml", err)
        self.assertIn("status=in_progress", err)

    def test_watching_for_the_next_run_without_a_dispatch(self):
        reads = {"newest_run": self.sequence(None), "read_run": self.sequence(RUNNING)}
        code, _, err = self.run_main(reads, argv=["--watch-next"])
        self.assertEqual(code, 1)
        self.assertIn("no runs of deploy-prod.yml are visible", err)

    def test_a_workflow_that_has_no_runs_is_its_own_read(self):
        reads = {"newest_run": self.sequence(None, None), "read_run": self.sequence(BEFORE)}
        code, _, err = self.run_main(reads, argv=["--watch-next"])
        self.assertEqual(code, 1)
        self.assertIn("no runs of deploy-prod.yml are visible", err)

    def test_a_watch_of_an_existing_workflow_names_the_run_it_saw(self):
        reads = {"newest_run": self.sequence(BEFORE, BEFORE), "read_run": self.sequence(BEFORE)}
        code, _, err = self.run_main(reads, argv=["--watch-next"])
        self.assertEqual(code, 1)
        self.assertIn("no new run of deploy-prod.yml appeared within 0s.", err)
        self.assertIn("the one seen before the watch began", err)
        self.assertNotIn("of the dispatch", err)

    def test_a_gh_that_errors_before_dispatching_is_named(self):
        def refuse(*_):
            raise wait.GhError("HTTP 404: workflow not found")

        reads = {"newest_run": refuse, "read_run": self.sequence(BEFORE)}
        code, _, err = self.run_main(reads, dispatch=lambda *a: None)
        self.assertEqual(code, 1)
        self.assertIn("cannot read deploy-prod.yml's runs before dispatching", err)

    def test_a_mode_is_required(self):
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                wait.main([])


if __name__ == "__main__":
    unittest.main()
