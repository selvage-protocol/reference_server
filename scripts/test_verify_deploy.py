"""What `verify_deploy.py` asserts, and what it only reports.

The two reads it separates cost a session to tell apart, so both are pinned here.

* The **origin**, read on the box through the front, is the assertion: a version
  that is not the one asked for, or a page that stops answering, fails the run and
  says what it saw.
* The **public** read, over an edge that answers a programmatic client on a
  datacenter address with a managed challenge, is a report in every shape it comes
  back in. The classifier is fed the response Cloudflare really sent — recorded
  from a runner — and the attempt is asserted to end on the first challenge rather
  than polling a deadline it cannot pass.

The poll loops are driven with real, tiny deadlines and injected reads, and the
one test that has to see several iterations stops its own loop from the fake read
rather than racing the wall clock; the two subprocess halves are driven against
stubs on `PATH`, which is where a mistake in an argument would otherwise only show
up on a runner.

Run it directly:

    python3 scripts/test_verify_deploy.py
"""

import io
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import verify_deploy as verify  # noqa: E402  (the path above is what makes this importable)

# The refusal Cloudflare serves a programmatic client on a datacenter address,
# as recorded from the box and from a GitHub runner. The read keeps the headers
# and the body, which is what makes this shape visible at all.
CHALLENGE_HEADERS = (
    "HTTP/2 403 \r\n"
    "date: Tue, 22 Sep 2026 06:21:30 GMT\r\n"
    "content-type: text/html; charset=UTF-8\r\n"
    "cf-mitigated: challenge\r\n"
    "server: cloudflare\r\n"
    "content-length: 5177\r\n"
)
CHALLENGE_BODY = (
    "<!DOCTYPE html><html><head><title>Just a moment...</title></head><body></body></html>"
)


class Enough(Exception):
    """Stops a poll loop from a fake read, so a test never waits on the clock."""


def meta(version):
    return '{"capabilities":["awareness"],"server":"selvaged/%s","wire_versions":["selvage/1"]}' % version


def public(status, body, headers="HTTP/2 200 \r\nserver: cloudflare\r\n"):
    return verify.Public(status=status, headers=headers, body=body)


def origin(page="200", version="0.2.1", error=""):
    return verify.Origin(page=page, meta=meta(version) if version else "", error=error)


class TheChallengeTest(unittest.TestCase):
    """The half that turned a healthy deploy red once."""

    def test_the_recorded_challenge_is_a_challenge(self):
        verdict, detail = verify.classify_public(public("403", CHALLENGE_BODY, CHALLENGE_HEADERS), "0.2.1")
        self.assertEqual(verdict, verify.CHALLENGE)
        self.assertIn("cf-mitigated: challenge", detail)
        self.assertIn("403", detail)

    def test_a_challenge_without_the_header_is_still_a_challenge(self):
        verdict, detail = verify.classify_public(
            public("403", CHALLENGE_BODY, "HTTP/2 403 \r\nserver: cloudflare\r\n"), "0.2.1"
        )
        self.assertEqual(verdict, verify.CHALLENGE)
        self.assertIn("interstitial", detail)

    def test_the_challenge_ends_the_attempt_rather_than_burning_the_deadline(self):
        calls = []

        def read():
            calls.append(1)
            return public("403", CHALLENGE_BODY, CHALLENGE_HEADERS)

        verdict, _ = verify.wait_for_public(read, "0.2.1", deadline=180.0, interval=5.0)
        self.assertEqual(verdict, verify.CHALLENGE)
        self.assertEqual(len(calls), 1, "a challenge is an answer, not a reason to keep polling")

    def test_a_challenge_after_a_retry_still_ends_them(self):
        reads = [public("502", "bad gateway", ""), public("403", CHALLENGE_BODY, CHALLENGE_HEADERS)]
        calls = []

        def read():
            calls.append(1)
            return reads[len(calls) - 1]

        verdict, _ = verify.wait_for_public(read, "0.2.1", deadline=60.0, interval=0.01)
        self.assertEqual(verdict, verify.CHALLENGE)
        self.assertEqual(len(calls), 2)


class ThePublicReadTest(unittest.TestCase):
    def test_a_read_that_gets_through_is_reported(self):
        verdict, detail = verify.classify_public(public("200", meta("0.2.1")), "0.2.1")
        self.assertEqual((verdict, detail), (verify.REPORTED, "selvaged/0.2.1"))

    def test_another_version_is_unreadable_rather_than_a_failure(self):
        verdict, detail = verify.classify_public(public("200", meta("0.2.0")), "0.2.1")
        self.assertEqual(verdict, verify.UNREADABLE)
        self.assertIn("selvaged/0.2.0", detail)

    def test_an_edge_that_never_answers_ends_at_the_deadline_with_what_it_said(self):
        calls = []

        def read():
            calls.append(1)
            return public("502", "bad gateway", "")

        verdict, detail = verify.wait_for_public(read, "0.2.1", deadline=0.05, interval=0.01)
        self.assertEqual(verdict, verify.UNREADABLE)
        self.assertIn("502", detail)
        self.assertGreaterEqual(len(calls), 1)

    def test_the_read_retries_before_it_reports(self):
        calls = []

        def read():
            calls.append(1)
            if len(calls) == 3:
                raise Enough
            return public("502", "bad gateway", "")

        with self.assertRaises(Enough):
            verify.wait_for_public(read, "0.2.1", deadline=60.0, interval=0.01)
        self.assertEqual(len(calls), 3, "a 502 is worth retrying; only a challenge is not")

    def test_a_read_that_never_arrives_is_unreadable_not_a_failure(self):
        unreadable = verify.Public("", "", "", "curl exited 6: Could not resolve host")
        verdict, detail = verify.wait_for_public(lambda: unreadable, "0.2.1", 0.03, 0.01)
        self.assertEqual(verdict, verify.UNREADABLE)
        self.assertIn("Could not resolve host", detail)

    def test_a_page_only_deploy_reads_the_public_version_without_comparing_it(self):
        verdict, detail = verify.classify_public(public("200", meta("0.1.0")), None)
        self.assertEqual((verdict, detail), (verify.REPORTED, "selvaged/0.1.0"))

    def test_a_200_without_a_server_field_is_not_an_answer(self):
        verdict, _ = verify.classify_public(public("200", "<html>nope</html>"), "0.2.1")
        self.assertEqual(verdict, verify.UNREADABLE)


class TheOriginTest(unittest.TestCase):
    def test_the_origin_must_serve_the_version_it_was_asked_for(self):
        with self.assertRaises(verify.OriginAssertion) as raised:
            verify.wait_for_origin(lambda: origin(version="0.2.0"), "0.2.1", 0.05, 0.01)
        message = str(raised.exception)
        self.assertIn("/meta reports selvaged/0.2.0", message)
        self.assertIn("the page answered 200", message)

    def test_it_recovers_when_the_origin_converges(self):
        readings = [origin(version="0.2.0"), origin(version="0.2.0"), origin(version="0.2.1")]
        calls = []

        def read():
            calls.append(1)
            return readings[min(len(calls), len(readings)) - 1]

        result = verify.wait_for_origin(read, "0.2.1", deadline=5.0, interval=0.01)
        self.assertEqual(verify.json_server(result.meta), "selvaged/0.2.1")
        self.assertEqual(len(calls), 3)

    def test_a_page_that_is_not_200_fails_the_run(self):
        with self.assertRaises(verify.OriginAssertion) as raised:
            verify.wait_for_origin(lambda: origin(page="502"), "0.2.1", 0.03, 0.01)
        self.assertIn("the page answered 502", str(raised.exception))

    def test_an_ssh_that_never_answers_fails_the_run(self):
        unreachable = verify.Origin("", "", "ssh to deployci@box exited 255: no route")
        with self.assertRaises(verify.OriginAssertion) as raised:
            verify.wait_for_origin(lambda: unreachable, "0.2.1", 0.03, 0.01)
        self.assertIn("exited 255", str(raised.exception))

    def test_a_web_only_deploy_reads_the_origin_version_without_comparing_it(self):
        reading = verify.wait_for_origin(lambda: origin(version="0.1.0"), None, 0.03, 0.01)
        self.assertEqual(verify.json_server(reading.meta), "selvaged/0.1.0")

    def test_meta_without_a_server_field_is_not_a_read(self):
        with self.assertRaises(verify.OriginAssertion) as raised:
            verify.wait_for_origin(
                lambda: verify.Origin("200", "<html>nope</html>"), "0.2.1", 0.03, 0.01
            )
        self.assertIn("no server field", str(raised.exception))


class TheEndingTest(unittest.TestCase):
    """`main`'s endings: the origin decides the colour, and the edge never does."""

    def run_main(self, origin_read, public_read, expect="0.2.1"):
        argv = ["--origin-deadline", "0.05", "--public-deadline", "0.05", "--poll-interval", "0.01"]
        if expect is not None:
            argv += ["--expect-version", expect]
        with mock.patch.object(verify, "read_origin", origin_read), mock.patch.object(
            verify, "read_public", public_read
        ):
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                code = verify.main(argv)
        return code, out.getvalue(), err.getvalue()

    def test_a_challenged_public_read_leaves_a_healthy_deploy_green(self):
        code, out, err = self.run_main(
            lambda *a: origin(version="0.2.1"),
            lambda *a: public("403", CHALLENGE_BODY, CHALLENGE_HEADERS),
        )
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertIn("selvaged/0.2.1", out)
        self.assertIn("not asserted", out)
        self.assertIn("Cloudflare", out)
        self.assertIn("cf-mitigated: challenge", out)
        self.assertIn("datacenter address", out)

    def test_a_public_read_that_is_not_a_challenge_is_reported_and_green(self):
        code, out, err = self.run_main(
            lambda *a: origin(version="0.2.1"), lambda *a: public("502", "bad gateway", "")
        )
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertIn("502", out)
        self.assertIn("did not fail the run", out)
        self.assertIn("selvaged/0.2.1", out)
        self.assertEqual(out.count("the origin:"), 1)

    def test_a_public_read_of_another_version_is_reported_and_green(self):
        code, out, err = self.run_main(
            lambda *a: origin(version="0.2.1"), lambda *a: public("200", meta("0.2.0"))
        )
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertIn("selvaged/0.2.0", out)

    def test_a_wrong_origin_is_red_before_the_public_read_happens(self):
        public_calls = []

        def never(*a):
            public_calls.append(1)
            return public("200", meta("0.2.1"))

        code, _, err = self.run_main(lambda *a: origin(version="0.2.0"), never)
        self.assertEqual(code, 1)
        self.assertEqual(public_calls, [])
        self.assertIn("selvaged/0.2.0", err)

    def test_a_page_only_dispatch_prints_the_origin_without_comparing(self):
        code, out, err = self.run_main(
            lambda *a: origin(version="0.1.0"), lambda *a: public("200", meta("0.1.0")), expect=None
        )
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertIn("not compared", out)


class TheSubprocessTest(unittest.TestCase):
    """The half that only a real argument list, or a missing binary, would break."""

    def stub(self, workdir, name, body):
        path = Path(workdir) / name
        path.write_text("#!/bin/sh\n" + body, encoding="utf-8")
        path.chmod(0o755)
        return path

    def test_the_ssh_read_parses_what_the_box_prints(self):
        with tempfile.TemporaryDirectory() as workdir:
            argv_file = Path(workdir) / "argv"
            bin_dir = Path(workdir) / "bin"
            bin_dir.mkdir()
            self.stub(
                bin_dir,
                "ssh",
                'printf "%s\\n" "$@" >"$STUB_ARGV"\n'
                "printf 'origin-page:%s\\n' 200\n"
                "printf 'origin-meta:%s\\n' '{\"server\":\"selvaged/0.2.1\"}'\n",
            )
            with mock.patch.dict(
                os.environ,
                {"PATH": f"{bin_dir}:{os.environ['PATH']}", "STUB_ARGV": str(argv_file)},
            ):
                reading = verify.read_origin("deployci@box", "https://127.0.0.1")
            self.assertEqual(reading, verify.Origin("200", '{"server":"selvaged/0.2.1"}'))
            sent = argv_file.read_text(encoding="utf-8")
            self.assertIn("deployci@box", sent)
            self.assertIn("BatchMode=yes", sent)
            self.assertIn("https://127.0.0.1/", sent)
            self.assertIn("https://127.0.0.1/meta", sent)

    def test_the_remote_command_carries_nothing_from_the_dispatch(self):
        command = verify.remote_read("https://127.0.0.1")
        self.assertIn("origin-page:", command)
        self.assertIn("origin-meta:", command)
        for foreign in ("0.2.1", "server_version", "SERVER_VERSION", "@VERSION@"):
            self.assertNotIn(foreign, command)

    def test_the_url_reaches_the_remote_shell_as_data(self):
        with tempfile.TemporaryDirectory() as workdir:
            bin_dir = Path(workdir) / "bin"
            bin_dir.mkdir()
            seen = Path(workdir) / "seen"
            pwned = Path(workdir) / "pwned"
            self.stub(bin_dir, "curl", f'printf "%s\\n" "$@" >"{seen}"\nprintf 200\n')
            hostile = f"https://127.0.0.1/$(touch {pwned});id"
            done = subprocess.run(
                ["/bin/sh", "-c", verify.remote_read(hostile)],
                capture_output=True,
                text=True,
                check=False,
                env=dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}"),
            )
            self.assertFalse(pwned.exists(), f"the remote shell executed the URL: {done.stdout}")
            self.assertIn(hostile + "/", seen.read_text(encoding="utf-8"))
            self.assertEqual(done.stdout.splitlines()[0], "origin-page:200")

    def test_an_ssh_that_refuses_is_a_read_that_fails(self):
        with tempfile.TemporaryDirectory() as workdir:
            bin_dir = Path(workdir) / "bin"
            bin_dir.mkdir()
            self.stub(bin_dir, "ssh", "echo 'no route to host' >&2\nexit 255\n")
            with mock.patch.dict(os.environ, {"PATH": f"{bin_dir}:{os.environ['PATH']}"}):
                reading = verify.read_origin("deployci@box", "https://127.0.0.1")
            self.assertEqual(reading.page, "")
            self.assertIn("exited 255", reading.error)
            self.assertIn("no route to host", reading.error)

    def test_a_missing_ssh_is_a_read_that_fails_rather_than_a_traceback(self):
        with tempfile.TemporaryDirectory() as empty:
            with mock.patch.dict(os.environ, {"PATH": empty}):
                reading = verify.read_origin("deployci@box", "https://127.0.0.1")
            self.assertEqual(reading.page, "")
            self.assertIn("could not be run", reading.error)

    def test_a_missing_curl_is_a_read_that_fails_rather_than_a_traceback(self):
        with tempfile.TemporaryDirectory() as empty:
            with mock.patch.dict(os.environ, {"PATH": empty}):
                reading = verify.read_public("https://selvage.dontblameme.dev")
            self.assertEqual(reading.status, "")
            self.assertIn("could not be run", reading.error)

    def test_the_public_read_keeps_the_headers_a_challenge_arrives_in(self):
        with tempfile.TemporaryDirectory() as workdir:
            bin_dir = Path(workdir) / "bin"
            bin_dir.mkdir()
            self.stub(
                bin_dir,
                "curl",
                "while [ $# -gt 0 ]; do case \"$1\" in -D) headers=$2; shift 2 ;; "
                "-o) body=$2; shift 2 ;; *) shift ;; esac; done\n"
                "printf 'HTTP/2 403 \\r\\ncf-mitigated: challenge\\r\\nserver: cloudflare\\r\\n' >\"$headers\"\n"
                "printf '%s' '<title>Just a moment...</title>' >\"$body\"\n"
                "printf '403'\n",
            )
            with mock.patch.dict(os.environ, {"PATH": f"{bin_dir}:{os.environ['PATH']}"}):
                reading = verify.read_public("https://selvage.dontblameme.dev")
            self.assertEqual(reading.status, "403")
            verdict, detail = verify.classify_public(reading, "0.2.1")
            self.assertEqual(verdict, verify.CHALLENGE)
            self.assertIn("cf-mitigated: challenge", detail)

    def test_a_transfer_that_fails_after_the_status_line_is_not_an_answer(self):
        with tempfile.TemporaryDirectory() as workdir:
            bin_dir = Path(workdir) / "bin"
            bin_dir.mkdir()
            self.stub(
                bin_dir,
                "curl",
                "while [ $# -gt 0 ]; do case \"$1\" in -D) headers=$2; shift 2 ;; "
                "-o) body=$2; shift 2 ;; *) shift ;; esac; done\n"
                "printf 'HTTP/2 200 \\r\\nserver: cloudflare\\r\\n' >\"$headers\"\n"
                "printf '%s' '{\"server\":\"selvaged/0.2.1\"}' >\"$body\"\n"
                "printf '200'\nexit 28\n",
            )
            with mock.patch.dict(os.environ, {"PATH": f"{bin_dir}:{os.environ['PATH']}"}):
                reading = verify.read_public("https://selvage.dontblameme.dev")
            self.assertIn("exited 28", reading.error)
            verdict, _ = verify.classify_public(reading, "0.2.1")
            self.assertEqual(verdict, verify.UNREADABLE)


if __name__ == "__main__":
    unittest.main(verbosity=2)
