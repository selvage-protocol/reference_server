"""The guard around `deploy.py`, which is the whole of the box's privilege model.

`/usr/local/sbin/selvage-deploy` is the only command `deployci` may run as root
(`deployci.sudoers`), so the request it reads on stdin is the only lever anything
holding a CI credential has on production. What carries that weight is asserted
here rather than argued:

1. **Only a tag of the two published images, and nothing else.** Every value a
   request may carry is matched against one fixed pattern, so most tests below are
   the shapes one might *hope* a script would refuse: the sibling repository under
   the wrong key, another registry, a shell metacharacter, a traversal, a control
   character, a duplicate key.
2. **Only the requested lines of `.env` move.** Everything else in the file is the
   owner's and comes through byte for byte.
3. **A deploy never runs compose beside the update timer**, and a failed one leaves
   `.env` naming what it named.

The rest is the tracked shape and units the script is paired with: the variables
the compose file interpolates are the ones the script writes, the front is not
rebuilt by a plain `up`, and the timer takes the lock the script takes.

Run it directly:

    python3 deploy/test_deploy.py

or as the flake check the workflows run: `nix build .#checks.<system>.prod-deploy`.
"""

import contextlib
import fcntl
import io
import os
import re
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import deploy  # noqa: E402  (the path above is what makes this importable)

SELVAGED = "ghcr.io/selvage-protocol/selvaged:0.4.6"
WEB = "ghcr.io/selvage-protocol/selvage-web:latest"


def request(*lines: str) -> bytes:
    return ("\n".join(lines) + "\n").encode()


class ParseRequestTest(unittest.TestCase):
    def test_both_images(self):
        parsed = deploy.parse_request(request(f"SELVAGED_IMAGE={SELVAGED}", f"SELVAGE_WEB_IMAGE={WEB}"))
        self.assertEqual(parsed, {"SELVAGED_IMAGE": SELVAGED, "SELVAGE_WEB_IMAGE": WEB})

    def test_one_image_alone_is_a_request(self):
        parsed = deploy.parse_request(request(f"SELVAGED_IMAGE={SELVAGED}"))
        self.assertNotIn("SELVAGE_WEB_IMAGE", parsed)

    def test_the_trailing_newline_is_optional(self):
        without = deploy.parse_request(f"SELVAGED_IMAGE={SELVAGED}".encode())
        self.assertEqual(without["SELVAGED_IMAGE"], SELVAGED)

    def test_a_request_that_names_no_image_deploys_nothing(self):
        for raw in (b"", b"\n"):
            with self.subTest(raw=raw), self.assertRaises(deploy.Refused):
                deploy.parse_request(raw)

    def test_the_tags_a_release_publishes_are_accepted(self):
        for tag in ("latest", "0.4.6", "0.4.6-97ff967", "1.0.0-rc.1", "v2_x"):
            with self.subTest(tag=tag):
                value = f"ghcr.io/selvage-protocol/selvaged:{tag}"
                self.assertEqual(deploy.parse_request(request(f"SELVAGED_IMAGE={value}"))["SELVAGED_IMAGE"], value)

    def test_a_tag_outside_the_grammar_is_refused(self):
        for tag in ("", ".hidden", "-flag", "a" * 129, "a/b", "a:b", "a b", "0.4.6 ", "$(id)", "a#b", "é"):
            with self.subTest(tag=tag), self.assertRaisesRegex(deploy.Refused, "SELVAGED_IMAGE"):
                deploy.parse_request(request(f"SELVAGED_IMAGE=ghcr.io/selvage-protocol/selvaged:{tag}"))

    def test_a_digest_or_a_bare_repository_is_not_a_tag(self):
        for value in (
            "ghcr.io/selvage-protocol/selvaged@sha256:" + "a" * 64,
            "ghcr.io/selvage-protocol/selvaged:0.4.6@sha256:" + "a" * 64,
            "ghcr.io/selvage-protocol/selvaged",
            "0.4.6",
        ):
            with self.subTest(value=value), self.assertRaisesRegex(deploy.Refused, "SELVAGED_IMAGE"):
                deploy.parse_request(request(f"SELVAGED_IMAGE={value}"))

    def test_a_reference_cannot_name_the_other_key_s_repository(self):
        with self.assertRaisesRegex(deploy.Refused, "SELVAGED_IMAGE"):
            deploy.parse_request(request(f"SELVAGED_IMAGE={WEB}"))

    def test_a_reference_cannot_leave_this_registry(self):
        for value in (
            "ghcr.io/selvage-protocol-elsewhere/selvaged:latest",
            "ghcr.io/other/selvaged:latest",
            "docker.io/selvage-protocol/selvaged:latest",
            "localhost/selvage-protocol/selvaged:latest",
            "selvaged:latest",
            "ghcr.io/selvage-protocol/../other/selvaged:latest",
        ):
            with self.subTest(value=value), self.assertRaisesRegex(deploy.Refused, "SELVAGED_IMAGE"):
                deploy.parse_request(request(f"SELVAGED_IMAGE={value}"))

    def test_a_shell_metacharacter_is_not_a_tag(self):
        for suffix in (";id", "$(id)", "`id`", "&&id", "|id", "'", '"', "${HOME}"):
            with self.subTest(suffix=suffix), self.assertRaises(deploy.Refused):
                deploy.parse_request(request(f"SELVAGED_IMAGE={SELVAGED}{suffix}"))

    def test_a_newline_in_a_value_is_a_second_line_and_nothing_more(self):
        """The grammar is per line, so there is no value that can smuggle one in."""
        parsed = deploy.parse_request(request(f"SELVAGED_IMAGE={SELVAGED}\nSELVAGE_WEB_IMAGE={WEB}"))
        self.assertEqual(parsed, {"SELVAGED_IMAGE": SELVAGED, "SELVAGE_WEB_IMAGE": WEB})
        with self.assertRaisesRegex(deploy.Refused, "not a key this box acts on"):
            deploy.parse_request(request(f"SELVAGED_IMAGE={SELVAGED}\nCOMPOSE_CMD=id"))

    def test_a_key_this_box_does_not_act_on_is_refused_rather_than_ignored(self):
        for line in ("SELVAGE_EXTRA_IMAGE=x", "COMPOSE_SHA256=" + "c" * 64, "COMPOSE_PROJECT_NAME=x"):
            with self.subTest(line=line), self.assertRaisesRegex(deploy.Refused, "not a key this box acts on"):
                deploy.parse_request(request(f"SELVAGED_IMAGE={SELVAGED}", line))

    def test_a_duplicate_key_is_refused(self):
        with self.assertRaisesRegex(deploy.Refused, "appears twice"):
            deploy.parse_request(request(f"SELVAGED_IMAGE={SELVAGED}", f"SELVAGED_IMAGE={SELVAGED}"))

    def test_the_grammar_has_no_room_for_a_parser_difference(self):
        for raw, why in (
            (b"SELVAGED_IMAGE=" + SELVAGED.encode() + b"\r\nSELVAGE_WEB_IMAGE=" + WEB.encode(), "CRLF"),
            (b"\nSELVAGED_IMAGE=" + SELVAGED.encode(), "a leading blank line"),
            (b"SELVAGED_IMAGE=" + SELVAGED.encode() + b"\n\nSELVAGE_WEB_IMAGE=" + WEB.encode(), "a blank line"),
            (b"SELVAGED_IMAGE=" + SELVAGED.encode() + b"\tSELVAGE_WEB_IMAGE=" + WEB.encode(), "a tab"),
            (b"SELVAGED_IMAGE = " + SELVAGED.encode(), "spaces around the separator"),
            (b"selvaged_image=" + SELVAGED.encode(), "a lower-case key"),
            (b"SELVAGED-IMAGE=" + SELVAGED.encode(), "a dashed key"),
            (b"SELVAGED_IMAGE" + SELVAGED.encode(), "no separator"),
            (b"SELVAGED_IMAGE=" + SELVAGED.encode() + b"\x00", "a NUL"),
            (b"\xff\xfe", "bytes that are not UTF-8"),
        ):
            with self.subTest(why=why), self.assertRaises(deploy.Refused):
                deploy.parse_request(raw)

    def test_an_oversized_request_is_refused_before_it_is_parsed(self):
        body = (f"SELVAGED_IMAGE={SELVAGED}\n" * 200).encode()
        self.assertGreater(len(body), deploy.MAX_REQUEST_BYTES)
        with self.assertRaisesRegex(deploy.Refused, "longer than"):
            deploy.parse_request(body)


class RewriteEnvTest(unittest.TestCase):
    HAND = (
        "# the owner's note\n"
        "\n"
        "SELVAGED_IMAGE=ghcr.io/selvage-protocol/selvaged:latest\n"
        "SELVAGE_WEB_IMAGE=ghcr.io/selvage-protocol/selvage-web@sha256:" + "b" * 64 + "\n"
        "OTHER=1\n"
    )

    def test_only_the_requested_line_moves(self):
        rewritten = deploy.rewrite_env(self.HAND, {"SELVAGED_IMAGE": SELVAGED})
        self.assertEqual(rewritten, self.HAND.replace("selvaged:latest", "selvaged:0.4.6"))

    def test_a_missing_key_is_appended(self):
        rewritten = deploy.rewrite_env("# only a comment\n", {"SELVAGE_WEB_IMAGE": WEB})
        self.assertEqual(rewritten, f"# only a comment\nSELVAGE_WEB_IMAGE={WEB}\n")

    def test_a_file_without_a_final_newline_gains_one(self):
        self.assertEqual(
            deploy.rewrite_env("OTHER=1", {"SELVAGED_IMAGE": SELVAGED}),
            f"OTHER=1\nSELVAGED_IMAGE={SELVAGED}\n",
        )

    def test_every_line_for_the_key_moves_and_a_commented_one_does_not(self):
        text = "# SELVAGED_IMAGE=old\nSELVAGED_IMAGE=a\n SELVAGED_IMAGE = b\n"
        rewritten = deploy.rewrite_env(text, {"SELVAGED_IMAGE": SELVAGED})
        self.assertEqual(rewritten, f"# SELVAGED_IMAGE=old\nSELVAGED_IMAGE={SELVAGED}\nSELVAGED_IMAGE={SELVAGED}\n")


class FilesTest(unittest.TestCase):
    def test_the_file_it_replaced_is_kept_and_both_belong_to_the_directory_s_owner(self):
        """Ownership is read from the calls: a non-root run owns every file it makes anyway."""
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            previous = Path(directory) / ".env.prev"
            env_file.write_text("SELVAGED_IMAGE=old\n")
            owned = []
            with mock.patch.multiple(deploy, ENV_FILE=env_file, ENV_PREV=previous), mock.patch(
                "os.fchown", lambda _fd, uid, gid: owned.append((uid, gid))
            ):
                self.assertTrue(deploy.record({"SELVAGED_IMAGE": SELVAGED}))
            self.assertEqual(previous.read_text(), "SELVAGED_IMAGE=old\n")
            owner = os.stat(directory)
            self.assertEqual(owned, [(owner.st_uid, owner.st_gid)] * 2)
            for path in (env_file, previous):
                self.assertEqual(path.stat().st_mode & 0o777, 0o600, path)
            self.assertEqual(env_file.read_text(), f"SELVAGED_IMAGE={SELVAGED}\n")
            self.assertEqual(sorted(p.name for p in Path(directory).iterdir()), [".env", ".env.prev"])

    def test_an_unchanged_file_is_not_rewritten(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            previous = Path(directory) / ".env.prev"
            previous.write_text("the last real previous state\n")
            env_file.write_text(f"SELVAGED_IMAGE={SELVAGED}\n")
            with mock.patch.multiple(deploy, ENV_FILE=env_file, ENV_PREV=previous):
                self.assertFalse(deploy.record({"SELVAGED_IMAGE": SELVAGED}))
            self.assertEqual(previous.read_text(), "the last real previous state\n")

    def test_a_linked_env_is_neither_read_nor_written_through(self):
        with tempfile.TemporaryDirectory() as directory:
            elsewhere = Path(directory) / "elsewhere"
            elsewhere.write_text("not the deployment's\n")
            env_file = Path(directory) / ".env"
            env_file.symlink_to(elsewhere)
            with mock.patch.multiple(deploy, ENV_FILE=env_file), self.assertRaises(OSError):
                deploy.read_env()
            deploy.atomic_write(env_file, "written\n")
            self.assertFalse(env_file.is_symlink())
            self.assertEqual(elsewhere.read_text(), "not the deployment's\n")

    def test_a_missing_env_reads_as_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.multiple(deploy, ENV_FILE=Path(directory) / ".env"):
                self.assertEqual(deploy.read_env(), "")


class MainTest(unittest.TestCase):
    """`main()` itself, and what it does before docker is ever reached."""

    def run_main(self, request_bytes, **paths):
        stdin = types.SimpleNamespace(buffer=io.BytesIO(request_bytes))
        with mock.patch.object(sys, "stdin", stdin), mock.patch.object(
            deploy, "compose", lambda *a: self.fail(f"docker compose {a} was called")
        ), mock.patch.object(deploy, "run", lambda *a: self.fail("a docker command was run")), mock.patch(
            "os.geteuid", return_value=0
        ), mock.patch.multiple(deploy, **paths) if paths else contextlib.nullcontext():
            return deploy.main(["selvage-deploy"])

    def test_arguments_are_refused(self):
        self.assertEqual(deploy.main(["selvage-deploy", "--rollback"]), 2)
        self.assertEqual(deploy.main(["selvage-deploy", SELVAGED]), 2)

    def test_a_refused_request_never_reaches_docker(self):
        self.assertEqual(self.run_main(b"SELVAGED_IMAGE=latest\n"), 1)

    def test_a_deploy_waits_for_the_timer_and_gives_up_without_touching_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            srv = Path(directory)
            (srv / ".env").write_text(f"SELVAGED_IMAGE={SELVAGED}\n")
            lock = srv / ".update.lock"
            with open(lock, "w") as held, mock.patch.multiple(
                deploy, SRV=srv, ENV_FILE=srv / ".env", ENV_STAGED=srv / ".env.deploy", LOCK=lock,
                LOCK_WAIT_SECONDS=0.0,
            ):
                fcntl.flock(held, fcntl.LOCK_EX)
                self.assertEqual(self.run_main(request(f"SELVAGED_IMAGE={WEB.replace('selvage-web', 'selvaged')}")), 1)
            self.assertFalse((srv / ".env.deploy").exists())
            self.assertEqual((srv / ".env").read_text(), f"SELVAGED_IMAGE={SELVAGED}\n")

    def test_a_linked_lock_is_neither_followed_nor_created_through(self):
        """Root opens the lock in a directory `selvage` owns, so a link there is refused."""
        for existing in (True, False):
            with self.subTest(existing=existing), tempfile.TemporaryDirectory() as directory:
                srv = Path(directory)
                target = srv / "elsewhere"
                if existing:
                    target.write_text("not the lock\n")
                (srv / ".update.lock").symlink_to(target)
                status = self.run_main(
                    request(f"SELVAGED_IMAGE={SELVAGED}"),
                    SRV=srv, ENV_FILE=srv / ".env", ENV_STAGED=srv / ".env.deploy", LOCK=srv / ".update.lock",
                )
                self.assertEqual(status, 1)
                if existing:
                    self.assertEqual(target.read_text(), "not the lock\n")
                else:
                    self.assertFalse(target.exists())
                self.assertFalse((srv / ".env.deploy").exists())


class StagedDeployTest(unittest.TestCase):
    """A failed deploy must not move the box's record of what it intends to run.

    `.env` is what the next timer tick and every hand `up -d` read, so the new
    references go into `.env.deploy` first, and `.env` is written only once the
    containers run what was asked for.
    """

    OTHER = "ghcr.io/selvage-protocol/selvaged:0.4.7"
    BEFORE = f"# kept\nSELVAGED_IMAGE={SELVAGED}\nSELVAGE_WEB_IMAGE={WEB}\nEXTRA=1\n"

    def deploy(self, srv, request_bytes, compose_stub, run_stub):
        stdin = types.SimpleNamespace(buffer=io.BytesIO(request_bytes))
        with mock.patch.multiple(
            deploy,
            SRV=srv,
            ENV_FILE=srv / ".env",
            ENV_PREV=srv / ".env.prev",
            ENV_STAGED=srv / ".env.deploy",
            LOCK=srv / ".update.lock",
        ), mock.patch.object(sys, "stdin", stdin), mock.patch.object(
            deploy, "compose", compose_stub
        ), mock.patch.object(deploy, "run", run_stub), mock.patch("os.geteuid", return_value=0):
            return deploy.main(["selvage-deploy"])

    def compose_failing(self, verb, asked):
        def stub(env_file, *arguments):
            asked.append((env_file, arguments))
            return subprocess.CompletedProcess([], 1 if arguments[:1] == (verb,) else 0, "", "")

        return stub

    def run_converged(self, argv):
        running = "sha256:" + "e" * 64
        if argv[:2] == ["docker", "image"]:
            return subprocess.CompletedProcess(argv, 0, running + "\n", "")
        if argv[:2] == ["docker", "inspect"]:
            return subprocess.CompletedProcess(argv, 0, f"{running}|running\n", "")
        return subprocess.CompletedProcess(argv, 0, "one\n", "")

    def test_a_failed_pull_or_up_leaves_env_alone(self):
        for verb in ("pull", "up"):
            with self.subTest(verb=verb), tempfile.TemporaryDirectory() as directory:
                srv = Path(directory)
                (srv / ".env").write_text(self.BEFORE)
                asked = []
                status = self.deploy(
                    srv,
                    request(f"SELVAGED_IMAGE={self.OTHER}"),
                    self.compose_failing(verb, asked),
                    self.run_converged,
                )
                self.assertEqual(status, 1)
                self.assertEqual((srv / ".env").read_text(), self.BEFORE)
                self.assertFalse((srv / ".env.prev").exists())
                self.assertIn(self.OTHER, (srv / ".env.deploy").read_text())
                self.assertEqual({env for env, _ in asked}, {srv / ".env.deploy"})

    def test_a_deploy_that_does_not_converge_leaves_env_alone(self):
        with tempfile.TemporaryDirectory() as directory:
            srv = Path(directory)
            (srv / ".env").write_text(self.BEFORE)

            def run_stub(argv):
                if argv[:2] == ["docker", "inspect"]:
                    return subprocess.CompletedProcess(argv, 0, "sha256:old|running\n", "")
                return self.run_converged(argv)

            status = self.deploy(srv, request(f"SELVAGED_IMAGE={self.OTHER}"), self.compose_failing("none", []), run_stub)
            self.assertEqual(status, 1)
            self.assertEqual((srv / ".env").read_text(), self.BEFORE)

    def test_a_service_that_is_not_running_leaves_env_alone(self):
        def exited(argv):
            if argv[:2] == ["docker", "inspect"]:
                return subprocess.CompletedProcess(argv, 0, "sha256:" + "e" * 64 + "|exited\n", "")
            return self.run_converged(argv)

        def missing(argv):
            if argv[:2] == ["docker", "compose"] and "ps" in argv:
                return subprocess.CompletedProcess(argv, 0, "\n", "")
            return self.run_converged(argv)

        for why, run_stub in (("proxy is exited", exited), ("proxy is missing", missing)):
            with self.subTest(why=why), tempfile.TemporaryDirectory() as directory, mock.patch(
                "sys.stderr", new_callable=io.StringIO
            ) as stderr:
                srv = Path(directory)
                (srv / ".env").write_text(self.BEFORE)
                status = self.deploy(srv, request(f"SELVAGED_IMAGE={self.OTHER}"), self.compose_failing("none", []), run_stub)
                self.assertEqual(status, 1)
                self.assertIn(f"refused: {why} after `up -d`", stderr.getvalue())
                self.assertEqual((srv / ".env").read_bytes(), self.BEFORE.encode())
                self.assertFalse((srv / ".env.prev").exists())

    def test_an_env_that_is_not_utf8_is_refused_and_left_alone(self):
        with tempfile.TemporaryDirectory() as directory:
            srv = Path(directory)
            (srv / ".env").write_bytes(b"SELVAGED_IMAGE=\xff\n")
            asked = []
            status = self.deploy(srv, request(f"SELVAGED_IMAGE={self.OTHER}"), self.compose_failing("none", asked), self.run_converged)
            self.assertEqual(status, 1)
            self.assertEqual((srv / ".env").read_bytes(), b"SELVAGED_IMAGE=\xff\n")
            self.assertEqual(asked, [])

    def test_a_hand_edit_made_during_the_deploy_survives_it(self):
        """Hand commands do not take the lock, so `.env` can move while `up` runs."""
        pinned = self.BEFORE.replace(WEB, "ghcr.io/selvage-protocol/selvage-web:0.4.5")
        with tempfile.TemporaryDirectory() as directory:
            srv = Path(directory)
            (srv / ".env").write_text(self.BEFORE)

            def compose_stub(env_file, *arguments):
                if arguments[:1] == ("up",):
                    (srv / ".env").write_text(pinned)
                return subprocess.CompletedProcess([], 0, "", "")

            status = self.deploy(srv, request(f"SELVAGED_IMAGE={self.OTHER}"), compose_stub, self.run_converged)
            self.assertEqual(status, 0)
            self.assertEqual((srv / ".env").read_text(), pinned.replace(SELVAGED, self.OTHER))
            self.assertEqual((srv / ".env.prev").read_text(), pinned)

    def test_a_converged_deploy_moves_one_line_and_clears_the_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            srv = Path(directory)
            (srv / ".env").write_text(self.BEFORE)
            asked = []
            status = self.deploy(
                srv, request(f"SELVAGED_IMAGE={self.OTHER}"), self.compose_failing("none", asked), self.run_converged
            )
            self.assertEqual(status, 0)
            self.assertEqual((srv / ".env").read_text(), self.BEFORE.replace(SELVAGED, self.OTHER))
            self.assertEqual((srv / ".env.prev").read_text(), self.BEFORE)
            self.assertFalse((srv / ".env.deploy").exists())
            self.assertEqual([arguments for _, arguments in asked][:2], [("pull", "selvaged"), ("up", "-d")])


class TrackedShapeTest(unittest.TestCase):
    """The compose file and the units the script is installed beside."""

    def setUp(self):
        self.compose = "\n".join(
            line
            for line in (HERE / "compose.yaml").read_text(encoding="utf-8").split("\n")
            if not line.lstrip().startswith("#")
        )
        self.service = (HERE / "selvage-update.service").read_text(encoding="utf-8")

    def test_the_shape_interpolates_exactly_the_variables_the_deploy_writes(self):
        variables = set(re.findall(r"\$\{([A-Z_][A-Z0-9_]*)", self.compose))
        self.assertEqual(variables, set(deploy.PINS))

    def test_the_shape_names_exactly_the_services_the_deploy_checks(self):
        services = set(re.findall(r"^  ([a-z][a-z0-9_-]*):$", self.compose, re.MULTILINE))
        self.assertEqual(services, set(deploy.SERVICES))

    def test_a_plain_up_does_not_rebuild_the_front(self):
        self.assertIn("build:", self.compose)
        self.assertNotIn("pull_policy", self.compose)

    def test_the_directory_is_self_contained(self):
        self.assertNotRegex(self.compose, r"\n\s+- /", "a host path outside the project directory")
        self.assertNotRegex(self.compose, r"source:\s*/", "a host path outside the project directory")
        self.assertIn("- ./tls/origin.key:", self.compose)

    def test_the_timer_takes_the_lock_the_deploy_takes(self):
        self.assertIn(f"flock --nonblock --conflict-exit-code 0 {deploy.LOCK} ", self.service)
        self.assertIn(f"WorkingDirectory={deploy.SRV}\n", self.service)
        self.assertIn("User=selvage\n", self.service)

    def test_a_tick_never_starts_a_docker_the_owner_stopped(self):
        self.assertIn("Requisite=docker.service\n", self.service)
        self.assertNotRegex(self.service, r"(?m)^Requires=")

    def test_a_deploy_loads_compose_the_way_the_timer_and_the_owner_do(self):
        """In `SRV` with no `-f`, so a `compose.override.yaml` there applies to all three."""
        self.assertNotRegex(self.service, r"docker compose -f|--project-directory")
        called = []
        with mock.patch.object(deploy, "SRV", Path("/srv/elsewhere")), mock.patch(
            "subprocess.run",
            lambda argv, **options: called.append((argv, options.get("cwd"))) or subprocess.CompletedProcess(argv, 0, "", ""),
        ):
            deploy.compose(deploy.ENV_STAGED, "up", "-d")
            deploy.service_state("selvaged", deploy.ENV_STAGED)
        self.assertEqual({cwd for _, cwd in called}, {Path("/srv/elsewhere")})
        for argv, _ in called:
            self.assertNotIn("-f", argv)
            self.assertNotIn("--project-directory", argv)

    def test_the_timer_skips_the_front_and_prunes(self):
        self.assertIn("docker compose pull --ignore-buildable", self.service)
        self.assertIn("docker image prune --force", self.service)


if __name__ == "__main__":
    unittest.main(verbosity=2)
