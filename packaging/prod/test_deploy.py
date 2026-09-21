"""The guard around `deploy.py`, which is the whole of the box's privilege model.

`/usr/local/sbin/selvage-deploy` is the only command `deployci` may run as root
(`deployci.sudoers`), so the request it reads on stdin is the only lever anything
holding a CI credential has on production. Two properties carry that weight and
both are asserted here rather than argued:

1. **Only the two published images, by digest, and nothing else.** Every value a
   request may carry is matched against one fixed pattern. There is no path, no
   flag, no shell word and no second repository in the grammar, so the tests below
   are mostly the shapes one might *hope* a script would refuse: a tag that the
   publisher can repoint, the sibling repository under the wrong key, a reference
   with a shell metacharacter, a traversal, a control character, a duplicate key.
2. **A shape mismatch stops the deploy before a container is touched.**
   `test_a_shape_mismatch_never_reaches_docker` is that one: `docker compose` is
   replaced by a stub that fails the test if it is called at all.

The rest is the small bookkeeping the run depends on — which reference survives an
omitted key, what `.env.prev` ends up holding, and how a container's before/after
state is turned into the sentence the run reports.

Run it directly:

    python3 packaging/prod/test_deploy.py

or as the flake check the workflows run: `nix build .#checks.<system>.prod-deploy`.
"""

import hashlib
import io
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import deploy  # noqa: E402  (the path above is what makes this importable)

SELVAGED = "ghcr.io/selvage-protocol/selvaged@sha256:" + "a" * 64
WEB = "ghcr.io/selvage-protocol/selvage-web@sha256:" + "b" * 64
SHAPE = "c" * 64


def request(*lines: str) -> bytes:
    return ("\n".join(lines) + "\n").encode()


class ParseRequestTest(unittest.TestCase):
    def test_both_images_and_the_shape(self):
        parsed = deploy.parse_request(
            request(f"COMPOSE_SHA256={SHAPE}", f"SELVAGED_IMAGE={SELVAGED}", f"SELVAGE_WEB_IMAGE={WEB}")
        )
        self.assertEqual(
            parsed,
            {"COMPOSE_SHA256": SHAPE, "SELVAGED_IMAGE": SELVAGED, "SELVAGE_WEB_IMAGE": WEB},
        )

    def test_one_image_alone_is_a_request(self):
        parsed = deploy.parse_request(request(f"COMPOSE_SHA256={SHAPE}", f"SELVAGED_IMAGE={SELVAGED}"))
        self.assertNotIn("SELVAGE_WEB_IMAGE", parsed)

    def test_the_trailing_newline_is_optional(self):
        without = deploy.parse_request(f"COMPOSE_SHA256={SHAPE}\nSELVAGED_IMAGE={SELVAGED}".encode())
        self.assertEqual(without["SELVAGED_IMAGE"], SELVAGED)

    def test_the_shape_is_not_optional(self):
        with self.assertRaisesRegex(deploy.Refused, "COMPOSE_SHA256 is required"):
            deploy.parse_request(request(f"SELVAGED_IMAGE={SELVAGED}"))

    def test_a_request_that_names_no_image_deploys_nothing(self):
        with self.assertRaisesRegex(deploy.Refused, "no image named"):
            deploy.parse_request(request(f"COMPOSE_SHA256={SHAPE}"))

    def test_a_tag_is_refused(self):
        for value in (
            "ghcr.io/selvage-protocol/selvaged:0.2.0",
            "ghcr.io/selvage-protocol/selvaged:latest",
            "ghcr.io/selvage-protocol/selvaged@sha256:" + "a" * 63,
            "ghcr.io/selvage-protocol/selvaged@sha256:" + "A" * 64,
            "ghcr.io/selvage-protocol/selvaged@sha256:" + "a" * 64 + " ",
            "ghcr.io/selvage-protocol/selvaged@sha512:" + "a" * 64,
        ):
            with self.subTest(value=value), self.assertRaisesRegex(deploy.Refused, "SELVAGED_IMAGE"):
                deploy.parse_request(request(f"COMPOSE_SHA256={SHAPE}", f"SELVAGED_IMAGE={value}"))

    def test_a_reference_cannot_name_the_other_key_s_repository(self):
        with self.assertRaisesRegex(deploy.Refused, "SELVAGED_IMAGE"):
            deploy.parse_request(request(f"COMPOSE_SHA256={SHAPE}", f"SELVAGED_IMAGE={WEB}"))

    def test_a_reference_cannot_leave_this_registry(self):
        for value in (
            "ghcr.io/selvage-protocol-elsewhere/selvaged@sha256:" + "a" * 64,
            "ghcr.io/other/selvaged@sha256:" + "a" * 64,
            "docker.io/selvage-protocol/selvaged@sha256:" + "a" * 64,
            "localhost/selvage-protocol/selvaged@sha256:" + "a" * 64,
        ):
            with self.subTest(value=value), self.assertRaisesRegex(deploy.Refused, "SELVAGED_IMAGE"):
                deploy.parse_request(request(f"COMPOSE_SHA256={SHAPE}", f"SELVAGED_IMAGE={value}"))

    def test_a_shell_metacharacter_is_not_a_digest(self):
        for value in (
            SELVAGED + ";id",
            SELVAGED + "$(id)",
            SELVAGED + "`id`",
            SELVAGED + "&&id",
            SELVAGED.replace("ghcr.io/selvage-protocol/", "ghcr.io/selvage-protocol/../../"),
        ):
            with self.subTest(value=value), self.assertRaises(deploy.Refused):
                deploy.parse_request(request(f"COMPOSE_SHA256={SHAPE}", f"SELVAGED_IMAGE={value}"))

    def test_a_newline_in_a_value_is_a_second_line_and_nothing_more(self):
        """The grammar is per line, so there is no value that can smuggle one in.

        A newline in a value is not an extra argument to anything: it ends the line,
        and what follows is a request line in its own right, judged on its own. This
        is the shape that would be an injection in a shell or an `eval`.
        """
        parsed = deploy.parse_request(
            request(f"COMPOSE_SHA256={SHAPE}", f"SELVAGED_IMAGE={SELVAGED}\nSELVAGE_WEB_IMAGE={WEB}")
        )
        self.assertEqual(
            parsed,
            {"COMPOSE_SHA256": SHAPE, "SELVAGED_IMAGE": SELVAGED, "SELVAGE_WEB_IMAGE": WEB},
        )
        with self.assertRaisesRegex(deploy.Refused, "not a key this box acts on"):
            deploy.parse_request(
                request(f"COMPOSE_SHA256={SHAPE}", f"SELVAGED_IMAGE={SELVAGED}\nCOMPOSE_CMD=id")
            )

    def test_a_key_this_box_does_not_act_on_is_refused_rather_than_ignored(self):
        with self.assertRaisesRegex(deploy.Refused, "not a key this box acts on"):
            deploy.parse_request(request(f"COMPOSE_SHA256={SHAPE}", "SELVAGE_EXTRA_IMAGE=x"))

    def test_a_duplicate_key_is_refused(self):
        with self.assertRaisesRegex(deploy.Refused, "appears twice"):
            deploy.parse_request(
                request(f"SELVAGED_IMAGE={SELVAGED}", f"SELVAGED_IMAGE={SELVAGED}", f"COMPOSE_SHA256={SHAPE}")
            )

    def test_the_grammar_has_no_room_for_a_parser_difference(self):
        for raw, why in (
            (b"COMPOSE_SHA256=" + SHAPE.encode() + b"\r\nSELVAGED_IMAGE=" + SELVAGED.encode(), "CRLF"),
            (b"\nCOMPOSE_SHA256=" + SHAPE.encode(), "a leading blank line"),
            (b"COMPOSE_SHA256=" + SHAPE.encode() + b"\n\nSELVAGED_IMAGE=" + SELVAGED.encode(), "a blank line"),
            (b"COMPOSE_SHA256=" + SHAPE.encode() + b"\tSELVAGED_IMAGE=" + SELVAGED.encode(), "a tab"),
            (b"COMPOSE_SHA256 = " + SHAPE.encode(), "spaces around the separator"),
            (b"compose_sha256=" + SHAPE.encode(), "a lower-case key"),
            (b"COMPOSE-SHA256=" + SHAPE.encode(), "a dashed key"),
            (b"COMPOSE_SHA256" + SHAPE.encode(), "no separator"),
            (b"SELVAGED_IMAGE=" + SELVAGED.encode() + b"\nCOMPOSE_SHA256=" + SHAPE.encode() + b"\x00", "a NUL"),
            (b"\xff\xfe", "bytes that are not UTF-8"),
        ):
            with self.subTest(why=why), self.assertRaises(deploy.Refused):
                deploy.parse_request(raw)

    def test_an_empty_value_is_refused(self):
        with self.assertRaises(deploy.Refused):
            deploy.parse_request(request("COMPOSE_SHA256=", f"SELVAGED_IMAGE={SELVAGED}"))

    def test_an_oversized_request_is_refused_before_it_is_parsed(self):
        padding = "SELVAGED_IMAGE=" + SELVAGED + "\n"
        body = (f"COMPOSE_SHA256={SHAPE}\n" + padding * 200).encode()
        self.assertGreater(len(body), deploy.MAX_REQUEST_BYTES)
        with self.assertRaisesRegex(deploy.Refused, "longer than"):
            deploy.parse_request(body)


class ReadEnvTest(unittest.TestCase):
    def test_it_reads_the_two_pins_and_ignores_the_rest(self):
        pins = deploy.read_env(
            "# a comment\n\nSELVAGED_IMAGE=" + SELVAGED + "\nOTHER=1\nSELVAGE_WEB_IMAGE=" + WEB + "\n"
        )
        self.assertEqual(pins, {"SELVAGED_IMAGE": SELVAGED, "SELVAGE_WEB_IMAGE": WEB})

    def test_a_malformed_value_is_read_as_it_is_and_refused_later(self):
        self.assertEqual(deploy.read_env("SELVAGED_IMAGE=latest\n"), {"SELVAGED_IMAGE": "latest"})


class ResolvePinsTest(unittest.TestCase):
    def setUp(self):
        self.current = {"SELVAGED_IMAGE": SELVAGED, "SELVAGE_WEB_IMAGE": WEB}

    def test_a_request_overrides_what_is_there(self):
        other = "ghcr.io/selvage-protocol/selvaged@sha256:" + "d" * 64
        pins = deploy.resolve_pins(self.current, {"SELVAGED_IMAGE": other})
        self.assertEqual(pins, {"SELVAGED_IMAGE": other, "SELVAGE_WEB_IMAGE": WEB})

    def test_an_omitted_key_leaves_that_service_alone(self):
        pins = deploy.resolve_pins(self.current, {"SELVAGE_WEB_IMAGE": WEB})
        self.assertEqual(pins["SELVAGED_IMAGE"], SELVAGED)

    def test_nothing_to_leave_alone_is_refused(self):
        with self.assertRaisesRegex(deploy.Refused, "neither the request nor"):
            deploy.resolve_pins({}, {"SELVAGED_IMAGE": SELVAGED})

    def test_a_reference_the_file_cannot_supply_is_refused(self):
        for current in (
            {"SELVAGED_IMAGE": "latest", "SELVAGE_WEB_IMAGE": WEB},
            {"SELVAGED_IMAGE": WEB, "SELVAGE_WEB_IMAGE": WEB},
            {"SELVAGED_IMAGE": SELVAGED, "SELVAGE_WEB_IMAGE": ""},
        ):
            with self.subTest(current=current), self.assertRaises(deploy.Refused):
                deploy.resolve_pins(current, {})

    def test_a_malformed_requested_value_is_refused_here_too(self):
        with self.assertRaises(deploy.Refused):
            deploy.resolve_pins(self.current, {"SELVAGED_IMAGE": "latest"})

    def test_rendering_round_trips(self):
        pins = {"SELVAGED_IMAGE": SELVAGED, "SELVAGE_WEB_IMAGE": WEB}
        self.assertEqual(deploy.read_env(deploy.render_env(pins)), pins)


class RecordTest(unittest.TestCase):
    def test_the_file_it_replaced_is_kept(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            previous = Path(directory) / ".env.prev"
            env_file.write_text("SELVAGED_IMAGE=old\n")
            with mock.patch.multiple(deploy, ENV_FILE=env_file, ENV_PREV=previous):
                self.assertTrue(deploy.record({"SELVAGED_IMAGE": SELVAGED, "SELVAGE_WEB_IMAGE": WEB}, "SELVAGED_IMAGE=old\n"))
            self.assertEqual(previous.read_text(), "SELVAGED_IMAGE=old\n")
            self.assertEqual(previous.stat().st_mode & 0o777, 0o600)
            self.assertEqual(env_file.stat().st_mode & 0o777, 0o600)
            self.assertIn(SELVAGED, env_file.read_text())

    def test_an_unchanged_file_is_not_rewritten(self):
        with tempfile.TemporaryDirectory() as directory:
            pins = {"SELVAGED_IMAGE": SELVAGED, "SELVAGE_WEB_IMAGE": WEB}
            env_file = Path(directory) / ".env"
            previous = Path(directory) / ".env.prev"
            previous.write_text("the last real previous state\n")
            env_file.write_text(deploy.render_env(pins))
            with mock.patch.multiple(deploy, ENV_FILE=env_file, ENV_PREV=previous):
                self.assertFalse(deploy.record(pins, deploy.render_env(pins)))
            self.assertEqual(previous.read_text(), "the last real previous state\n")


class VerdictTest(unittest.TestCase):
    def state(self, identifier, started, status="running"):
        return {"id": identifier, "started": started, "image": "sha256:" + "e" * 64, "status": status}

    def test_a_new_container_id_is_a_recreation(self):
        self.assertEqual(deploy.verdict(self.state("one", "t0"), self.state("two", "t1")), "recreated")

    def test_the_same_container_restarted_is_a_restart(self):
        self.assertEqual(deploy.verdict(self.state("one", "t0"), self.state("one", "t1")), "restarted")

    def test_the_same_container_still_running_is_unchanged(self):
        self.assertEqual(deploy.verdict(self.state("one", "t0"), self.state("one", "t0")), "unchanged")

    def test_a_service_that_was_not_there(self):
        self.assertEqual(deploy.verdict({}, self.state("one", "t0")), "created")
        self.assertEqual(deploy.verdict({}, {}), "absent")
        self.assertEqual(deploy.verdict(self.state("one", "t0"), {}), "removed")


class MainTest(unittest.TestCase):
    """`main()` itself, with the two things that decide whether it is safe to call."""

    def run_main(self, request_bytes, arguments=None, compose_stub=None):
        stdin = types.SimpleNamespace(buffer=io.BytesIO(request_bytes))
        with mock.patch.object(sys, "stdin", stdin), mock.patch.object(
            deploy, "compose", compose_stub or (lambda *a: self.fail(f"docker compose {a} was called"))
        ), mock.patch.object(deploy, "run", lambda *a: self.fail("a docker command was run")):
            return deploy.main(arguments or ["selvage-deploy"])

    def test_arguments_are_refused(self):
        self.assertEqual(deploy.main(["selvage-deploy", "--rollback"]), 2)
        self.assertEqual(deploy.main(["selvage-deploy", SELVAGED]), 2)

    def test_a_shape_mismatch_never_reaches_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            etc = Path(directory)
            compose = etc / "compose.yaml"
            compose.write_text("name: selvage-prod\n")
            env_file = etc / ".env"
            env_file.write_text(f"SELVAGED_IMAGE={SELVAGED}\nSELVAGE_WEB_IMAGE={WEB}\n")
            with mock.patch.multiple(
                deploy,
                ETC=etc,
                COMPOSE=compose,
                ENV_FILE=env_file,
                ENV_PREV=etc / ".env.prev",
            ), mock.patch("os.geteuid", return_value=0):
                status = self.run_main(
                    request(f"SELVAGED_IMAGE={SELVAGED}", f"COMPOSE_SHA256={'0' * 64}")
                )
            self.assertEqual(status, 1)
            self.assertFalse((etc / ".env.prev").exists())
            self.assertEqual(env_file.read_text(), f"SELVAGED_IMAGE={SELVAGED}\nSELVAGE_WEB_IMAGE={WEB}\n")

    def test_a_refused_request_never_reaches_docker(self):
        with mock.patch("os.geteuid", return_value=0):
            self.assertEqual(self.run_main(b"SELVAGED_IMAGE=latest\n"), 1)


class StagedDeployTest(unittest.TestCase):
    """A failed deploy must not move the box's record of what it intends to run.

    `.env` is the intent for the *next* `up -d`, so writing the new references into
    it before the containers are up turns one bad release into a box that a hand
    `up -d` and every later dispatch reproduce. The references therefore go into
    `.env.deploy`, which is what `pull`, `up` and the convergence check read, and
    `.env` is written only once they are up and converged.
    """

    OTHER = "ghcr.io/selvage-protocol/selvaged@sha256:" + "d" * 64

    def prepare(self, directory):
        etc = Path(directory)
        (etc / "compose.yaml").write_text("name: selvage-prod\n")
        before = f"SELVAGED_IMAGE={SELVAGED}\nSELVAGE_WEB_IMAGE={WEB}\n"
        (etc / ".env").write_text(before)
        return etc, before, hashlib.sha256((etc / "compose.yaml").read_bytes()).hexdigest()

    def deploy(self, etc, shape, request_bytes, compose_stub, run_stub):
        stdin = types.SimpleNamespace(buffer=io.BytesIO(request_bytes))
        with mock.patch.multiple(
            deploy,
            ETC=etc,
            COMPOSE=etc / "compose.yaml",
            ENV_FILE=etc / ".env",
            ENV_PREV=etc / ".env.prev",
            ENV_STAGED=etc / ".env.deploy",
        ), mock.patch.object(sys, "stdin", stdin), mock.patch.object(
            deploy, "compose", compose_stub
        ), mock.patch.object(deploy, "run", run_stub), mock.patch(
            "os.geteuid", return_value=0
        ):
            return deploy.main(["selvage-deploy"])

    def test_a_failed_up_leaves_the_persistent_pins_alone(self):
        with tempfile.TemporaryDirectory() as directory:
            etc, before, shape = self.prepare(directory)
            asked = []

            def compose_stub(env_file, *arguments):
                asked.append((env_file, arguments))
                return subprocess.CompletedProcess(
                    [], 1 if arguments[:1] == ("up",) else 0, "", ""
                )

            def run_stub(argv):
                return subprocess.CompletedProcess(argv, 0, "", "")

            status = self.deploy(
                etc,
                shape,
                request(f"COMPOSE_SHA256={shape}", f"SELVAGED_IMAGE={self.OTHER}"),
                compose_stub,
                run_stub,
            )

            self.assertEqual(status, 1)
            self.assertEqual((etc / ".env").read_text(), before)
            self.assertFalse((etc / ".env.prev").exists())
            self.assertIn(self.OTHER, (etc / ".env.deploy").read_text())
            self.assertEqual({env for env, _ in asked}, {etc / ".env.deploy"})

    def test_a_converged_deploy_moves_the_record_and_clears_the_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            etc, before, shape = self.prepare(directory)
            asked = []
            running = "sha256:" + "e" * 64

            def compose_stub(env_file, *arguments):
                asked.append((env_file, arguments))
                return subprocess.CompletedProcess([], 0, "", "")

            def run_stub(argv):
                if argv[:2] == ["docker", "image"]:
                    return subprocess.CompletedProcess(argv, 0, running + "\n", "")
                if argv[:2] == ["docker", "inspect"]:
                    return subprocess.CompletedProcess(
                        argv, 0, f"one|t0|{running}|running\n", ""
                    )
                return subprocess.CompletedProcess(argv, 0, "one\n", "")

            status = self.deploy(
                etc,
                shape,
                request(f"COMPOSE_SHA256={shape}", f"SELVAGED_IMAGE={self.OTHER}"),
                compose_stub,
                run_stub,
            )

            self.assertEqual(status, 0)
            self.assertIn(self.OTHER, (etc / ".env").read_text())
            self.assertEqual((etc / ".env.prev").read_text(), before)
            self.assertFalse((etc / ".env.deploy").exists())
            self.assertEqual({env for env, _ in asked}, {etc / ".env.deploy"})

    def test_a_failed_run_leaves_the_next_request_its_values_to_resolve(self):
        """A request that omits the service still finds the pins that work.

        This is the failure CodeRabbit named: a failed update followed by a request
        naming only the other service. Because `.env` never moved, the omitted key
        still resolves to the reference that was good, and the run deploys that.
        """
        with tempfile.TemporaryDirectory() as directory:
            etc, _, shape = self.prepare(directory)
            asked = []

            def compose_stub(env_file, *arguments):
                asked.append((env_file, arguments))
                return subprocess.CompletedProcess(
                    [], 1 if arguments[:1] == ("up",) else 0, "", ""
                )

            def run_stub(argv):
                return subprocess.CompletedProcess(argv, 0, "", "")

            self.deploy(
                etc,
                shape,
                request(f"COMPOSE_SHA256={shape}", f"SELVAGED_IMAGE={self.OTHER}"),
                compose_stub,
                run_stub,
            )
            self.deploy(
                etc,
                shape,
                request(f"COMPOSE_SHA256={shape}", f"SELVAGE_WEB_IMAGE={WEB}"),
                compose_stub,
                run_stub,
            )

            staged = (etc / ".env.deploy").read_text()
            self.assertIn(f"SELVAGED_IMAGE={SELVAGED}", staged)
            self.assertNotIn(self.OTHER, staged)
            self.assertIn("up", [call for _, call in asked][-1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
