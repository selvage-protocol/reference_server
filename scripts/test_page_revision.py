"""The guard around `scripts/page-revision.sh`, which names the page the server image bakes.

The release workflow runs it and hands what it prints to
`scripts/bump-version.sh --page-sha`, so `Dockerfile`'s `ARG WEB_CLIENT_SHA=` is
moved by a release rather than by hand. What that value has to be, and what must
stop the release when it cannot be resolved:

1. **The commit of `web_client`'s latest release, and not the tag object.** A tag
   is one of two objects and `git ls-remote` answers them differently: an
   **annotated** tag is an object of its own with a peeled `refs/tags/<tag>^{}`
   line naming the commit it points at, and a **lightweight** tag *is* the commit
   with no such line. The Dockerfile peels whatever it is given into a
   `git fetch --depth 1 origin <sha>`, which takes a commit, so the peeled line has
   to win — and the plain pattern alone is not enough to get it, which is why the
   script asks for both.
2. **One line of stdout, 40 lowercase hex, and nothing else on it.** The workflow
   reads it as a value.
3. **A refusal says which of the two commands failed and what it saw**, writes
   nothing on stdout, and exits non-zero: no latest release, a release that names
   no tag, a tag the remote does not have, an unreadable remote, and a value that
   is not a commit.

Both commands are reached through `PATH`, so the tests put stand-ins there that
answer what the test tells them to and record what they were asked. No test needs
a network, a token or a checkout of another repository.

Run it directly:

    python3 -B scripts/test_page_revision.py

or as the flake check the workflows run: `nix build .#checks.<system>.page-revision`.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "page-revision.sh"
BASH = shutil.which("bash") or "/bin/bash"

TAG = "v0.5.1"
# The page repository's latest release, as its commit and as an annotated tag's
# own object: two different shas, so a test cannot pass by accident.
COMMIT = "ecd07aa5daabdcc224f892784202b156b4c5f6a7"
TAG_OBJECT = "3033274e239935039046c812b5dcd1844d7afa06"

# `git ls-remote <remote> refs/tags/<tag> refs/tags/<tag>^{}`: one line for a
# lightweight tag (the commit), two for an annotated one (its object, then the
# commit it peels to). Both forms are recorded from the real command.
LIGHTWEIGHT = f"{COMMIT}\trefs/tags/{TAG}\n"
ANNOTATED = f"{TAG_OBJECT}\trefs/tags/{TAG}\n{COMMIT}\trefs/tags/{TAG}^{{}}\n"

STUB_GH = '''\
#!/usr/bin/env python3
import os
import sys

log = os.environ.get("STUB_GH_LOG")
if log:
    with open(log, "a", encoding="utf-8") as handle:
        handle.write(" ".join(sys.argv[1:]) + "\\n")

if os.environ.get("STUB_GH_STATUS"):
    print("stub gh: no release", file=sys.stderr)
    sys.exit(int(os.environ["STUB_GH_STATUS"]))

print(os.environ.get("STUB_GH_TAG", ""))
'''

STUB_GIT = '''\
#!/usr/bin/env python3
import os
import sys

log = os.environ.get("STUB_GIT_LOG")
if log:
    with open(log, "a", encoding="utf-8") as handle:
        handle.write(" ".join(sys.argv[1:]) + "\\n")

if os.environ.get("STUB_GIT_STATUS"):
    print("stub git: cannot reach the remote", file=sys.stderr)
    sys.exit(int(os.environ["STUB_GIT_STATUS"]))

sys.stdout.write(os.environ.get("STUB_GIT_REFS", ""))
'''


class Stubs:
    """A `bin/` holding the two commands the script reaches through `PATH`."""

    def __init__(self, root: Path):
        self.root = root
        self.bin = root / "bin"
        self.bin.mkdir(parents=True)
        self.gh_log = root / "gh.log"
        self.git_log = root / "git.log"
        for name, source in (("gh", STUB_GH), ("git", STUB_GIT)):
            stub = self.bin / name
            # The stand-in is executed by the script through `PATH`, so its
            # shebang has to name this interpreter: a nix build sandbox has no
            # `/usr/bin/env`.
            stub.write_text(f"#!{sys.executable}\n" + source.split("\n", 1)[1], encoding="utf-8")
            stub.chmod(0o755)

    def asked(self, log: Path, what: str) -> "str":
        return log.read_text(encoding="utf-8") if log.exists() else what


class RevisionTest(unittest.TestCase):
    """A stubbed tree, and the two ways the script answers."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.stubs = Stubs(Path(self.directory.name))

    def run_script(self, **environment):
        env = dict(os.environ)
        env["PATH"] = f"{self.stubs.bin}:{env['PATH']}"
        env["STUB_GH_LOG"] = str(self.stubs.gh_log)
        env["STUB_GIT_LOG"] = str(self.stubs.git_log)
        # Every case is a repository whose latest release names this tag; the
        # cases that are about *not* resolving one override it.
        env["STUB_GH_TAG"] = TAG
        env.update(environment)
        done = subprocess.run(
            [BASH, str(SCRIPT)],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        done.asked_gh = self.stubs.asked(self.stubs.gh_log, "")
        done.asked_git = self.stubs.asked(self.stubs.git_log, "")
        return done

    def resolve(self, **environment):
        done = self.run_script(STUB_GH_TAG=TAG, **environment)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        return done

    def refused(self, says: str, **environment):
        done = self.run_script(**environment)
        self.assertNotEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("refused:", done.stderr)
        self.assertIn(says, done.stderr)
        self.assertEqual(done.stdout, "", "a refusal wrote to stdout")
        return done


class ResolutionTest(RevisionTest):
    """The value the release pins the page stage to."""
    def test_a_lightweight_tag_resolves_to_its_commit(self):
        done = self.resolve(STUB_GIT_REFS=LIGHTWEIGHT)
        self.assertEqual(done.stdout, f"{COMMIT}\n", "stdout is the commit's own line")

    def test_an_annotated_tag_resolves_to_the_commit_it_peels_to(self):
        done = self.resolve(STUB_GIT_REFS=ANNOTATED)
        self.assertEqual(done.stdout, f"{COMMIT}\n")
        self.assertNotIn(TAG_OBJECT, done.stdout, "the tag object is not a commit")

    def test_both_tag_patterns_are_asked_for(self):
        """The plain pattern alone answers an annotated tag with the tag object."""
        done = self.resolve(STUB_GIT_REFS=ANNOTATED)
        self.assertIn(f"refs/tags/{TAG}^{{}}", done.asked_git)
        self.assertIn(f"refs/tags/{TAG} ", done.asked_git)

    def test_the_latest_release_is_what_names_the_tag(self):
        done = self.resolve(STUB_GIT_REFS=LIGHTWEIGHT)
        self.assertIn("releases/latest", done.asked_gh)
        self.assertIn("selvage-protocol/web_client", done.asked_gh)

    def test_the_value_is_one_line_and_nothing_else(self):
        done = self.resolve(STUB_GIT_REFS=ANNOTATED)
        self.assertEqual(done.stdout.splitlines(), [COMMIT])


class RefusalTest(RevisionTest):
    """Nothing to pin is a stopped release, not a guess."""

    def test_a_repository_with_no_latest_release_is_refused(self):
        self.refused("cannot read", STUB_GH_STATUS="1", STUB_GIT_REFS=LIGHTWEIGHT)

    def test_a_release_that_names_no_tag_is_refused(self):
        self.refused("names no tag", STUB_GH_TAG="", STUB_GIT_REFS=LIGHTWEIGHT)

    def test_a_tag_the_remote_does_not_have_is_refused(self):
        self.refused("no tag", STUB_GIT_REFS="")

    def test_an_unreadable_remote_is_refused(self):
        self.refused("cannot read", STUB_GIT_STATUS="1")

    def test_a_value_that_is_not_a_commit_is_refused(self):
        for answer in (f"not-a-sha\trefs/tags/{TAG}\n", f"{TAG_OBJECT[:39]}\trefs/tags/{TAG}\n"):
            with self.subTest(answer=answer):
                self.refused("40-hex", STUB_GIT_REFS=answer)

    def test_a_refusal_does_not_reach_the_second_command(self):
        self.refused("cannot read", STUB_GH_STATUS="1")
        self.assertEqual(self.stubs.asked(self.stubs.git_log, ""), "")


class InstallationTest(unittest.TestCase):
    """The script is run as a program, so the tracked file is executable."""

    def test_the_script_is_executable_and_has_a_shebang(self):
        self.assertTrue(os.access(SCRIPT, os.X_OK), SCRIPT)
        self.assertEqual(SCRIPT.read_bytes().split(b"\n", 1)[0], b"#!/usr/bin/env bash")


if __name__ == "__main__":
    unittest.main(verbosity=2)
