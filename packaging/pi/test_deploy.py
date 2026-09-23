"""The guard around `packaging/pi/deploy.py`, which is the whole of the Pi's
privilege model the way `/usr/local/sbin/selvage-deploy` is the public demo's:
the request it reads on stdin is the only lever anything holding a CI credential
has on that box.

The Pi's deploy is the public demo's deploy with this box's paths, so the two
properties that carry the weight — only digest-pinned references in this registry
and nothing else, and a shape mismatch that stops the run before a container is
touched — are asserted at length in `packaging/prod/test_deploy.py`. This file
runs that suite against this box's module rather than restating it, because a
second copy of five hundred lines of guard is a second copy to keep true, and the
two scripts are meant to refuse exactly the same things.

What is asserted here and not there is the other direction: the compose file this
box deploys names the services the deploy records, and interpolates the variables
it pins. A third published image added to the shape without a line in `PINS` would
otherwise be brought up by `up -d` and never recorded, and the next `pull` would
not move it.

Run it directly:

    python3 packaging/pi/test_deploy.py

or as the flake check the workflows run: `nix build .#checks.<system>.pi-deploy`.
"""

import importlib.util
import re
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent


def module_from(name, path):
    """A module loaded by its path, so its name cannot decide what it imports.

    Both this box and the public demo have a `deploy.py` and a `test_deploy.py`,
    so two `import`s by name would be a question about `sys.path` order rather
    than about which file is meant.
    """
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


pi_deploy = module_from("pi_deploy", HERE / "deploy.py")
# The public demo's suite reads the guard through the name `deploy`, which is
# what its module imports at the top. Registering this box's module under that
# name before that file is executed is the whole of what makes the suite run
# against the Pi's script instead of the one beside it.
sys.modules["deploy"] = pi_deploy
prod_guard = module_from(
    "prod_test_deploy", HERE.parent / "prod" / "test_deploy.py"
)
prod_guard.deploy = pi_deploy


def compose_body(text: str) -> str:
    """The file's directives, with its comment lines dropped.

    Both reads below are scans, and a scan that walks the prose too reports what
    the prose says rather than what the file declares.
    """
    return "\n".join(
        line for line in text.split("\n") if not line.lstrip().startswith("#")
    )


def service_names(text: str) -> set:
    """The services the compose file defines, read off the file rather than named."""
    names = set()
    in_services = False
    for line in compose_body(text).split("\n"):
        if line.startswith("services:"):
            in_services = True
            continue
        if in_services and line and not line.startswith(" "):
            in_services = False
        if in_services and re.fullmatch(r"  [a-z][a-z0-9_-]*:", line):
            names.add(line.strip().rstrip(":"))
    return names


def interpolated_variables(text: str) -> set:
    """The `${VAR:?…}` variables the compose file reads out of `.env`."""
    return set(re.findall(r"\$\{([A-Z][A-Z0-9_]*):\?", compose_body(text)))


class ComposeShapeTest(unittest.TestCase):
    """The shape and the deploy agree about what is deployed.

    Both halves are guards against the same failure in opposite directions: an
    image in the shape the deploy does not know about, and a pin in the deploy the
    shape does not use.
    """

    def setUp(self):
        self.compose = HERE / "compose.yaml"
        self.text = self.compose.read_text(encoding="utf-8")

    def test_the_file_was_read_and_both_reads_found_something(self):
        self.assertIn("services:", self.text)
        self.assertTrue(
            service_names(self.text), "no service was read out of compose.yaml"
        )
        self.assertTrue(
            interpolated_variables(self.text),
            "no ${VAR:?…} interpolation was read out of compose.yaml",
        )

    def test_the_shape_names_exactly_the_services_the_deploy_records(self):
        self.assertEqual(service_names(self.text), set(pi_deploy.SERVICES))

    def test_the_shape_interpolates_exactly_the_references_the_deploy_pins(self):
        self.assertEqual(interpolated_variables(self.text), set(pi_deploy.PINS))

    def test_the_shape_uses_no_tabs(self):
        """YAML forbids a tab in its indentation, and nothing here parses the file.

        This is the one structural mistake a scan can catch that a hand edit is
        likely to make, and the box would only find it at `docker compose up`.
        """
        offending = [
            number
            for number, line in enumerate(self.text.split("\n"), 1)
            if "\t" in line
        ]
        self.assertEqual([], offending, f"compose.yaml has a tab on line(s) {offending}")

    def test_neither_image_is_named_anywhere_in_the_shape(self):
        """A digest is the deployment's record of what runs, so it has one home.

        A reference written into `compose.yaml` would be a second copy of what
        `.env` says, and the shape would no longer be the thing that does not
        change on a release.
        """
        for line in compose_body(self.text).split("\n"):
            body = line.split("#", 1)[0]
            if body.strip().startswith("image:"):
                self.assertIn(
                    "${",
                    body,
                    f"compose.yaml names an image outright: {line.strip()}",
                )


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for name in sorted(dir(prod_guard)):
        case = getattr(prod_guard, name)
        if isinstance(case, type) and issubclass(case, unittest.TestCase):
            suite.addTests(loader.loadTestsFromTestCase(case))
    suite.addTests(loader.loadTestsFromTestCase(ComposeShapeTest))
    return suite


if __name__ == "__main__":
    unittest.main(verbosity=2)
