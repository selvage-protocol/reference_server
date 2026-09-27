"""The guard around `install_shape.py`, which is half of the box's privilege model.

`/usr/local/sbin/selvage-install-shape` is one of the two commands `deployci` may
run as root (`installci.sudoers`), and the tar stream it reads on stdin is a
root-owned write into `/srv/selvage` that ends in `docker compose up -d --build
proxy`. What carries that weight is asserted here rather than argued:

1. **Only the box's shape, and nothing else.** `compose.yaml` and anything under
   `proxy/`, by one grammar: relative, plain names. Every test below is a shape
   one might *hope* a script would refuse — an entry outside the set, an absolute
   path, a traversal, a symlink, a hardlink, a device, a fifo.
2. **A refusal writes nothing.** Each of those seeds a valid entry *before* the
   refused one, so a stream that was installed as it was read would show up here,
   and each compares the whole tree byte for byte.
3. **A step is resolved inside the descriptor the step before it found.** A
   symlinked *directory* — not the symlinked file the obvious guard already
   rejects — is the shape that catches a check-then-use, and it is refused with
   the directory it points at left alone.
4. **The files the shape does not own are not touched**: `.env`, `.env.example`,
   `tls/`, and anything else in the box.

The composer of the stream is the runbook's own command —
`git archive --format=tar <sha>:deploy compose.yaml proxy` — so the last test
feeds the **tracked** shape through the parser: if a file is added to `proxy/`
that a real release would send and this refuses, that test fails rather than the
install.

Run it directly:

    python3 -B deploy/test_install_shape.py

or as the flake check the workflows run: `nix build .#checks.<system>.prod-shape`.
"""

import contextlib
import io
import os
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import install_shape  # noqa: E402  (the path above is what makes this importable)

COMPOSE = b"services:\n  proxy:\n    build: ./proxy\n"
DEFAULT_CONF = b"server { listen 8080; }\n"


def work_root() -> str:
    """Where the harness writes. Not `/tmp` by default: it is a RAM-backed tmpfs
    on this project's host, and a build there has taken a machine down."""
    root = os.environ.get("INSTALL_SHAPE_WORKDIR")
    if not root:
        root = os.path.join(os.path.dirname(HERE), ".tmp", "install-shape")
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)
    return root


def archive(specs, format=tarfile.PAX_FORMAT) -> bytes:
    """A tar stream of `(kind, name, payload)` specifications.

    `PAX_FORMAT` is what `git archive` writes, so the pseudo-header that carries a
    long name is exercised rather than assumed away.
    """
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:", format=format) as writing:
        for kind, name, payload in specs:
            info = tarfile.TarInfo(name)
            if kind == "dir":
                info.type = tarfile.DIRTYPE
                info.mode = 0o755
            elif kind == "file":
                info.type = tarfile.REGTYPE
                info.mode = 0o644
                info.size = len(payload)
            elif kind == "sym":
                info.type = tarfile.SYMTYPE
                info.linkname = payload.decode()
            elif kind == "hard":
                info.type = tarfile.LNKTYPE
                info.linkname = payload.decode()
            elif kind in ("chr", "blk"):
                info.type = tarfile.CHRTYPE if kind == "chr" else tarfile.BLKTYPE
                info.devmajor, info.devminor = 1, 3
            elif kind == "fifo":
                info.type = tarfile.FIFOTYPE
            else:
                raise AssertionError(kind)
            writing.addfile(info, io.BytesIO(payload) if kind == "file" else None)
    return buffer.getvalue()


def tracked_shape() -> bytes:
    """The tracked shape under the two names the runbook's archive writes."""
    specs = [("file", "compose.yaml", (HERE / "compose.yaml").read_bytes())]
    for path in sorted((HERE / "proxy").rglob("*")):
        relative = f"proxy/{path.relative_to(HERE / 'proxy')}"
        if path.is_dir():
            specs.append(("dir", f"{relative}/", b""))
        else:
            specs.append(("file", relative, path.read_bytes()))
    return archive(specs)


def shape() -> bytes:
    """A stream that is entirely in the set: the front's three files and a document."""
    return archive(
        [
            ("file", "compose.yaml", COMPOSE),
            ("dir", "proxy/", b""),
            ("file", "proxy/Dockerfile", b"FROM nginx\n"),
            ("dir", "proxy/conf.d/", b""),
            ("file", "proxy/conf.d/default.conf", DEFAULT_CONF),
        ]
    )


def conf_stream() -> bytes:
    """The front's configuration alone, with no directory member to refuse first."""
    return archive(
        [
            ("file", "compose.yaml", COMPOSE),
            ("file", "proxy/conf.d/default.conf", DEFAULT_CONF),
        ]
    )


def snapshot(root: Path) -> dict:
    """Every path under `root`: its kind, its mode, its mtime and its bytes.

    The mtime is what makes "writes nothing" a claim a refusal and an
    already-current install have to meet rather than describe: a rewrite of the
    same bytes does not change the mode or the content, and would slip past a
    snapshot that recorded only those.
    """
    taken = {}
    for path in sorted(root.rglob("*")):
        info = path.lstat()
        relative = str(path.relative_to(root))
        if stat.S_ISLNK(info.st_mode):
            taken[relative] = ("link", os.readlink(path))
        elif stat.S_ISDIR(info.st_mode):
            taken[relative] = ("dir", info.st_mode & 0o777, info.st_mtime_ns)
        else:
            taken[relative] = ("file", info.st_mode & 0o777, info.st_mtime_ns, path.read_bytes())
    return taken


class Box:
    """A stand-in for `/srv/selvage`, and the one compose call the script makes."""

    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="box-", dir=work_root()))
        self.srv = self.root / "srv" / "selvage"
        self.srv.mkdir(parents=True)
        self.outside = self.root / "outside"
        self.outside.mkdir()
        self.calls = []
        self.status = 0
        self.stdout = ""
        self.stderr = ""

    def seed(self, name: str, text: str):
        path = self.srv / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def link(self, name: str, target) -> None:
        (self.srv / name).symlink_to(target)

    def compose(self, argv):
        self.calls.append(list(argv))
        return subprocess.CompletedProcess(list(argv), self.status, "", "")

    def run(self, stream: bytes, argv=("selvage-install-shape",), root=True, status=0, bound=None, compose=None):
        self.status = status
        patches = [
            mock.patch.multiple(install_shape, SRV=self.srv, LOCK=self.srv / ".update.lock"),
            mock.patch.object(install_shape, "compose", compose or self.compose),
            mock.patch("os.geteuid", return_value=0 if root else 1000),
        ]
        if bound is not None:
            patches.append(mock.patch.object(install_shape, "MAX_STREAM_BYTES", bound))
        stdin = types.SimpleNamespace(buffer=io.BytesIO(stream))
        out, err = io.StringIO(), io.StringIO()
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            stack.enter_context(mock.patch.object(sys, "stdin", stdin))
            stack.enter_context(contextlib.redirect_stdout(out))
            stack.enter_context(contextlib.redirect_stderr(err))
            code = install_shape.main(list(argv))
        self.stdout, self.stderr = out.getvalue(), err.getvalue()
        return code


class RefusalTest(unittest.TestCase):
    """Every one of these leaves the box byte for byte as it was."""

    def setUp(self):
        self.box = Box()
        self.before = snapshot(self.box.srv)

    def assert_refused(self, stream: bytes, named: str, says: str | None = None, **options):
        """`named` is the entry the refusal names; `says` is what it says it was."""
        code = self.box.run(stream, **options)
        message = self.box.stdout + self.box.stderr
        self.assertEqual(code, 1, message)
        self.assertIn("refused:", self.box.stderr)
        self.assertIn(named, self.box.stderr)
        if says is not None:
            self.assertIn(says, self.box.stderr)
        self.assertEqual(snapshot(self.box.srv), self.before, "a refusal wrote to the box")
        self.assertEqual(self.box.calls, [], "a refusal reached docker compose")

    def test_an_entry_outside_the_shape_is_refused(self):
        for name in ("deploy/.env.example", ".env", ".env.example", "other.txt", "proxy", "compose.yml"):
            with self.subTest(name=name):
                stream = archive([("file", "compose.yaml", COMPOSE), ("file", name, b"x")])
                self.assert_refused(stream, name)

    def test_an_absolute_path_is_refused(self):
        for name in ("/etc/passwd", "/srv/selvage/compose.yaml", "//etc/passwd"):
            with self.subTest(name=name):
                self.assert_refused(archive([("file", name, b"x")]), name)

    def test_a_parent_component_is_refused(self):
        for name in ("proxy/../compose.yaml", "../compose.yaml", "proxy/conf.d/../../x", "./compose.yaml"):
            with self.subTest(name=name):
                self.assert_refused(archive([("file", "compose.yaml", COMPOSE), ("file", name, b"x")]), name)

    def test_a_symlink_member_is_refused(self):
        stream = archive([("file", "compose.yaml", COMPOSE), ("sym", "proxy/nginx.conf", b"/etc/passwd")])
        self.assert_refused(stream, "proxy/nginx.conf", says="is a symbolic link")

    def test_a_hardlink_member_is_refused(self):
        stream = archive(
            [
                ("file", "compose.yaml", COMPOSE),
                ("file", "proxy/nginx.conf", b"worker_processes 1;\n"),
                ("hard", "proxy/other.conf", b"proxy/nginx.conf"),
            ]
        )
        self.assert_refused(stream, "proxy/other.conf", says="is a hard link")

    def test_a_device_member_is_refused(self):
        for kind in ("chr", "blk"):
            with self.subTest(kind=kind):
                stream = archive([("file", "compose.yaml", COMPOSE), (kind, "proxy/zero", b"")])
                self.assert_refused(stream, "proxy/zero", says="is a device node")

    def test_a_fifo_member_is_refused(self):
        stream = archive([("file", "compose.yaml", COMPOSE), ("fifo", "proxy/pipe", b"")])
        self.assert_refused(stream, "proxy/pipe", says="is a fifo")

    def test_an_empty_stream_is_refused(self):
        self.assert_refused(b"", "the stream is empty")

    def test_a_tar_with_no_entries_is_refused(self):
        self.assert_refused(archive([]), "carries no file")

    def test_a_stream_of_directories_alone_is_refused(self):
        self.assert_refused(archive([("dir", "proxy/", b""), ("dir", "proxy/conf.d/", b"")]), "carries no file")

    def test_a_stream_that_is_not_a_tar_is_refused(self):
        self.assert_refused(b"not a tar archive at all\n", "not a tar archive")

    def test_a_stream_longer_than_the_bound_is_refused(self):
        body = b"x" * 2048
        self.assert_refused(body, "longer than 1024", bound=1024)

    def test_a_shape_file_that_has_become_a_link_is_refused(self):
        target = self.box.outside / "elsewhere.yaml"
        target.write_text("not the shape\n", encoding="utf-8")
        self.box.link("compose.yaml", target)
        self.before = snapshot(self.box.srv)
        self.assert_refused(archive([("file", "compose.yaml", COMPOSE)]), "compose.yaml")
        self.assertEqual(target.read_text(encoding="utf-8"), "not the shape\n")

    def test_a_leaf_where_a_directory_belongs_is_refused_with_nothing_written(self):
        """Both entries are in the set; the second is refused before the first lands."""
        (self.box.srv / "proxy").mkdir()
        (self.box.srv / "proxy" / "www").mkdir()
        self.before = snapshot(self.box.srv)
        stream = archive(
            [
                ("file", "proxy/conf.d/default.conf", DEFAULT_CONF),
                ("file", "proxy/www", b"x"),
            ]
        )
        self.assert_refused(stream, "proxy/www")

    def test_a_symlinked_directory_inside_the_target_is_refused(self):
        """The shape the obvious guard misses: a *directory* that is a link.

        The stream names `proxy/conf.d/default.conf`, and `proxy` is a link out of
        the box. A guard that checks names and then extracts would follow it.
        """
        self.box.link("proxy", self.box.outside)
        self.before = snapshot(self.box.srv)
        outside_before = snapshot(self.box.outside)
        self.assert_refused(conf_stream(), "proxy/conf.d/default.conf")
        self.assertEqual(snapshot(self.box.outside), outside_before, "the link was followed")
        self.assertEqual(sorted(p.name for p in self.box.outside.iterdir()), [])

    def test_a_link_that_leaves_the_target_at_a_deeper_step_is_refused(self):
        (self.box.srv / "proxy").mkdir()
        (self.box.outside / "conf.d").mkdir()
        self.box.link("proxy/conf.d", self.box.outside / "conf.d")
        self.before = snapshot(self.box.srv)
        outside_before = snapshot(self.box.outside)
        self.assert_refused(conf_stream(), "proxy/conf.d/default.conf")
        self.assertEqual(snapshot(self.box.outside), outside_before)

    def test_a_directory_member_that_is_a_link_is_refused(self):
        (self.box.srv / "proxy").mkdir()
        (self.box.outside / "www").mkdir()
        self.box.link("proxy/www", self.box.outside / "www")
        self.before = snapshot(self.box.srv)
        self.assert_refused(archive([("file", "compose.yaml", COMPOSE), ("dir", "proxy/www/", b"")]), "proxy/www")

    def test_arguments_are_refused(self):
        for argv in (("selvage-install-shape", "--force"), ("selvage-install-shape", "/etc/passwd")):
            with self.subTest(argv=argv):
                self.assertEqual(self.box.run(shape(), argv=argv), 2)
                self.assertEqual(snapshot(self.box.srv), self.before)
                self.assertEqual(self.box.calls, [])

    def test_a_run_that_is_not_root_is_refused(self):
        self.assertEqual(self.box.run(shape(), root=False), 2)
        self.assertIn("must run as root", self.box.stderr)
        self.assertEqual(snapshot(self.box.srv), self.before)
        self.assertEqual(self.box.calls, [])


class InstallTest(unittest.TestCase):
    def setUp(self):
        self.box = Box()

    def test_the_shape_is_installed_and_the_front_brought_up(self):
        self.box.seed(".env", "SELVAGED_IMAGE=ghcr.io/selvage-protocol/selvaged:0.5.1\n")
        self.box.seed(".env.example", "# an example\n")
        self.box.seed("tls/origin.key", "the certificate\n")
        code = self.box.run(shape())
        self.assertEqual(code, 0, self.box.stderr)

        self.assertEqual((self.box.srv / "compose.yaml").read_bytes(), COMPOSE)
        self.assertEqual((self.box.srv / "proxy" / "Dockerfile").read_bytes(), b"FROM nginx\n")
        self.assertEqual((self.box.srv / "proxy" / "conf.d" / "default.conf").read_bytes(), DEFAULT_CONF)
        for name in ("compose.yaml", "proxy/Dockerfile", "proxy/conf.d/default.conf"):
            self.assertEqual((self.box.srv / name).stat().st_mode & 0o777, 0o644, name)
        for name in ("proxy", "proxy/conf.d"):
            self.assertEqual((self.box.srv / name).stat().st_mode & 0o777, 0o755, name)

        # The box's own files, and the files nothing here owns, are untouched.
        self.assertEqual((self.box.srv / ".env").read_text(encoding="utf-8"), "SELVAGED_IMAGE=ghcr.io/selvage-protocol/selvaged:0.5.1\n")
        self.assertEqual((self.box.srv / ".env.example").read_text(encoding="utf-8"), "# an example\n")
        self.assertEqual((self.box.srv / "tls" / "origin.key").read_text(encoding="utf-8"), "the certificate\n")

        self.assertEqual(self.box.calls, [["docker", "compose", "up", "-d", "--build", "proxy"]])
        for name in ("compose.yaml", "proxy/Dockerfile", "proxy/conf.d/default.conf"):
            self.assertIn(f"{self.box.srv}/{name}\n", self.box.stdout)
        self.assertIn("left alone", self.box.stdout)

    def test_every_installed_file_is_owned_by_the_box_s_directory_owner(self):
        owner = os.stat(self.box.srv)
        self.box.run(shape())
        for name in ("compose.yaml", "proxy", "proxy/Dockerfile", "proxy/conf.d", "proxy/conf.d/default.conf"):
            installed = os.stat(self.box.srv / name)
            self.assertEqual((installed.st_uid, installed.st_gid), (owner.st_uid, owner.st_gid), name)

    def test_a_directory_the_stream_does_not_name_is_created_and_a_missing_one_is_kept(self):
        self.box.seed("kept.txt", "not the shape's\n")
        code = self.box.run(shape())
        self.assertEqual(code, 0, self.box.stderr)
        self.assertEqual((self.box.srv / "kept.txt").read_text(encoding="utf-8"), "not the shape's\n")
        self.assertEqual(sorted(p.name for p in self.box.srv.iterdir()), [".update.lock", "compose.yaml", "kept.txt", "proxy"])

    def test_an_installed_shape_is_replaced_rather_than_merged(self):
        self.box.seed("proxy/nginx.conf", "the old configuration\n")
        self.box.seed("proxy/gone.conf", "a file nothing sends any more\n")
        self.box.run(shape())
        self.assertEqual((self.box.srv / "proxy" / "nginx.conf").read_text(encoding="utf-8"), "the old configuration\n")
        self.assertTrue((self.box.srv / "proxy" / "gone.conf").exists(), "the install must not delete what it does not send")

    def test_the_front_is_the_only_service_and_the_command_is_the_hand_command(self):
        self.box.run(shape())
        self.assertEqual(self.box.calls, [["docker", "compose", "up", "-d", "--build", "proxy"]])
        for argv in (install_shape.UP_ARGV, install_shape.REBUILD_ARGV):
            for argument in ("-f", "--project-directory", "--env-file"):
                self.assertNotIn(argument, argv)

    def test_compose_runs_in_the_box_directory(self):
        called = []
        with mock.patch("subprocess.run", lambda argv, **options: called.append((argv, options.get("cwd"))) or subprocess.CompletedProcess(argv, 0, "", "")):
            install_shape.compose(install_shape.UP_ARGV)
        self.assertEqual(called, [(install_shape.UP_ARGV, install_shape.SRV)])

    def test_a_front_that_fails_to_build_is_a_non_zero_exit(self):
        code = self.box.run(shape(), status=1)
        self.assertEqual(code, 1)
        self.assertIn("refused:", self.box.stderr)
        self.assertIn("not rebuilt", self.box.stderr)
        self.assertEqual((self.box.srv / "compose.yaml").read_bytes(), COMPOSE)

    def test_the_box_is_locked_while_it_installs(self):
        import fcntl

        held = []

        def composing(argv):
            probe = open(self.box.srv / ".update.lock", "r")
            try:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                held.append(True)
            else:
                held.append(False)
            finally:
                probe.close()
            return self.box.compose(argv)

        self.assertEqual(self.box.run(shape(), compose=composing), 0)
        self.assertEqual(held, [True], "the install did not hold the update timer's lock")

    def test_the_tracked_shape_is_a_stream_this_installs(self):
        """A file a release adds under `proxy/` must not need a change here."""
        code = self.box.run(tracked_shape())
        self.assertEqual(code, 0, self.box.stderr)
        self.assertEqual((self.box.srv / "compose.yaml").read_bytes(), (HERE / "compose.yaml").read_bytes())
        for path in sorted((HERE / "proxy").rglob("*")):
            if path.is_file():
                self.assertEqual((self.box.srv / "proxy" / path.relative_to(HERE / "proxy")).read_bytes(), path.read_bytes(), path)

class IdempotenceTest(unittest.TestCase):
    """A release installs the shape on every deploy, so the box must not be
    restarted for a stream it already holds, and the two differences that can be
    left are the two compose commands there are."""

    def setUp(self):
        self.box = Box()
        self.assertEqual(self.box.run(shape()), 0, self.box.stderr)
        self.box.calls = []

    def test_a_stream_the_box_already_holds_writes_nothing_and_runs_nothing(self):
        # By content, not by mtime: a file whose bytes are right is right however
        # old it looks, and a reinstall that rewrote it would restart a live front.
        os.utime(self.box.srv / "compose.yaml")
        os.utime(self.box.srv / "proxy" / "conf.d" / "default.conf")
        before = snapshot(self.box.srv)
        self.assertEqual(self.box.run(shape()), 0, self.box.stderr)
        self.assertEqual(snapshot(self.box.srv), before, "an already-current install wrote to the box")
        self.assertEqual(self.box.calls, [], "an already-current install reached docker compose")
        self.assertIn("nothing differs", self.box.stdout)
        self.assertNotIn("=== installed ===", self.box.stdout)

    def test_a_compose_only_change_brings_the_services_up_without_a_build(self):
        moved = COMPOSE + b"  web:\n    image: example\n"
        stream = archive(
            [
                ("file", "compose.yaml", moved),
                ("dir", "proxy/", b""),
                ("file", "proxy/Dockerfile", b"FROM nginx\n"),
                ("dir", "proxy/conf.d/", b""),
                ("file", "proxy/conf.d/default.conf", DEFAULT_CONF),
            ]
        )
        self.assertEqual(self.box.run(stream), 0, self.box.stderr)
        self.assertEqual(self.box.calls, [["docker", "compose", "up", "-d"]])
        self.assertEqual((self.box.srv / "compose.yaml").read_bytes(), moved)
        self.assertIn("compose.yaml", self.box.stdout)
        self.assertIn("=== docker compose up -d ===", self.box.stdout)

    def test_a_proxy_change_rebuilds_the_front(self):
        moved = b"server { listen 8080; listen 8081; }\n"
        stream = archive(
            [
                ("file", "compose.yaml", COMPOSE),
                ("dir", "proxy/", b""),
                ("file", "proxy/Dockerfile", b"FROM nginx\n"),
                ("dir", "proxy/conf.d/", b""),
                ("file", "proxy/conf.d/default.conf", moved),
            ]
        )
        self.assertEqual(self.box.run(stream), 0, self.box.stderr)
        self.assertEqual(self.box.calls, [["docker", "compose", "up", "-d", "--build", "proxy"]])
        self.assertEqual((self.box.srv / "proxy" / "conf.d" / "default.conf").read_bytes(), moved)
        self.assertIn("proxy/conf.d/default.conf", self.box.stdout)
        self.assertIn("=== docker compose up -d --build proxy ===", self.box.stdout)

    def test_a_file_the_box_does_not_have_yet_is_a_rebuild(self):
        stream = archive(
            [
                ("file", "compose.yaml", COMPOSE),
                ("file", "proxy/Dockerfile", b"FROM nginx\n"),
                ("file", "proxy/www/terms.html", b"<p>terms</p>\n"),
            ]
        )
        self.assertEqual(self.box.run(stream), 0, self.box.stderr)
        self.assertEqual(self.box.calls, [["docker", "compose", "up", "-d", "--build", "proxy"]])
        self.assertEqual((self.box.srv / "proxy" / "www" / "terms.html").read_bytes(), b"<p>terms</p>\n")


class SudoersTest(unittest.TestCase):
    """The line that bounds the script, asserted rather than read by eye.

    The trailing `""` is the whole of the argument bound — without it sudo permits
    *any* argument, because an empty argument list in sudoers means "any" rather
    than "none" — and it is one keystroke from being dropped.
    """

    def test_the_one_command_is_named_with_no_argument_list(self):
        lines = [
            line
            for line in (HERE / "installci.sudoers").read_text(encoding="utf-8").splitlines()
            if line and not line.startswith("#")
        ]
        self.assertEqual(lines, ['deployci ALL=(root) NOPASSWD: /usr/local/sbin/selvage-install-shape ""'])


class ParserTest(unittest.TestCase):
    """The parts a stream is read with, called directly."""

    def test_the_name_grammar_admits_a_component_and_refuses_everything_else(self):
        admitted = ("compose.yaml", "proxy", "proxy/nginx.conf", "proxy/conf.d/default.conf", "proxy/www/a-b_c.html")
        refused = ("", "/", "/compose.yaml", "proxy/", ".", "..", "proxy/.", "proxy/..", "proxy//x", "proxy/x/", " x", "proxy/ x", "-x/compose.yaml", "proxy/x\n", "proxy/\\x")
        for name in admitted:
            with self.subTest(name=name):
                self.assertTrue(install_shape._NAME.match(name))
        for name in refused:
            with self.subTest(name=name):
                self.assertIsNone(install_shape._NAME.match(name))

    def test_the_set_is_compose_yaml_and_everything_under_proxy(self):
        self.assertTrue(install_shape._in_shape("compose.yaml", False))
        self.assertTrue(install_shape._in_shape("proxy/anything/at/all", False))
        self.assertFalse(install_shape._in_shape("compose.yaml", True))
        self.assertFalse(install_shape._in_shape("compose.yaml/x", False))
        self.assertFalse(install_shape._in_shape("proxytail/x", False))
        self.assertFalse(install_shape._in_shape("deploy/proxy/x", False))
        self.assertFalse(install_shape._in_shape(".env.example", False))
        self.assertTrue(install_shape._in_shape("proxy", True))
        self.assertFalse(install_shape._in_shape("proxy", False))

    def test_a_duplicated_entry_is_refused(self):
        stream = archive([("file", "compose.yaml", COMPOSE), ("file", "compose.yaml", COMPOSE)])
        with self.assertRaisesRegex(install_shape.Refused, "appears twice"):
            install_shape.read_stream(stream)

    def test_the_same_directory_twice_is_one_directory(self):
        stream = archive([("dir", "proxy/", b""), ("dir", "proxy", b""), ("file", "proxy/nginx.conf", b"x\n")])
        names = [entry.name for entry in install_shape.read_stream(stream)]
        self.assertEqual(names, ["proxy", "proxy/nginx.conf"])

    def test_a_pax_named_entry_and_a_gnu_named_entry_are_read(self):
        long_name = "proxy/www/" + "a" * 150 + ".html"
        for format in (tarfile.PAX_FORMAT, tarfile.GNU_FORMAT):
            with self.subTest(format=format):
                stream = archive([("file", long_name, b"x")], format=format)
                self.assertEqual([entry.name for entry in install_shape.read_stream(stream)], [long_name])

    def test_a_name_that_is_longer_than_the_bounded_read_is_still_read(self):
        """The bound is on the stream, not on one member's name."""
        served = archive([("file", "compose.yaml", b"x" * 200)])
        entries = install_shape.read_stream(served)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].content, b"x" * 200)


if __name__ == "__main__":
    unittest.main(verbosity=2)
