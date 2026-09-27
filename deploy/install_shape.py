#!/usr/bin/env python3
"""Install the box's shape from a tar stream, then bring the front up to it.

Installed by hand as `/usr/local/sbin/selvage-install-shape`, root-owned and mode
0755, and one of the two commands the `deployci` user may run through sudo
(`deploy/installci.sudoers`, whose line ends in `""` so that sudo permits no
argument at all rather than any). That confinement is why this takes **no
arguments**: an argument to a script that reaches `docker compose` is a root shell
with extra steps. The shape arrives as a tar stream on stdin instead, and the
producer the runbook names is a bare-name archive of the two tracked paths:

    git archive --format=tar <sha>:deploy compose.yaml proxy | ssh <box> \
      '/usr/bin/sudo /usr/local/sbin/selvage-install-shape'

What the set is, and why it is those two: `compose.yaml` is the shape Compose
reads, and `proxy/` is the front's build context, whose files its image bakes at
`/etc/nginx/nginx.conf`, `/etc/nginx/conf.d/` and `/usr/share/selvage/www/` rather
than mounting. **`deploy/.env.example` is deliberately not in the set**: the file
it seeds, `/srv/selvage/.env`, is live — the deploy path rewrites its image lines
and the owner edits the rest — so this stays out of that pair entirely instead of
holding a lever on the file beside it. `.env.example` is copied once by hand at a
first install, the systemd units are installed by hand, and `tls/` is the box's
own; `docs/runbook-prod-demo.md` owns all three.

What a stream can and cannot do, because that is the whole of the argument:

* it may name entries of that set, by one fixed grammar: relative, plain names,
  no absolute path, no `.` or `..` component, and no link, hardlink, device or
  fifo member. Anything else is refused, and a refusal writes nothing;
* the whole stream is read and validated before the first write, so a stream
  refused at its last entry installs none of the ones before it;
* every step below `/srv/selvage` is opened inside the descriptor the step before
  it found, with `O_NOFOLLOW` on each, so nothing is validated as a path and then
  used as one, and a link planted in the way is refused rather than followed;
* it overwrites the shape's files and creates the directories it needs, and it
  **never deletes**: a file a release removes from `proxy/` stays in the build
  context until someone removes it by hand. That is the one residual of the
  install — deleting under a root process on the strength of a stream is a bigger
  lever than installing one needs.

And the residual that is a race rather than a decision: between the preflight and
the write, something with access to `/srv/selvage` (`selvage` owns it) could
replace a directory with a link. The write then refuses that entry, because every
step is opened with `O_NOFOLLOW` inside its parent — so a swap costs a failed
install, not an escape.

The install holds the lock the update timer and `selvage-deploy` hold, so a tick
cannot run `up -d` against a half-written build context.
"""

import fcntl
import io
import os
import re
import stat
import subprocess
import sys
import tarfile
import time
from pathlib import Path

SRV = Path("/srv/selvage")
# Shared with `selvage-update.service` and `selvage-deploy`.
LOCK = SRV / ".update.lock"
LOCK_WAIT_SECONDS = 600.0

FRONT = "proxy"
COMPOSE_ARGV = ["docker", "compose", "up", "-d", "--build", FRONT]
FILE_MODE = 0o644
DIRECTORY_MODE = 0o755

# The shape is 41 KB today; the bound is here so a caller cannot make a root
# process read for as long as it likes, the same reasoning as `deploy.py`'s
# request bound, with room for whatever the page brings with it.
MAX_STREAM_BYTES = 8 * 1024 * 1024

# One component: a plain name, so `.` and `..` are not names this grammar has and
# a stream has no room for a parser difference.
_COMPONENT = r"[A-Za-z0-9][A-Za-z0-9._-]*"
# `\Z` rather than `$`: `$` also matches before a trailing newline, and a name
# that ends in one is not a name this box takes.
_NAME = re.compile(rf"^{_COMPONENT}(?:/{_COMPONENT})*\Z")

COMPOSE = "compose.yaml"


class Refused(Exception):
    """The stream is not one this box acts on, or the install did not converge."""


class Entry:
    """One member of the stream that is in the set: its name, and its bytes."""

    def __init__(self, name: str, content: bytes | None):
        self.name = name
        self.content = content

    @property
    def is_directory(self) -> bool:
        return self.content is None


def _in_shape(name: str, directory: bool) -> bool:
    """`compose.yaml` the file, and `proxy` or anything below it."""
    if name == FRONT:
        return directory
    if name == COMPOSE:
        return not directory
    return name.startswith(f"{FRONT}/")


def _classify(member: tarfile.TarInfo) -> str:
    """The member's name, or `Refused` naming what about it is not allowed."""
    name = member.name.rstrip("/") if member.isdir() else member.name
    if member.issym():
        raise Refused(f"{name} is a symbolic link")
    if member.islnk():
        raise Refused(f"{name} is a hard link")
    if member.ischr() or member.isblk():
        raise Refused(f"{name} is a device node")
    if member.isfifo():
        raise Refused(f"{name} is a fifo")
    if not member.isfile() and not member.isdir():
        raise Refused(f"{name} is neither a regular file nor a directory")
    if not _NAME.match(name):
        raise Refused(f"{name} is not a relative path of plain names")
    if not _in_shape(name, member.isdir()):
        raise Refused(f"{name} is not part of the box's shape")
    return name


def read_stream(data: bytes) -> list[Entry]:
    """Every entry of `data`, validated, or `Refused` naming the first one that is not.

    Nothing here writes, and the caller installs only once this has returned: a
    stream whose last entry is refused leaves the box as it found it.
    """
    if not data:
        raise Refused("the stream is empty; send the shape as a tar archive")
    if len(data) > MAX_STREAM_BYTES:
        raise Refused(f"the stream is longer than {MAX_STREAM_BYTES} bytes")
    try:
        archive = tarfile.open(fileobj=io.BytesIO(data), mode="r:")
    except tarfile.TarError as error:
        raise Refused(f"the stream is not a tar archive: {error}") from error

    entries: list[Entry] = []
    seen: set[str] = set()
    with archive:
        for member in archive:
            name = _classify(member)
            if name in seen and member.isdir():
                continue
            if name in seen:
                raise Refused(f"{name} appears twice")
            seen.add(name)
            if member.isdir():
                entries.append(Entry(name, None))
                continue
            content = archive.extractfile(member)
            if content is None:
                raise Refused(f"{name} carries no content")
            entries.append(Entry(name, content.read()))

    if not any(not entry.is_directory for entry in entries):
        raise Refused("the stream carries no file of the box's shape")
    return entries


def open_below(relative: str, create: bool = False) -> int:
    """The descriptor of `relative` below `SRV`, or an `OSError` saying why not.

    Each component is opened inside the descriptor the component before it found
    and with `O_NOFOLLOW`, so a link at any step is refused rather than followed
    and no step is a path that was checked and then used. `create` makes a missing
    component, and gives it to the owner of its parent, so the box's own user can
    still edit what root installed.
    """
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptor = os.open(SRV, flags)
    try:
        for part in relative.split("/") if relative else []:
            try:
                step = os.open(part, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(part, DIRECTORY_MODE, dir_fd=descriptor)
                owner = os.fstat(descriptor)
                step = os.open(part, flags, dir_fd=descriptor)
                os.fchmod(step, DIRECTORY_MODE)
                os.fchown(step, owner.st_uid, owner.st_gid)
            os.close(descriptor)
            descriptor = step
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def preflight(entries: list[Entry]) -> None:
    """Every entry resolves inside `SRV` as far as it exists, before any write.

    A component that is not there ends the walk: below it the install creates its
    own directories, and a link cannot be one of those. A component that is there
    and is not a directory — a link out of the box, a file where a directory
    belongs, a shape file that has become a link — is refused here, with the box
    untouched.
    """
    for entry in entries:
        parts = entry.name.split("/")
        # A directory member's own name is a step; a file's last part is not, and
        # is looked at as a leaf instead.
        steps = parts if entry.is_directory else parts[:-1]
        for stop in range(len(steps) + 1):
            try:
                os.close(open_below("/".join(steps[:stop])))
            except FileNotFoundError:
                # From here down the install makes its own directories, and a link
                # cannot be one of those.
                break
            except OSError as error:
                where = f"{SRV}/{'/'.join(steps[:stop])}"
                raise Refused(f"{entry.name} does not resolve: {where} is not a directory ({error.strerror})") from error
        if entry.is_directory:
            continue
        try:
            parent = open_below("/".join(parts[:-1]))
        except OSError:
            # The steps above already refuse whatever the parent walk can find
            # wrong; a change between the two walks costs the install, which then
            # fails on the write rather than following anything.
            continue
        try:
            leaf = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            continue
        finally:
            os.close(parent)
        if not stat.S_ISREG(leaf.st_mode):
            raise Refused(f"{entry.name} is there and is not a regular file")


def install(entries: list[Entry]) -> list[str]:
    """Write the shape under `SRV`, in name order, and return what was written."""
    written = []
    for entry in sorted(entries, key=lambda item: item.name):
        if entry.is_directory:
            os.close(open_below(entry.name, create=True))
            continue
        parent_name, _, leaf = entry.name.rpartition("/")
        descriptor = open_below(parent_name, create=True)
        try:
            owner = os.fstat(descriptor)
            handle = os.open(
                leaf,
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW,
                FILE_MODE,
                dir_fd=descriptor,
            )
            with os.fdopen(handle, "wb") as stream:
                stream.write(entry.content)
                os.fchmod(stream.fileno(), FILE_MODE)
                os.fchown(stream.fileno(), owner.st_uid, owner.st_gid)
        finally:
            os.close(descriptor)
        written.append(entry.name)
    return written


def take_lock() -> int:
    """Hold the lock the update timer takes, waiting out a tick in progress."""
    descriptor = os.open(LOCK, os.O_RDONLY | os.O_CREAT | os.O_NOFOLLOW, 0o644)
    deadline = time.monotonic() + LOCK_WAIT_SECONDS
    while True:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return descriptor
        except BlockingIOError:
            if time.monotonic() >= deadline:
                os.close(descriptor)
                raise Refused(
                    f"{LOCK} is still held after {LOCK_WAIT_SECONDS:.0f}s; "
                    f"`systemctl status selvage-update.service` shows what holds it"
                ) from None
            time.sleep(1)


def compose() -> subprocess.CompletedProcess:
    """The one command that makes an installed `proxy/` live: as root, in `SRV`.

    No `-f`: Compose finds `compose.yaml` in the working directory, and a
    `compose.override.yaml` beside it too, exactly as the timer and every hand
    command do. The front's files are baked into its image, so an installed
    `proxy/` is not what runs until this rebuilds it.
    """
    return subprocess.run(COMPOSE_ARGV, cwd=SRV, check=False)


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("selvage-install-shape takes no arguments; the shape arrives on stdin", file=sys.stderr)
        return 2
    if os.geteuid() != 0:
        print("selvage-install-shape must run as root", file=sys.stderr)
        return 2

    try:
        entries = read_stream(sys.stdin.buffer.read(MAX_STREAM_BYTES + 1))
        preflight(entries)
        lock = take_lock()
        try:
            written = install(entries)
            print("=== installed ===")
            for name in written:
                print(f"{SRV}/{name}")
            print(f"left alone: {SRV}/.env (live; selvage-deploy and the owner write it)")
            print(f"left alone: {SRV}/.env.example, tls/ and the systemd units (never part of the shape)")
            print(f"=== docker compose {' '.join(COMPOSE_ARGV[2:])} ===")
            if compose().returncode != 0:
                raise Refused(
                    f"`docker compose {' '.join(COMPOSE_ARGV[2:])}` failed; the shape is on disk "
                    f"and the front was not rebuilt, so it goes on serving the previous one"
                )
        finally:
            os.close(lock)
        return 0
    except Refused as refusal:
        print(f"refused: {refusal}", file=sys.stderr)
        return 1
    except OSError as error:
        print(f"refused: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
