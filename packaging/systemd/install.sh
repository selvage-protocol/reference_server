#!/usr/bin/env bash
# Install selvaged as a systemd user unit. User scope only — no root, no
# packages. Run from the repository root: packaging/systemd/install.sh [binary]
set -euo pipefail

binary="${1:-$HOME/selvage/selvaged}"
dest="$HOME/selvage"
unit="$HOME/.config/systemd/user/selvaged.service"

mkdir -p "$dest" "$(dirname "$unit")"
if [ "$binary" != "$dest/selvaged" ]; then
    install -m755 "$binary" "$dest/selvaged"
fi
# A bare run defaults to the destination itself, skipping the copy above:
# refuse to enable a unit that would point at a missing executable.
if [ ! -x "$dest/selvaged" ]; then
    echo "no executable at $dest/selvaged — pass the built binary: $0 <binary>" >&2
    exit 1
fi
install -m644 packaging/systemd/selvaged.service "$unit"
systemctl --user daemon-reload
loginctl enable-linger "$USER" || echo "linger refused; the unit still runs while you are logged in"
systemctl --user enable --now selvaged
systemctl --user is-active selvaged
echo "log: tail -f $dest/selvaged.log"
