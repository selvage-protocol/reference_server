# The demo, and its recording

One command starts a server, a room, and every client this machine can open in it:

```sh
reference_server/scripts/demo/demo.sh
```

The script is `scripts/demo/demo.sh` in this repository. It builds `selvaged` when this checkout
has no build, builds the browser page when `web_client/dist` is missing, copies
`scripts/demo/workspace/` to a temporary folder, and starts:

- `selvaged` on `127.0.0.1:8080`, serving the page from the same origin, so the invite a person
  opens and the socket a client dials are one address;
- a Neovim hosting that folder, which prints the invite and writes it to the run's own file;
- a Neovim joining with that invite, in its own data directory;
- the page, in the machine's browser;
- with `--agent`, an MCP agent in its own terminal, joining the same room and editing a file.

It prints the invite, the folder it shares, the MCP entry an agent joins by, and how to stop. The
folder is a copy: nothing in the checkout is written. `--keep` leaves the copy, the server log and
each client's log behind. `--port` moves the server, `--no-browser` and `--no-terminal` run the
clients without opening windows.

What a machine cannot open is said rather than skipped: with no terminal emulator the two Neovim
instances run headless and the script prints the command that opens one in a window yourself; with
no browser opener it prints the invite to paste. A VS Code window joins with `Selvage: Join a
session from an invite link` and the same invite, and its MCP entry is the one printed.

## The recording

[`docs/images/selvage-demo.mp4`](images/selvage-demo.mp4) is 78 seconds of one run, silent, 1920×1080:
Ada hosts from Neovim, Grace joins from the invite link, both type in `src/limit.lua` at once, the
page joins as a third seat, and an MCP agent reads and edits the same file through its own server.
Presence, join and leave are the room's own, and the captions are the only drawn-on pixels.

`scripts/demo/record.sh` is what takes it: an X display of its own (`Xvfb`), two `kitty` windows, a
Chromium window and a terminal for the agent, captured frame by frame with `import`, then encoded
with `ffmpeg` at the rate the frames were taken. It needs `nix` for the display, the window mover and
the grabber, plus `ffmpeg`, `kitty` and a built `web_client/dist` and `mcp/dist/main.js`. The
captions and their timing are in the script, in one place.

```sh
scripts/demo/record.sh                 # → scripts/demo/out/selvage-demo.mp4
scripts/demo/record.sh --from-frames   # re-encode what the last run captured
```

The recording is made from the same scripts the demo runs — `scene/host.lua`, `scene/guest.lua`
and `agent.mjs` — with `SELVAGE_DEMO_TYPE` set, which is the one thing a recording adds: the text
the two editors type, so a run is the same length every time. Everything else is the clients'
own behaviour against a real server.
