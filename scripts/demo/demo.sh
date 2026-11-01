#!/usr/bin/env bash
#
# One command: a server, a room, and every client this machine can open in it.
#
#   scripts/demo/demo.sh                 # Neovim hosting, Neovim joining, the page, the invite
#   scripts/demo/demo.sh --port 9000     # a port other than 8080
#   scripts/demo/demo.sh --keep          # keep the shared folder and logs when it exits
#   scripts/demo/demo.sh --agent         # ...and an MCP agent joining and editing, in a terminal
#   scripts/demo/demo.sh --no-terminal   # the clients run, but no window is opened for them
#   scripts/demo/demo.sh --no-browser    # do not open the invite in a browser
#
# The shared folder is a copy of `scripts/demo/workspace/` under a temporary directory, so a
# demo leaves nothing in the checkout. What a machine cannot open is printed rather than
# silently skipped: an editor with no terminal emulator to open it in, a browser with no
# opener, an agent with no MCP client to start it.
#
# The server serves the browser page itself (`--serve-page web_client/dist`), so the invite is
# one origin: the page a guest opens and the socket it dials are the same address.
set -euo pipefail

demo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
server_repo=$(cd -- "$demo_dir/../.." && pwd)
checkout=$(cd -- "$server_repo/.." && pwd)
nvim_client="$checkout/nvim_client"
web_client="$checkout/web_client"
mcp_repo="$checkout/mcp"

port=8080
keep=0
with_agent=0
open_browser=1
use_terminal=1

while (($# > 0)); do
  case "$1" in
    --port) port=${2:?--port needs a number}; shift 2 ;;
    --keep) keep=1; shift ;;
    --agent) with_agent=1; shift ;;
    --no-browser) open_browser=0; shift ;;
    --no-terminal) use_terminal=0; shift ;;
    -h|--help) sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "demo.sh: unknown argument $1" >&2; exit 2 ;;
  esac
done

say() { printf '%s\n' "$*"; }
fail() { printf 'demo.sh: %s\n' "$*" >&2; exit 1; }

command -v nvim >/dev/null || fail 'Neovim is not on PATH: the demo hosts and joins with it'
command -v node >/dev/null || fail 'Node is not on PATH: both Neovim instances run their sync engine on it'
[[ -d "$nvim_client" ]] || fail "no nvim_client checkout beside $server_repo"

# The server's address, spelled once: everything below hands a client this string.
server_url=$(printf 'ws://127.0.0.1:%s' "$port")

# The server: this checkout's own build when there is one, built otherwise.
selvaged="$server_repo/target/release/selvaged"
[[ -x "$selvaged" ]] || selvaged="$server_repo/target/debug/selvaged"
if [[ ! -x "$selvaged" ]]; then
  say 'building selvaged (no build in this checkout yet)'
  if command -v nix >/dev/null && [[ -f "$server_repo/flake.nix" ]]; then
    ( cd "$server_repo" && nix develop . -c cargo build -p selvaged )
  else
    ( cd "$server_repo" && cargo build -p selvaged )
  fi
  selvaged="$server_repo/target/debug/selvaged"
fi
[[ -x "$selvaged" ]] || fail "no selvaged at $selvaged"

# The page the server serves: built here when `web_client/dist` is not there yet.
page="$web_client/dist"
if [[ ! -f "$page/index.html" ]]; then
  [[ -d "$web_client" ]] || fail "no web_client checkout beside $server_repo, and no page to serve"
  say 'building the browser page (web_client/dist is not there yet)'
  ( cd "$web_client" && { [[ -d node_modules ]] || npm ci --no-audit --no-fund; } && npm run build )
fi

run_dir=$(mktemp -d "${TMPDIR:-/tmp}/selvage-demo.XXXXXX")
broadcast="$run_dir/invite.txt"
workspace="$run_dir/taskboard"
mkdir -p "$workspace"
cp -R "$demo_dir/workspace/." "$workspace/"

server_log="$run_dir/selvaged.log"
"$selvaged" --listen "127.0.0.1:$port" --serve-page "$page" >"$server_log" 2>&1 &
server_pid=$!

cleanup() {
  local status=$?
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  if (( keep )); then
    say "kept: $run_dir (server log: $server_log)"
  else
    rm -rf "$run_dir"
  fi
  return "$status"
}
trap cleanup EXIT INT TERM

for _ in $(seq 1 50); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$port/meta" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
curl -fsS -o /dev/null "http://127.0.0.1:$port/meta" || fail "the server did not answer on 127.0.0.1:$port"

# An editor per window, when this machine has a terminal emulator to open one in.
terminal=''
if (( use_terminal )); then
  for candidate in "${TERMINAL:-}" kitty alacritty foot wezterm ghostty xterm konsole; do
    if [[ -n "$candidate" ]] && command -v "${candidate%% *}" >/dev/null; then
      terminal=$candidate
      break
    fi
  done
fi

editor_env=(SELVAGE_DEMO_LINK_FILE="$broadcast" SELVAGE_DEMO_SERVER="$server_url" SELVAGE_DEMO_SCENE_DIR="$demo_dir/scene")
plugin_args=(--cmd "set rtp+=$nvim_client" -n)

open_editor() { # name, lua scene file, working directory, extra env...
  local name=$1 scene=$2 directory=$3
  shift 3
  if [[ -n "$terminal" ]]; then
    local -a terminal_cmd
    read -r -a terminal_cmd <<<"$terminal"
    ( cd "$directory" && env "${editor_env[@]}" SELVAGE_DEMO_NAME="$name" "$@" \
        "${terminal_cmd[@]}" -e nvim "${plugin_args[@]}" -c "luafile $scene" >/dev/null 2>&1 & )
  else
    ( cd "$directory" && env "${editor_env[@]}" SELVAGE_DEMO_NAME="$name" "$@" \
        nvim "${plugin_args[@]}" -c "luafile $scene" >>"$run_dir/$name.log" 2>&1 & )
  fi
}

open_editor Ada "$demo_dir/scene/host.lua" "$workspace"

for _ in $(seq 1 300); do
  [[ -s $broadcast ]] && break
  sleep 0.1
done
[[ -s $broadcast ]] || fail "the host wrote no invite; see $run_dir and the server log $server_log"
invite=$(cat "$broadcast")

# A guest keeps its own mirror: its own data directory, so it never shares the host's.
guest_home="$run_dir/guest"
mkdir -p "$guest_home/data" "$guest_home/state" "$guest_home/cache"
open_editor Grace "$demo_dir/scene/guest.lua" "$run_dir" \
  XDG_DATA_HOME="$guest_home/data" XDG_STATE_HOME="$guest_home/state" XDG_CACHE_HOME="$guest_home/cache"

if (( open_browser )); then
  if command -v xdg-open >/dev/null; then
    xdg-open "$invite" >/dev/null 2>&1 &
  elif command -v open >/dev/null; then
    open "$invite" >/dev/null 2>&1 &
  else
    say 'no browser opener here (xdg-open or open): paste the invite into a browser yourself'
  fi
fi

if (( with_agent )); then
  main_js="$mcp_repo/dist/main.js"
  if [[ ! -f $main_js ]]; then
    # shellcheck disable=SC2016 # the backticks are prose, not a command substitution
  say 'the MCP server is not built (mcp/dist/main.js): run `npm ci` in the mcp checkout'
  else
    agent_cmd=(env SELVAGE_DEMO_LINK_FILE="$broadcast" SELVAGE_MCP_MAIN="$main_js"
      SELVAGE_SERVER_URL="$server_url" SELVAGE_DISPLAY_NAME=agent
      node "$demo_dir/agent.mjs")
    if [[ -n $terminal ]]; then
      $terminal -e "${agent_cmd[@]}" >/dev/null 2>&1 &
    else
      say "start the agent with: ${agent_cmd[*]}"
    fi
  fi
fi

say ''
say 'Selvage demo'
say "  server    http://127.0.0.1:$port/meta   (log: $server_log)"
say "  folder    $workspace"
say "  invite    $invite"
say ''
if [[ -z $terminal ]]; then
  say 'No terminal emulator found, so the two editors are running without a window:'
  say "  Ada's output:   $run_dir/Ada.log"
  say "  Grace's output: $run_dir/Grace.log"
  say 'Open one yourself with:'
  say "  cd $workspace && SELVAGE_DEMO_LINK_FILE=$broadcast SELVAGE_DEMO_SERVER=$server_url \\"
  say "    SELVAGE_DEMO_SCENE_DIR=$demo_dir/scene SELVAGE_DEMO_NAME=Ada nvim -n \\"
  say "    --cmd 'set rtp+=$nvim_client' -c 'luafile $demo_dir/scene/host.lua'"
  say ''
fi
# shellcheck disable=SC2016 # the backticks are prose, not a command substitution
say 'A VS Code window joins the same room with `Selvage: Join a session from an invite link`'
say 'and the invite above; an agent seat joins with this MCP entry:'
say ''
say '  {"mcpServers":{"selvage":{"command":"node","args":["'"$mcp_repo"'/dist/main.js"],'
say "    \"env\":{\"SELVAGE_SERVER_URL\":\"$server_url\",\"SELVAGE_DISPLAY_NAME\":\"agent\"}}}"
say ''
say 'Press Ctrl-C to end the room.'
wait "$server_pid"
