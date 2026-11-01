#!/usr/bin/env bash
#
# Records the demo: one `selvaged`, two Neovim windows, the browser page and an MCP agent seat,
# all in the same room, captured frame by frame on an X display of this script's own.
#
#   scripts/demo/record.sh                      # → scripts/demo/out/selvage-demo.mp4
#   scripts/demo/record.sh --out /tmp/demo.mp4
#   scripts/demo/record.sh --from-frames        # re-encode what the last run captured
#
# Everything the video shows is a real client against a real server: the edits are typed into one
# editor and arrive in the others, the agent's edit is a `tools/call` to the MCP server, and the
# frames are the display itself rather than anything drawn afterwards. The captions and the two
# title cards are the only added pixels.
#
# It needs `nix` for the display, the window mover and the frame grabber (`xvfb-run`, `xdotool`,
# `imagemagick`), plus `ffmpeg`, `kitty`, a browser and this checkout's `web_client/dist`.
set -euo pipefail

demo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
server_repo=$(cd -- "$demo_dir/../.." && pwd)
checkout=$(cd -- "$server_repo/.." && pwd)
out="$demo_dir/out/selvage-demo.mp4"
from_frames=0

while (($# > 0)); do
  case "$1" in
    --out) out=${2:?--out needs a path}; shift 2 ;;
    --from-frames) from_frames=1; shift ;;
    -h|--help) sed -n '2,18p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "record.sh: unknown argument $1" >&2; exit 2 ;;
  esac
done

width=1920
height=1080
fps=5
run="$server_repo/.tmp/demo-record"
frames="$run/frames"

encode() {
  local captions=(
    "1 · Ada hosts a room, from her own Neovim"
    "2 · Grace joins from the invite link"
    "3 · Both edit the same file at once"
    "4 · The page joins as a third seat"
    "5 · An MCP agent joins the room and edits"
    "6 · They leave; the host ends the room"
  )
  local windows=("2" "13" "20" "38" "49" "64")
  local filter="scale=${width}:${height}"
  local index=0
  for caption in "${captions[@]}"; do
    local from=${windows[$index]}
    local to
    if (( index + 1 < ${#windows[@]} )); then
      to=${windows[$((index + 1))]}
    else
      to=999
    fi
    local escaped=${caption//:/\\:}
    escaped=${escaped//\'/}
    filter+=",drawbox=x=0:y=h-96:w=iw:h=96:color=black@0.72:t=fill:enable='between(t,${from},${to})'"
    filter+=",drawtext=fontfile=$font:text='${escaped}':fontcolor=white:fontsize=36:x=64:y=h-66:enable='between(t,${from},${to})'"
    index=$((index + 1))
  done
  local rate=$fps
  if [[ -s $frames/../frame-times.txt ]]; then
    rate=$(awk 'NR==1 { first = $1 } { last = $1; count++ } END {
      if (count > 1 && last > first) printf "%.4f", (count - 1) * 1000 / (last - first);
      else printf "5";
    }' "$frames/../frame-times.txt")
  fi
  printf 'frames taken at %s fps\n' "$rate"
  ffmpeg -y -loglevel error -start_number 0 -framerate "$rate" -i "$frames/f%05d.png" \
    -vf "$filter" -c:v libx264 -preset slow -crf 20 -pix_fmt yuv420p -movflags +faststart "$out"
  printf 'wrote %s (%s)\n' "$out" "$(du -h "$out" | cut -f1)"
}

font=$(fc-match -f '%{file}' 'Liberation Sans' 2>/dev/null || true)
if [[ -z $font ]]; then
  font=$(fc-match -f '%{file}' 'sans' 2>/dev/null || true)
fi

if (( from_frames )); then
  [[ -d $frames ]] || { echo "record.sh: no frames under $frames" >&2; exit 1; }
  encode
  exit 0
fi

for tool in ffmpeg kitty nix node nvim; do
  command -v "$tool" >/dev/null || { echo "record.sh: $tool is not on PATH" >&2; exit 1; }
done
[[ -f "$checkout/web_client/dist/index.html" ]] || {
  echo "record.sh: build the page first (npm ci && npm run build in web_client)" >&2
  exit 1
}
[[ -f "$checkout/mcp/dist/main.js" ]] || {
  echo "record.sh: build the MCP server first (npm ci in mcp)" >&2
  exit 1
}

selvaged="$server_repo/target/release/selvaged"
[[ -x $selvaged ]] || selvaged="$server_repo/target/debug/selvaged"
[[ -x $selvaged ]] || { echo "record.sh: no selvaged; run scripts/demo/demo.sh once" >&2; exit 1; }

rm -rf "$run"
mkdir -p "$frames" "$run/taskboard"
cp -R "$demo_dir/workspace/." "$run/taskboard/"
port=8079
server_url=$(printf 'ws://127.0.0.1:%s' "$port")

table=$(cat <<'SCRIPT'
set -uo pipefail
export PATH=/etc/profiles/per-user/user/bin:$PATH

width=1920
height=1080
run=__RUN__
demo_dir=__DEMO_DIR__
server_repo=__SERVER_REPO__
checkout=__CHECKOUT__
selvaged=__SELVAGED__
server_url=__SERVER_URL__
port=__PORT__
workspace=$run/taskboard
broadcast=$run/invite.txt
kitten=/etc/profiles/per-user/user/bin/kitty

Xvfb :99 -screen 0 ${width}x${height}x24 -nolisten tcp >$run/xvfb.log 2>&1 &
xvfb_pid=$!
sleep 2
export DISPLAY=:99
unset WAYLAND_DISPLAY

place() { # window title, x, y, width, height
  local id
  for _ in $(seq 1 100); do
    id=$(xdotool search --name "^$1\$" 2>/dev/null | head -1)
    [[ -n $id ]] && break
    sleep 0.1
  done
  [[ -n $id ]] || { echo "no window named $1" >>$run/record.log; return 1; }
  xdotool windowmove "$id" "$2" "$3" windowsize "$id" "$4" "$5"
  xdotool windowraise "$id"
}

# The display as it is, frame by frame, for as long as the flag file stands.
: > $run/capturing
(
  index=0
  while [[ -f $run/capturing ]]; do
    import -window root -silent "$run/frames/f$(printf %05d $index).png" 2>/dev/null || true
    date +%s%3N >> "$run/frame-times.txt"
    index=$((index + 1))
    sleep 0.19
  done
) &
capture_pid=$!

t0=$(date +%s%3N)
now() { echo $(( $(date +%s%3N) - t0 )); }
until_at() { # milliseconds
  local target=$1
  while (( $(now) < target )); do sleep 0.05; done
}

"$selvaged" --listen 127.0.0.1:$port --serve-page "$checkout/web_client/dist" >$run/selvaged.log 2>&1 &
for _ in $(seq 1 100); do curl -fsS -o /dev/null http://127.0.0.1:$port/meta 2>/dev/null && break; sleep 0.1; done

echo "host window" >>$run/record.log
( cd "$workspace" && env \
    SELVAGE_DEMO_LINK_FILE=$broadcast SELVAGE_DEMO_SERVER=$server_url \
    SELVAGE_DEMO_SCENE_DIR=$demo_dir/scene SELVAGE_DEMO_NAME=Ada \
    SELVAGE_DEMO_TYPE='M.wip_cap = 2' SELVAGE_DEMO_TYPE_DELAY=6000 \
    $kitten --title Ada -o initial_window_width=660 -o initial_window_height=1010 \
      -o font_size=13 -o confirm_os_window_close=0 \
      nvim -n --listen $run/ada.sock -u "$demo_dir/scene/init.lua" \
      --cmd "set rtp+=${checkout}/nvim_client" \
      -c "luafile $demo_dir/scene/host.lua" >$run/ada.log 2>&1 & )
place Ada 0 0 660 1010 || true

for _ in $(seq 1 300); do [[ -s $broadcast ]] && break; sleep 0.1; done
invite=$(cat "$broadcast")
echo "invite $invite" >>$run/record.log

until_at 13000
echo "guest window" >>$run/record.log
mkdir -p /tmp/sv-guest/data /tmp/sv-guest/state /tmp/sv-guest/cache
rm -rf /tmp/sv-guest/cache/*
( cd "$run" && env \
    SELVAGE_DEMO_LINK_FILE=$broadcast SELVAGE_DEMO_SERVER=$server_url \
    SELVAGE_DEMO_SCENE_DIR=$demo_dir/scene SELVAGE_DEMO_NAME=Grace \
    XDG_DATA_HOME=/tmp/sv-guest/data XDG_STATE_HOME=/tmp/sv-guest/state XDG_CACHE_HOME=/tmp/sv-guest/cache \
    SELVAGE_DEMO_TYPE=$'M.done_hidden = false\n' SELVAGE_DEMO_TYPE_DELAY=7000 \
    $kitten --title Grace -o initial_window_width=660 -o initial_window_height=1010 \
      -o font_size=13 -o confirm_os_window_close=0 \
      nvim -n --listen $run/grace.sock -u "$demo_dir/scene/init.lua" \
      --cmd "set rtp+=${checkout}/nvim_client" \
      -c "luafile $demo_dir/scene/guest.lua" >$run/grace.log 2>&1 & )
place Grace 660 0 660 1010 || true

until_at 38000
echo "browser window" >>$run/record.log
chromium --ozone-platform=x11 --user-data-dir=$run/chrome --no-first-run \
  --no-default-browser-check --disable-features=Translate \
  --remote-debugging-port=9222 --app=about:blank --kiosk \
  --window-size=600,560 --window-position=1320,0 >$run/chrome.log 2>&1 &
for _ in $(seq 1 300); do
  id=$(xdotool search --class chromium 2>/dev/null | head -1)
  [[ -n $id ]] && break
  sleep 0.1
done
[[ -n ${id:-} ]] && xdotool windowmove "$id" 1320 0 windowsize "$id" 600 560 windowraise "$id"
sleep 3
[[ -n ${id:-} ]] && xdotool windowmove "$id" 1320 0 windowsize "$id" 600 560 windowraise "$id"
node "$demo_dir/browser.mjs" --port 9222 --link "$invite" \
  --name 'Grace (browser)' >>$run/browser.log 2>&1 || echo "the browser seat did not join" >>$run/browser.log

until_at 48000
echo "agent window" >>$run/record.log
( env SELVAGE_DEMO_LINK_FILE=$broadcast SELVAGE_MCP_MAIN=$checkout/mcp/dist/main.js \
    SELVAGE_SERVER_URL=$server_url SELVAGE_DISPLAY_NAME=agent \
    $kitten --title Agent -o initial_window_width=600 -o initial_window_height=450 \
      -o font_size=11 -o confirm_os_window_close=0 \
      bash -c 'node __DEMO_DIR__/agent.mjs; sleep 600' >$run/agent.log 2>&1 & )
place Agent 1320 560 600 450 || true

until_at 64000
echo "guest leaves" >>$run/record.log
nvim --server "$run/grace.sock" --remote-send '<C-\><C-N>:SelvageLeave<CR>' 2>/dev/null || true

until_at 72000
echo "host leaves" >>$run/record.log
nvim --server "$run/ada.sock" --remote-send '<C-\><C-N>:qa!<CR>' 2>/dev/null || true

until_at 78000
rm -f $run/capturing
wait $capture_pid 2>/dev/null || true
kill $xvfb_pid 2>/dev/null || true
pkill -f "user-data-dir=$run/chrome" 2>/dev/null || true
echo "done" >>$run/record.log
SCRIPT
)

table=${table//__RUN__/$run}
table=${table//__DEMO_DIR__/$demo_dir}
table=${table//__SERVER_REPO__/$server_repo}
table=${table//__SERVER_URL__/$server_url}
table=${table//__CHECKOUT__/$checkout}
table=${table//__SELVAGED__/$selvaged}
table=${table//__PORT__/$port}

mkdir -p "$run"
printf '%s\n' "$table" > "$run/record-inner.sh"

nix shell nixpkgs#xorgserver nixpkgs#xdotool nixpkgs#imagemagick \
  -c bash -c "bash '$run/record-inner.sh'" > "$run/record.log" 2>&1 || {
    echo "record.sh: the recording run failed; see $run/record.log" >&2
    tail -20 "$run/record.log" >&2
    exit 1
  }

mkdir -p "$(dirname -- "$out")"
encode
