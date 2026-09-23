#!/usr/bin/env bash
#
# The terms notice, read back out of the bytes the front serves.
#
#   packaging/prod/check-terms.sh
#
# The notice lives in two places, and both are asserted here against a running
# nginx rather than against the files that configure it:
#
#   - `proxy/www/terms.html`, which the `/terms` location serves, and which the
#     banner on the page links to;
#   - the banner `proxy/conf.d/default.conf` substitutes into the page's own
#     HTML before `</body>`.
#
# What matters is not that a string is in a file this repository owns, but that
# a visitor receives it. A `/terms` location that 404s, a page container that
# stops ending in `</body>`, or a licence that falls out of the notice in a
# rewrite are all invisible to a grep of the configuration and all fail here.
# `README.md` owns why the notice is the front's and not the page's; the
# deployment's own isolated proof on the box is also in `README.md`, and this
# script is the same proof's local half.
#
# No Docker: this host has none. The front's real configuration runs under
# nginx from the host, or from nixpkgs when there is no nginx on `PATH`. Two
# substitutions make that possible — the upstream names answered on loopback,
# and TLS off, because a location answers the same way over either — and each
# one is asserted below, so a change to the front's shape cannot be silently
# masked by the harness.
#
# Everything written stays under `.tmp/` in this checkout: `/tmp` is a
# RAM-backed tmpfs on some hosts, and building there has taken a machine down.
set -euo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd -- "$here/../.." && pwd)
proxy="$here/proxy"
work="$repo/.tmp/proxy-check-terms"

server_conf="$proxy/nginx.conf"
default_conf="$proxy/conf.d/default.conf"

if [ -n "${NGINX:-}" ]; then
    ngx=$NGINX
elif command -v nginx >/dev/null 2>&1; then
    ngx=$(command -v nginx)
elif command -v nix >/dev/null 2>&1; then
    ngx=$(nix build --no-link --print-out-paths nixpkgs#nginx)/bin/nginx
else
    printf 'no nginx on PATH and no nix to get one; nothing was proved\n' >&2
    exit 2
fi

failures=0
ok() { printf '  ok    %s\n' "$*"; }
bad() {
    printf '  FAIL  %s\n' "$*"
    failures=$((failures + 1))
}
say() { printf '\n=== %s ===\n' "$*"; }

# A free loopback port, asked of the kernel rather than assumed, so a run
# beside another nginx cannot collide or fail for the wrong reason.
free_port() {
    local port
    for port in $(seq "$1" "$2"); do
        if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            printf '%s\n' "$port"
            return 0
        fi
    done
    printf 'no free port in %s-%s\n' "$1" "$2" >&2
    return 1
}

rm -rf "$work"
mkdir -p "$work/conf.d" "$work/tmp" "$work/prefix" "$work/stub"

listen_port=$(free_port 18443 18462)
server_port=$(free_port 18463 18482)
page_port=$(free_port 18483 18502)

stub_pid=''
cleanup() {
    "$ngx" -c "$work/nginx.conf" -p "$work/prefix" -e "$work/error.log" -s stop >/dev/null 2>&1 || true
    if [ -n "$stub_pid" ]; then
        kill "$stub_pid" 2>/dev/null || true
        wait "$stub_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

say "the harness"

# The root the front serves the terms page from, read out of the configuration
# rather than repeated here: the Dockerfile below is asserted to install it
# there, so the two cannot drift apart in silence.
terms_root=$(sed -n 's/^[[:space:]]*root \(.*\);$/\1/p' "$default_conf")
if [ -z "$terms_root" ]; then
    printf 'no root directive in %s: the harness has nothing to point at\n' "$default_conf" >&2
    exit 2
fi

sed \
    -e "s#^pid /dev/shm/nginx.pid;#pid $work/nginx.pid;#" \
    -e "s#^error_log /dev/stderr crit;#error_log $work/error.log crit;#" \
    -e "s#/dev/shm/client_temp#$work/tmp/client_temp#" \
    -e "s#/dev/shm/proxy_temp#$work/tmp/proxy_temp#" \
    -e "s#/dev/shm/fastcgi_temp#$work/tmp/fastcgi_temp#" \
    -e "s#/dev/shm/uwsgi_temp#$work/tmp/uwsgi_temp#" \
    -e "s#/dev/shm/scgi_temp#$work/tmp/scgi_temp#" \
    -e "s#include /etc/nginx/conf.d/\*.conf;#include $work/conf.d/*.conf;#" \
    "$server_conf" >"$work/nginx.conf"

sed \
    -e "s#^\( *\)listen 8080 ssl;#\1listen 127.0.0.1:$listen_port;#" \
    -e "/^ *ssl_certificate/d" \
    -e "s#http://selvaged:8080#http://127.0.0.1:$server_port#g" \
    -e "s#http://selvage-web:8080#http://127.0.0.1:$page_port#g" \
    -e "s#^\( *\)root $terms_root;#\1root $proxy/www;#" \
    "$default_conf" >"$work/conf.d/default.conf"

cp "$proxy/conf.d/cloudflare-ips.conf" "$work/conf.d/"

# Every string the two `sed`s above rewrite must still be in the real file, and
# must be gone from the harness. A front whose shape changed — a third
# `proxy_pass`, a second listener, an upstream renamed — stops this script
# instead of being quietly rewritten into something that passes.
rewritten() {
    local needle=$1 file=$2
    if [ "$(grep -cF -- "$needle" "$proxy/$file" 2>/dev/null || true)" -lt 1 ]; then
        bad "'$needle' is gone from $file; this harness rewrites it and must be updated"
        return 0
    fi
    local harness_file="$work/$file"
    if grep -qF -- "$needle" "$harness_file" 2>/dev/null; then
        bad "'$needle' survived into the harness copy of $file"
    fi
}

rewritten '/dev/shm/nginx.pid' 'nginx.conf'
rewritten '/dev/stderr crit' 'nginx.conf'
rewritten '/dev/shm/client_temp' 'nginx.conf'
rewritten '/dev/shm/proxy_temp' 'nginx.conf'
rewritten '/dev/shm/fastcgi_temp' 'nginx.conf'
rewritten '/dev/shm/uwsgi_temp' 'nginx.conf'
rewritten '/dev/shm/scgi_temp' 'nginx.conf'
rewritten '/etc/nginx/conf.d/*.conf' 'nginx.conf'
rewritten 'listen 8080 ssl;' 'conf.d/default.conf'
rewritten 'http://selvaged:8080' 'conf.d/default.conf'
rewritten 'http://selvage-web:8080' 'conf.d/default.conf'
rewritten "root $terms_root;" 'conf.d/default.conf'

# The certificate pair is dropped rather than rewritten, so it is counted: the
# front terminates TLS and a copy of its configuration without a certificate is
# this script's, not the deployment's.
certs=$(grep -c '^ *ssl_certificate' "$default_conf" || true)
if [ "$certs" != 2 ]; then
    bad "$default_conf has $certs ssl_certificate lines, expected 2 (certificate and key)"
fi
if grep -q '^ *ssl_certificate' "$work/conf.d/default.conf"; then
    bad 'a ssl_certificate line survived into the harness'
fi

if grep -qF -- "www/ $terms_root" "$proxy/Dockerfile"; then
    ok "the image installs www/ at $terms_root, where the /terms location reads it"
else
    bad "the image does not install www/ at $terms_root, so the /terms location would 404"
fi

say "starting the front"

# The page container's stand-in. It ends in `</body>`, which is the whole of
# what the banner's substitution depends on, and it answers a sibling path so
# that `/termsomething` can be shown to reach the page rather than the terms
# file.
cat >"$work/stub/index.html" <<'HTML'
<!doctype html>
<html lang="en">
  <head><meta charset="utf-8" /><title>the page container's stand-in</title></head>
  <body><p>the page container's stand-in</p></body>
</html>
HTML
printf 'the page container answered this path\n' >"$work/stub/termsomething"

python3 -m http.server "$page_port" --bind 127.0.0.1 --directory "$work/stub" \
    >"$work/page.log" 2>&1 &
stub_pid=$!

"$ngx" -c "$work/nginx.conf" -p "$work/prefix" -e "$work/error.log"

deadline=$((SECONDS + 15))
until curl -sS -o /dev/null --max-time 2 "http://127.0.0.1:$listen_port/terms"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
        bad 'the front did not answer /terms within 15s'
        printf '  front log:\n'
        sed 's/^/    /' "$work/error.log" || true
        printf '  page log:\n'
        sed 's/^/    /' "$work/page.log" || true
        exit 1
    fi
    sleep 0.25
done
ok "the front is listening on 127.0.0.1:$listen_port"

say "the terms page, as served"

fetch() { curl -sS --max-time 5 -o "$2" -w '%{http_code} %{content_type}' "$1"; }

answer=$(fetch "http://127.0.0.1:$listen_port/terms" "$work/served-terms.html")
code=${answer%% *}
kind=${answer#* }
if [ "$code" = 200 ]; then ok "/terms answers $code"; else bad "/terms answers $code"; fi
case $kind in
text/html*) ok "/terms is served as $kind" ;;
*) bad "/terms is served as $kind, so a browser would show the page as source" ;;
esac

answer=$(fetch "http://127.0.0.1:$listen_port/terms/" "$work/served-terms-slash.html")
if [ "${answer%% *}" = 200 ]; then
    ok "/terms/ answers 200"
else
    bad "/terms/ answers ${answer%% *}: a link or a typed URL depends on the trailing slash"
fi
if cmp -s "$work/served-terms.html" "$work/served-terms-slash.html"; then
    ok '/terms and /terms/ are the same bytes'
else
    bad '/terms and /terms/ answer differently'
fi

if cmp -s "$work/served-terms.html" "$proxy/www/terms.html"; then
    ok 'the bytes served for /terms are the file in this repository'
else
    bad 'the bytes served for /terms are not www/terms.html'
fi

# The claims, in the text a reader sees. These are what the notice has to keep;
# the prose around them is free to change.
python3 - "$work/served-terms.html" >"$work/served-terms.txt" <<'PY'
import html.parser
import sys


class Text(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.quiet = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in ("style", "script", "title"):
            self.quiet += 1

    def handle_endtag(self, tag):
        if tag in ("style", "script", "title") and self.quiet:
            self.quiet -= 1

    def handle_data(self, data):
        if not self.quiet:
            self.parts.append(data)


reader = Text()
with open(sys.argv[1], encoding="utf-8") as page:
    reader.feed(page.read())
print(" ".join(" ".join(reader.parts).split()))
PY

while IFS= read -r claim; do
    if grep -qF -- "$claim" "$work/served-terms.txt"; then
        ok "the page says: $claim"
    else
        bad "the page no longer says: $claim"
    fi
done <<'CLAIMS'
non-commercial use only
personal and evaluation
not persisted
reset at any time
self-host
build with Selvage
MIT
Apache-2.0
FSL-1.1-MIT
CC-BY-4.0
selvage@dontblameme.dev
CLAIMS

# The links a reader of this page wants, by target. A dead one is worse than
# none, which is why the target is asserted and not the anchor text.
while IFS= read -r link; do
    if grep -qF -- "href=\"$link\"" "$work/served-terms.html"; then
        ok "the page links to $link"
    else
        bad "the page no longer links to $link"
    fi
done <<'LINKS'
/
https://github.com/selvage-protocol
https://github.com/selvage-protocol/reference_server
https://github.com/selvage-protocol/reference_server/blob/main/LICENSE-MIT
https://github.com/selvage-protocol/reference_server/blob/main/LICENSE-APACHE
https://github.com/selvage-protocol/reference_server/blob/main/crates/selvaged/LICENSE
https://github.com/selvage-protocol/specification/blob/main/LICENSE
https://selvage-protocol.vercel.app
mailto:selvage@dontblameme.dev
LINKS

say "the banner, as substituted into the page's own bytes"

fetch "http://127.0.0.1:$listen_port/" "$work/served-page.html" >/dev/null
while IFS= read -r fact; do
    if grep -qF -- "$fact" "$work/served-page.html"; then
        ok "the banner carries: $fact"
    else
        bad "the banner no longer carries: $fact"
    fi
done <<'BANNER'
<aside
non-commercial use only.
Not a hosted product; rooms are not persisted and may be reset at any time.
href="/terms"
BANNER

# The `location` is anchored, so a sibling path is the page's and not this
# page. Both are asked for: the front must have proxied the request on, and it
# must not have answered it with the terms page.
fetch "http://127.0.0.1:$listen_port/termsomething" "$work/served-other.html" >/dev/null
if cmp -s "$work/served-other.html" "$work/served-terms.html"; then
    bad '/termsomething is answered with the terms page'
fi
if grep -q 'GET /termsomething' "$work/page.log"; then
    ok 'a sibling path reaches the page container, not the terms file'
else
    bad '/termsomething did not reach the page container'
fi

say "result"
if [ "$failures" = 0 ]; then
    printf '  the notice reached the served bytes in every place it has to\n'
else
    printf '  %s assertion(s) failed\n' "$failures" >&2
fi
# The last command, so the script's status is this and the EXIT trap above still
# runs. `exit` here would be reported as making that trap unreachable.
[ "$failures" = 0 ]
