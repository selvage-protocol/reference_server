#!/usr/bin/env python3
"""The front's own contribution to a response: the name it answers, its
per-source limits, and the `Strict-Transport-Security` field it deliberately
does not add.

    python3 -B test_front_limits.py

The front is the only layer in this deployment with a per-source view: the
server sees connections, not who opened them, so `PROTOCOL.md` §12's connection
cap and rate limit are kept here or nowhere. What matters is therefore not what
the zones say in the file but that a source is refused, and that one endpoint's
traffic cannot spend another's budget — the page reads `/meta` on every load and
opens `/session` on the click that follows, so a shared counter is a visitor
refused at the join.

All of it is driven against the front's real `nginx.conf` and
`conf.d/default.conf`, under nginx with the two upstream names answered on
loopback and TLS off, exactly as `check-terms.sh` does it. Five claims cannot
be made by reading the files and are made by measuring:

  - a request naming a Host the front is not the server for is closed rather
    than proxied to the demo's locations, and one naming the front's own Host is
    served — `selvage.dontblameme.dev` is the project's landing page, served
    somewhere else entirely;
  - `/session` refuses past its own burst (`limit_req`), with a real 429;
  - `/meta` refuses past its own concurrency (`limit_conn`), with a real 429;
  - a source that has just spent one metered endpoint is *not* refused on the
    other;
  - the front serves no `Strict-Transport-Security` field of its own, on any of
    its three locations.

Three of them come with their own control, in this file: the same run against a
copy of the configuration with the host guard removed, one whose `/meta` names
the `/session` request zone, and one that adds an HSTS field to the server
block. Each copy must *fail* the claim it is the control for. A control that
passes would mean the test cannot see the defect it exists for.

Every client is a loopback address of its own (`127.0.0.2` and up), because the
zones are keyed on `$binary_remote_addr`. A fresh address is a fresh bucket, so
no claim here waits for a rate to refill, and no assertion is made about a
sleep: the status of the request that follows is the measured thing.
"""

import http.client
import http.server
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time

# The harness's own upstream host, assembled rather than written as one
# `http://127.0.0.1:port` literal: a bare URL in a text file is something the
# repository's link check fetches, and a loopback port is not fetchable.
LOOPBACK = "127.0.0.1"
HERE = os.path.dirname(os.path.abspath(__file__))
PROXY = os.path.join(HERE, "proxy")
SERVER_CONF = os.path.join(PROXY, "nginx.conf")
DEFAULT_CONF = os.path.join(PROXY, "conf.d", "default.conf")

failures = []


def ok(what):
    print(f"  ok    {what}")


def bad(what):
    print(f"  FAIL  {what}")
    failures.append(what)


def say(what):
    print(f"\n=== {what} ===")


def nginx_binary():
    if os.environ.get("NGINX_BIN"):
        return os.environ["NGINX_BIN"]
    found = shutil.which("nginx")
    if found:
        return found
    print("no nginx on PATH and no NGINX_BIN set; nothing was proved", file=sys.stderr)
    raise SystemExit(2)


def work_root():
    """Where the harness writes. Not `/tmp` by default: it is a RAM-backed tmpfs
    on this project's host, and a build there has taken a machine down."""
    root = os.environ.get("FRONT_LIMITS_WORKDIR")
    if not root:
        root = os.path.join(os.path.dirname(os.path.dirname(HERE)), ".tmp", "front-limits")
    shutil.rmtree(root, ignore_errors=True)
    return root


def server_name(text):
    """The one name the front's block is the server for, read out of the shipped
    configuration rather than repeated here.

    Every request below has to name it: the front closes the connection for any
    other Host, so a harness asking under a name of its own would fail every
    claim for the wrong reason. The first name is the one the guard compares
    against, which is why only it is read.
    """
    found = re.search(r"^\s*server_name\s+([^;\s]+);", text, re.M)
    if not found:
        print(f"no server_name in {DEFAULT_CONF}: the harness has no Host to ask for", file=sys.stderr)
        raise SystemExit(2)
    return found.group(1)


# ---------------------------------------------------------------- the rewrite
#
# Every substitution the harness makes is asserted in both directions: the
# needle has to be in the real file, and it has to be gone from the harness
# copy. A front whose shape changed — a second listener, an upstream renamed, a
# root moved — stops this script instead of being quietly rewritten into
# something that passes.

def rewrite(text, pairs, where):
    for needle, replacement in pairs:
        if needle not in text:
            bad(f"'{needle}' is gone from {where}; this harness rewrites it and must be updated")
            continue
        text = text.replace(needle, replacement)
        if needle in text:
            bad(f"'{needle}' survived into the harness copy of {where}")
    return text


def harness_configs(work, listen_port, stub_port, default_text=None):
    os.makedirs(os.path.join(work, "conf.d"), exist_ok=True)
    os.makedirs(os.path.join(work, "tmp"), exist_ok=True)
    os.makedirs(os.path.join(work, "prefix"), exist_ok=True)

    with open(SERVER_CONF, encoding="utf-8") as handle:
        server = handle.read()
    server = rewrite(server, [
        ("pid /dev/shm/nginx.pid;", f"pid {work}/nginx.pid;"),
        ("error_log /dev/stderr crit;", f"error_log {work}/error.log crit;"),
        ("/dev/shm/client_temp", f"{work}/tmp/client_temp"),
        ("/dev/shm/proxy_temp", f"{work}/tmp/proxy_temp"),
        ("/dev/shm/fastcgi_temp", f"{work}/tmp/fastcgi_temp"),
        ("/dev/shm/uwsgi_temp", f"{work}/tmp/uwsgi_temp"),
        ("/dev/shm/scgi_temp", f"{work}/tmp/scgi_temp"),
        ("include /etc/nginx/conf.d/*.conf;", f"include {work}/conf.d/*.conf;"),
    ], "nginx.conf")

    if default_text is None:
        with open(DEFAULT_CONF, encoding="utf-8") as handle:
            default_text = handle.read()
    root = re.search(r"^\s*root (.*);$", default_text, re.M)
    if not root:
        bad(f"no root directive in {DEFAULT_CONF}: the harness has nothing to point /terms at")
    pairs = [
        ("listen 8080 ssl;", f"listen 127.0.0.1:{listen_port};"),
        ("http://selvaged:8080", f"http://{LOOPBACK}:{stub_port}"),
        ("http://selvage-web:8080", f"http://{LOOPBACK}:{stub_port}"),
    ]
    if root:
        pairs.append((f"root {root.group(1)};", f"root {PROXY}/www;"))
    default = rewrite(default_text, pairs, "conf.d/default.conf")

    certs = len(re.findall(r"^\s*ssl_certificate", default, re.M))
    if certs != 2:
        bad(f"{DEFAULT_CONF} has {certs} ssl_certificate lines, expected 2")
    default = re.sub(r"^\s*ssl_certificate.*$", "", default, flags=re.M)
    if re.search(r"^\s*ssl_certificate", default, re.M):
        bad("an ssl_certificate line survived into the harness")

    with open(os.path.join(work, "nginx.conf"), "w", encoding="utf-8") as handle:
        handle.write(server)
    with open(os.path.join(work, "conf.d", "default.conf"), "w", encoding="utf-8") as handle:
        handle.write(default)
    shutil.copy(os.path.join(PROXY, "conf.d", "cloudflare-ips.conf"), os.path.join(work, "conf.d"))
    return default


# ----------------------------------------------------------------- the stub
#
# Stands in for `selvaged` and `selvage-web` at once. It answers everything
# `200 text/html` and ends in `</body>` so the banner's substitution has
# something to substitute into. `/meta?slow=1` is held open for a moment, so
# that concurrent connections genuinely overlap: `limit_conn` counts requests in
# flight, and against a stub that answers instantly nothing ever is.

class Stub(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):  # noqa: N802 - http.server's naming
        if self.path.startswith("/meta") and "slow=1" in self.path:
            time.sleep(0.6)
        body = b"<html><body>the upstream's stand-in</body></html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


def start_stub():
    stub = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Stub)
    threading.Thread(target=stub.serve_forever, daemon=True).start()
    return stub, stub.server_address[1]


# --------------------------------------------------------------- the clients

def source_conn(port, source, timeout=15):
    """A client whose source address is `source`, so its zone key is its own."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.bind((source, 0))
    sock.connect(("127.0.0.1", port))
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    conn.sock = sock
    return conn


def get_response(conn, path, host):
    conn.request("GET", path, headers={"Host": host, "User-Agent": "front-limits"})
    response = conn.getresponse()
    headers = {name.lower(): value for name, value in response.getheaders()}
    response.read()
    return response.status, headers


def get(conn, path, host):
    return get_response(conn, path, host)[0]


def attempt(port, source, path, host):
    """One request's outcome: the status the front answered, or how the connection
    ended when it answered nothing.

    `444` is a connection closed with no response, which `http.client` reports as
    the peer hanging up rather than as a status, so that is what a refusal reads
    as here.
    """
    conn = source_conn(port, source)
    try:
        return get(conn, path, host), ""
    except (http.client.HTTPException, OSError) as error:
        return None, type(error).__name__
    finally:
        conn.close()


def refuse_after(conn, path, budget, host):
    """Send until the front refuses, and report which request was refused."""
    for index in range(1, budget + 1):
        if get(conn, path, host) == 429:
            return index
    return None


# ---------------------------------------------------------------- the phases

def phase_host_routing(port, label, host, expect_wrong_served):
    """The Host a request names, not the address it arrives at, decides whether
    the front serves it.

    The demo's block is the only one on its port, so nginx makes it the default
    and would otherwise answer any name that resolves to this origin —
    `selvage.dontblameme.dev` is the project's landing page, served somewhere
    else entirely, and every stray name pointed here would reach the demo's
    locations too. What the front does with the refused name is its own choice;
    `444` closes the connection without a response, which is what `attempt`
    reports as a failure rather than a status.

    The refusal is a negative, so the claim carries its own control: the same run
    against a copy with the guard removed has to *serve* the wrong name, or the
    claim cannot see the defect it exists for.
    """
    wrong = "selvage.dontblameme.dev"
    for path, what in (("/", "the page"), ("/meta", "the server's JSON")):
        status, how = attempt(port, "127.0.0.7", path, host)
        if status == 200:
            ok(f"[{label}] {path} named {host} is served ({what})")
        else:
            bad(f"[{label}] {path} named the front's own Host answered {status or how}: the demo stopped answering")

    for path in ("/", "/meta"):
        status, how = attempt(port, "127.0.0.8", path, wrong)
        served = status == 200
        if expect_wrong_served:
            if served:
                ok(f"[{label}] the copy with no host guard served {path} for {wrong}: this claim can see the defect")
            else:
                bad(f"[{label}] the control removes the host guard and {path} for {wrong} answered {status or how}: the claim cannot see the defect")
        elif served:
            bad(f"[{label}] {path} named {wrong} was served ({status}): the front answers a name it is not the server for")
        else:
            ok(f"[{label}] {path} named {wrong} was not served ({status or how})")


def phase_own_limits(port, host):
    say("each metered endpoint refuses on its own account")
    conn = source_conn(port, "127.0.0.3")
    refused = refuse_after(conn, "/session", 30, host)
    conn.close()
    if refused:
        ok(f"/session was refused (429) at request {refused} of 30 from one source")
    else:
        bad("/session was never refused in 30 requests: its rate limit is not in force")

    # Six sockets at once from one source, each held open by the stub, against
    # `limit_conn permeta 4`. Separate sockets, one address: the same key.
    statuses = [None] * 6
    gate = threading.Barrier(len(statuses))

    def reader(index):
        try:
            conn = source_conn(port, "127.0.0.4")
            gate.wait(timeout=10)
            statuses[index] = get(conn, "/meta?slow=1", host)
            conn.close()
        except Exception as error:  # noqa: BLE001 - reported, not raised
            statuses[index] = f"error {error}"

    threads = [threading.Thread(target=reader, args=(index,)) for index in range(len(statuses))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if 429 in statuses:
        ok(f"6 concurrent /meta reads from one source were refused: {statuses}")
    else:
        bad(f"6 concurrent /meta reads from one source were all served: {statuses} — limit_conn is not in force")


def phase_independence(port, label, host, expect_refusal=False):
    """Spend `/meta` from one source, then ask that same source for `/session`.

    The claim is about the status of the request that follows the spend, not
    about a sleep: the source is one whose buckets are otherwise untouched, and
    the spend is not reported as having happened unless `/meta` was really
    refused first.
    """
    conn = source_conn(port, "127.0.0.2")
    spent_at = refuse_after(conn, "/meta", 60, host)
    if spent_at is None:
        bad(f"[{label}] /meta was never refused in 60 requests: the spend did not happen")
        conn.close()
        return
    first = get(conn, "/session", host)
    rest = [get(conn, "/session", host) for _ in range(2)]
    conn.close()
    if expect_refusal:
        if first == 429:
            ok(f"[{label}] /meta spent at request {spent_at}; the very next /session was refused — the zones are shared")
        else:
            bad(f"[{label}] /meta spent at request {spent_at} but /session answered {first} ({rest}): the control cannot see the defect")
    elif first != 429:
        ok(f"[{label}] /meta spent at request {spent_at}; /session answered {first} right after ({rest})")
    else:
        bad(f"[{label}] /meta spent at request {spent_at} and /session was refused: one endpoint's budget spent another's")


def phase_header_posture(port, label, host, expect_sts):
    """The `Strict-Transport-Security` field the front adds: none.

    The demo's HSTS is the edge's. Cloudflare emits one field for every response
    on the zone, the ones the origin never writes included, and a policy is a
    property of the host rather than of the response that carried it -- so a
    second field here would buy the deployment nothing and would make which of
    the two a browser honours depend on their order, since a user agent
    processes only the first (`RFC 6797` §8.1). `README.md` owns why the edge
    owns it and what the zone's own field currently says.

    The claim is a negative, so it carries the check that it is reading the
    thing it claims to: a response with no header block at all would satisfy it
    for the wrong reason, and every path asked for has to answer with a media
    type before its silence about HSTS means anything.
    """
    asked = (("/", "page"), ("/terms", "the front's own file"), ("/meta", "the server's JSON"))
    silent = []
    sent = []
    for path, what in asked:
        conn = source_conn(port, "127.0.0.6")
        try:
            status, headers = get_response(conn, path, host)
        finally:
            conn.close()
        if not headers.get("content-type"):
            bad(f"[{label}] {path} ({what}) answered with no media type: this claim is not reading headers")
        if "strict-transport-security" in headers:
            sent.append(f"{path} -> {headers['strict-transport-security']!r}")
        else:
            silent.append(f"{path} -> {status} ({what})")
    if expect_sts:
        if sent:
            ok(f"[{label}] the copy that adds an HSTS field was seen carrying one: {'; '.join(sent)}")
        else:
            bad(f"[{label}] the control adds an HSTS field and this claim did not see it: it cannot see the defect it exists for")
    elif not sent:
        ok(f"[{label}] the front added no Strict-Transport-Security: {'; '.join(silent)}")
    else:
        bad(
            f"[{label}] the front added Strict-Transport-Security: {'; '.join(sent)} — the edge already "
            f"emits one for the zone and RFC 6797 §8.1 makes a browser honour only the first"
        )


def phase_page_independence(port, host):
    conn = source_conn(port, "127.0.0.5")
    for _ in range(15):
        get(conn, "/", host)
    first = get(conn, "/session", host)
    conn.close()
    if first != 429:
        ok(f"15 page reads from one source left /session answerable ({first})")
    else:
        bad("15 page reads from one source refused the /session that followed")


# ----------------------------------------------------------------- the runs

def stop_front(ngx, work):
    subprocess.run([ngx, "-c", f"{work}/nginx.conf", "-p", f"{work}/prefix", "-e", f"{work}/error.log", "-s", "stop"],
                   check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_ready(port, host, deadline=15.0):
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        try:
            conn = source_conn(port, "127.0.0.9", timeout=2)
            answer = get(conn, "/terms", host)
            conn.close()
            if answer == 200:
                return True
        except OSError:
            pass
        time.sleep(0.2)
    return False


def port_free(port):
    with socket.socket() as probe:
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def run(ngx, work, stub_port, label, host, default_text=None, expect_refusal=False, expect_sts=False,
        expect_wrong_host_served=False):
    """Start the front on a port of its own and make the claims against it.

    A run whose nginx failed to bind (a leftover from a killed run holding the
    port) must not go on to measure the leftover: the pid file nginx itself
    wrote has to be there, and the front has to answer, before any claim is
    made. A busy port is not an assertion failure, only an exhausted range is.
    """
    default = None
    started = False
    for port in range(18440, 18500):
        if not port_free(port):
            continue
        default = harness_configs(work, port, stub_port, default_text)
        subprocess.run([ngx, "-c", f"{work}/nginx.conf", "-p", f"{work}/prefix", "-e", f"{work}/error.log"],
                       check=True)
        started = os.path.exists(f"{work}/nginx.pid") and wait_ready(port, host)
        if started:
            break
        stop_front(ngx, work)
    if not started:
        bad(f"[{label}] the harness could not start a front of its own on any port in 18440-18499")
        return default
    try:
        ok(f"[{label}] the front is listening on 127.0.0.1:{port}")
        phase_host_routing(port, label, host, expect_wrong_host_served)
        phase_own_limits(port, host)
        phase_independence(port, label, host, expect_refusal=expect_refusal)
        phase_page_independence(port, host)
        phase_header_posture(port, label, host, expect_sts=expect_sts)
    finally:
        stop_front(ngx, work)
    return default


def main():
    ngx = nginx_binary()
    root = work_root()
    stub, stub_port = start_stub()

    with open(DEFAULT_CONF, encoding="utf-8") as handle:
        shipped = handle.read()
    host = server_name(shipped)

    say("the shipped configuration")
    run(ngx, os.path.join(root, "front"), stub_port, "the shipped configuration", host)

    # The control: the same configuration with the host guard removed. That is
    # the front that answers any name that resolves to it — the landing page's
    # name included — and the host-routing claim above has to fail against it or
    # it is not testing anything. The real file is not touched; the copy is a
    # string.
    say("the control: no guard on the Host the front answers")
    guard = "    if ($host != $server_name) {\n        return 444;\n    }\n"
    if guard not in shipped:
        bad("the host guard is not in the server block; the control has nothing to mutate")
    else:
        run(ngx, os.path.join(root, "host-control"), stub_port, "the host control", host,
            default_text=shipped.replace(guard, ""), expect_wrong_host_served=True)

    # The control: the same configuration with `/meta` pointed at `/session`'s
    # request zone. That is the defect the split exists to prevent, and the
    # independence claim above has to fail against it or the claim is not
    # testing anything. The real file is not touched; the copy is a string.
    say("the control: /meta naming the session request zone")
    needle = "limit_req  zone=metaread burst=10 nodelay;"
    if needle not in shipped:
        bad(f"'{needle}' is not in the /meta location; the control has nothing to mutate")
    else:
        control = shipped.replace(needle, "limit_req  zone=handshake burst=10 nodelay;")
        run(ngx, os.path.join(root, "control"), stub_port, "the control", host,
            default_text=control, expect_refusal=True)

    # The second control: the shipped configuration with an HSTS field added to
    # the server block. That is what a reader who took HSTS for the front's to
    # set would write, and the posture claim above has to fail against it or it
    # is not testing anything.
    say("the control: the front adding a Strict-Transport-Security field")
    marker = "    server_tokens off;"
    if marker not in shipped:
        bad(f"'{marker}' is not in the server block; the control has nothing to mutate")
    else:
        sts_control = shipped.replace(
            marker,
            marker + '\n    add_header Strict-Transport-Security "max-age=0; includeSubDomains; preload" always;',
        )
        run(ngx, os.path.join(root, "sts-control"), stub_port, "the STS control", host,
            default_text=sts_control, expect_sts=True)
    stub.shutdown()

    say("result")
    if failures:
        print(f"  {len(failures)} assertion(s) failed")
        return 1
    print("  the front answered only its own name, refused on its own account, one "
          "endpoint's traffic spent no other's, and it added no HSTS of its own")
    return 0


if __name__ == "__main__":
    sys.exit(main())
