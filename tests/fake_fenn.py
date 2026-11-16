#!/usr/bin/env python3
"""Fake Fenn: tests the speedtest plugin without Fenn.

Covers the protocol (tile at all three sizes, layout persistence across a
restart), a full run against a local mock server, a 429 abort, the size-probe
fallback on 403, and an unreachable server. No traffic leaves the loopback
interface -- the mock server stands in for speed.cloudflare.com.

Usage:
    python3 tests/fake_fenn.py [path-to-binary]      # offline test suite
    python3 tests/fake_fenn.py [binary] --live       # one real Cloudflare run
"""

import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BIN = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else "target/release/speedtest"
LIVE = "--live" in sys.argv

FAILURES = []


def check(cond, what):
    tag = "ok" if cond else "FAIL"
    print(f"  [{tag}] {what}")
    if not cond:
        FAILURES.append(what)


class Plugin:
    """One plugin process plus a reader thread that validates every line."""

    def __init__(self, data_dir, env=None):
        self.proc = subprocess.Popen(
            [BIN],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env={**os.environ, "FENN_DATA_DIR": data_dir, **(env or {})},
        )
        self.q = queue.Queue()
        self.bad_lines = []
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        for line in self.proc.stdout:
            try:
                self.q.put(json.loads(line))
            except json.JSONDecodeError:
                self.bad_lines.append(line.rstrip())
        self.q.put(None)

    def send(self, obj):
        self.proc.stdin.write(json.dumps(obj) + "\n")
        self.proc.stdin.flush()

    def next_msg(self, timeout=60):
        m = self.q.get(timeout=timeout)
        if m is None:
            raise EOFError("plugin exited unexpectedly")
        return m

    def wait_for(self, pred, what, timeout=90):
        """Consume messages until pred(msg); returns the matching message."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            m = self.next_msg(max(1, deadline - time.monotonic()))
            if pred(m):
                return m
        raise TimeoutError(f"timed out waiting for {what}")

    def shutdown(self):
        self.send({"type": "shutdown"})
        try:
            return self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            return None

    def stderr(self):
        try:
            return self.proc.communicate(timeout=5)[1]
        except subprocess.TimeoutExpired:
            return ""


def hello(p, data_dir):
    p.send({
        "type": "hello", "protocol": 1, "fenn_version": "0.5.0",
        "plugin_id": "speedtest", "data_dir": data_dir,
        "theme": {"dark": True, "accent": "#7C5CFF"},
    })


def layout(p, size, w, h):
    p.send({"type": "layout", "target": "tile", "size": size, "width": w, "height": h})


def tile_kind(view):
    """small / wide / large, told apart by the shape of the view tree."""
    if view.get("type") == "row":
        return "wide"
    kids = view.get("children", [])
    if any(c.get("type") == "text" and c.get("size") == "title" for c in kids):
        return "small"
    return "large"


# ---------------------------------------------------------------- mock server

class MockState:
    cap = None          # 403 for __down bytes above this
    limit_after = None  # 429 after this many __down requests (None = never)
    hits = 0
    log = []


class Mock(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _reply(self, code, body=b"", head=False, headers=()):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head and body:
            self.wfile.write(body)

    def do_HEAD(self):
        self._serve(head=True)

    def do_GET(self):
        self._serve(head=False)

    def _serve(self, head):
        n = int(self.path.split("bytes=")[1]) if "bytes=" in self.path else 0
        MockState.hits += 1
        MockState.log.append(("HEAD" if head else "GET", n))
        if MockState.limit_after is not None and MockState.hits > MockState.limit_after:
            self._reply(429, b"x" if not head else b"", head,
                        [("retry-after", "7")])
            return
        if MockState.cap is not None and n > MockState.cap:
            self._reply(403, b"x", head)
            return
        hdrs = [("Content-Type", "application/octet-stream"),
                ("Server-Timing", "cfSpeedEdge;dur=20, cfSpeedWorker;dur=5")]
        if head:
            self._reply(200, b"", head, hdrs + [("Content-Length", str(n))])
            return
        self.send_response(200)
        for k, v in hdrs:
            self.send_header(k, v)
        self.send_header("Content-Length", str(n))
        self.end_headers()
        chunk = b"\0" * 65536
        sent = 0
        while sent < n:
            piece = chunk if n - sent >= 65536 else chunk[: n - sent]
            self.wfile.write(piece)
            sent += len(piece)

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        while n > 0:
            n -= len(self.rfile.read(min(65536, n)))
        MockState.log.append(("POST", "up"))
        self._reply(200)

    def log_message(self, *a):
        pass


def mock_server():
    class QuietHTTPServer(ThreadingHTTPServer):
        # The plugin drops connections mid-body when the clock runs out;
        # that is expected, not an error worth a traceback.
        def handle_error(self, request, client_address):
            pass

    srv = QuietHTTPServer(("127.0.0.1", 0), Mock)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def reset_mock(cap=None, limit_after=None):
    MockState.cap = cap
    MockState.limit_after = limit_after
    MockState.hits = 0
    MockState.log = []


# ------------------------------------------------------------------- the tests

def t_protocol_and_layout(tmp):
    print("protocol and tile sizes")
    p = Plugin(tmp)
    hello(p, tmp)
    tile = p.wait_for(lambda m: m.get("type") == "tile", "first tile")
    check(tile_kind(tile["view"]) == "wide", "hello draws the wide tile by default")
    p.wait_for(lambda m: m.get("type") == "page", "page on hello")

    layout(p, "small", 44, 44)
    tile = p.wait_for(lambda m: m.get("type") == "tile" and tile_kind(m["view"]) == "small",
                      "small tile")
    check(tile_kind(tile["view"]) == "small", "1x1 layout draws the small tile")

    layout(p, "wide", 104, 36)
    p.wait_for(lambda m: m.get("type") == "tile" and tile_kind(m["view"]) == "wide", "wide tile")
    check(True, "2x1 layout draws the wide tile")

    layout(p, "large", 104, 104)
    tile = p.wait_for(lambda m: m.get("type") == "tile" and tile_kind(m["view"]) == "large",
                      "large tile")
    kids = tile["view"].get("children", [])
    check(kids and kids[0].get("text") == "Speedtest", "2x2 tile has its title")

    # an unknown message type must be ignored, not fatal (protocol v1 rule)
    p.send({"type": "something-new", "stuff": 1})
    layout(p, "small", 44, 44)
    p.wait_for(lambda m: m.get("type") == "tile" and tile_kind(m["view"]) == "small",
               "small tile again")
    check(True, "unknown message types are ignored")

    check(p.shutdown() == 0, "shutdown exits cleanly")
    check(not p.bad_lines, "every stdout line was JSON")
    check(json.load(open(os.path.join(tmp, "layout.json")))["size"] == "small",
          "tile size persisted to layout.json")

    print("layout survives a restart")
    p = Plugin(tmp)
    hello(p, tmp)
    tile = p.wait_for(lambda m: m.get("type") == "tile", "tile after restart")
    check(tile_kind(tile["view"]) == "small",
          "first draw after restart uses the persisted size, before any layout message")
    check(p.shutdown() == 0, "shutdown exits cleanly (2)")


def t_success(tmp):
    print("full run against the mock server")
    reset_mock()
    srv = mock_server()
    p = Plugin(tmp, env={"SPEEDTEST_BASE_URL": f"http://127.0.0.1:{srv.server_address[1]}",
                         "SPEEDTEST_SECS": "0.6", "SPEEDTEST_WARMUP_SECS": "0.3"})
    hello(p, tmp)
    p.wait_for(lambda m: m.get("type") == "page", "page")

    p.send({"type": "event", "id": "run", "kind": "click"})
    badges = set()

    # wait for the run to finish: the page button goes back to "Run test"
    # after numbers (not dashes) are on the cards
    deadline = time.monotonic() + 60
    finished = False
    saw_numbers = False
    while time.monotonic() < deadline and not finished:
        m = p.next_msg(5)
        if m.get("type") == "badge" and isinstance(m.get("badge"), dict):
            badges.add(m["badge"].get("label"))
        if m.get("type") == "page":
            kids = m["view"].get("children", [])
            grid = next((c for c in kids if c.get("type") == "grid"), None)
            if grid:
                texts = [t.get("text") for g in grid.get("children", [])
                         for t in g.get("children", []) if t.get("type") == "text"]
                if any(t and t.replace(".", "").replace("-", "").isdigit() for t in texts):
                    saw_numbers = True
            btn = next((c for c in kids if c.get("id") == "run"), None)
            if btn and btn.get("label") == "Run test" and saw_numbers:
                finished = True
    check(finished, "run finishes and the page shows numbers")
    check(badges == {"P", "D", "U"}, f"ring badges during phases (got {badges})")

    hist = json.load(open(os.path.join(tmp, "history.json")))
    check(len(hist) == 1 and len(hist[0]) == 3, "history.json has one run")
    d, u, pg = hist[0]
    check(d > 0 and u > 0 and pg >= 0, f"plausible numbers: down={d:.0f} up={u:.0f} ping={pg:.1f}")
    check(MockState.log[0][0] in ("HEAD", "GET"), "mock saw requests")

    p.send({"type": "event", "id": "run", "kind": "click"})  # a second run must also work
    time.sleep(1.5)
    check(p.proc.poll() is None, "plugin still alive after a second run started")
    check(p.shutdown() == 0, "shutdown exits cleanly")
    check(not p.bad_lines, "every stdout line was JSON")
    srv.shutdown()


def t_rate_limited(tmp):
    print("429 aborts cleanly")
    reset_mock(limit_after=0)  # every request 429s, retry-after: 7
    srv = mock_server()
    p = Plugin(tmp, env={"SPEEDTEST_BASE_URL": f"http://127.0.0.1:{srv.server_address[1]}",
                         "SPEEDTEST_SECS": "0.6", "SPEEDTEST_WARMUP_SECS": "0.3"})
    hello(p, tmp)
    p.wait_for(lambda m: m.get("type") == "page", "page")
    p.send({"type": "event", "id": "run", "kind": "click"})

    err = None
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        m = p.next_msg(5)
        if m.get("type") == "page":
            texts = [c.get("text", "") for c in m["view"].get("children", [])
                     if c.get("type") == "text"]
            err = next((t for t in texts if "Cloudflare" in t), err)
            btn = next((c for c in m["view"].get("children", []) if c.get("id") == "run"), None)
            if err and btn and btn.get("label") == "Run test":
                break
    check(err is not None and "limiting" in err and "7 seconds" in err,
          f"friendly 429 message with the wait time (got: {err!r})")
    check(p.proc.poll() is None, "plugin still alive after the abort")
    check(not os.path.exists(os.path.join(tmp, "history.json")), "no history written for a failed run")
    check(p.shutdown() == 0, "shutdown exits cleanly")
    srv.shutdown()


def t_size_fallback(tmp):
    print("403 makes the probe fall back to a smaller chunk")
    reset_mock(cap=40_000_000)  # refuses the 50 MB rung
    srv = mock_server()
    p = Plugin(tmp, env={"SPEEDTEST_BASE_URL": f"http://127.0.0.1:{srv.server_address[1]}",
                         "SPEEDTEST_SECS": "0.6", "SPEEDTEST_WARMUP_SECS": "0.3"})
    hello(p, tmp)
    p.wait_for(lambda m: m.get("type") == "page", "page")
    p.send({"type": "event", "id": "run", "kind": "click"})

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        m = p.next_msg(5)
        if m.get("type") == "active" and m.get("active") is False:
            break
    heads = [n for meth, n in MockState.log if meth == "HEAD"]
    gets = [n for meth, n in MockState.log if meth == "GET"]
    check(50_000_000 in heads, "probe tried the 50 MB rung")
    check(25_000_000 in heads, "probe fell back to the 25 MB rung")
    check(gets and max(gets) == 25_000_000, "download ran with 25 MB chunks")
    check(os.path.exists(os.path.join(tmp, "history.json")), "the run still succeeded")
    check(p.shutdown() == 0, "shutdown exits cleanly")
    srv.shutdown()


def t_byte_cap(tmp):
    print("byte budgets end a phase early without breaking the result")
    reset_mock()
    srv = mock_server()
    p = Plugin(tmp, env={"SPEEDTEST_BASE_URL": f"http://127.0.0.1:{srv.server_address[1]}",
                         "SPEEDTEST_SECS": "2", "SPEEDTEST_WARMUP_SECS": "1",
                         "SPEEDTEST_DOWN_CAP": "100000000", "SPEEDTEST_UP_CAP": "400000000"})
    hello(p, tmp)
    p.wait_for(lambda m: m.get("type") == "page", "page")
    p.send({"type": "event", "id": "run", "kind": "click"})
    ok = False
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        m = p.next_msg(5)
        if m.get("type") == "active" and m.get("active") is False:
            ok = os.path.exists(os.path.join(tmp, "history.json"))
            break
    check(ok, "run finishes when the byte budgets bind")
    hist = json.load(open(os.path.join(tmp, "history.json")))
    d, u, pg = hist[0]
    check(d > 0 and u > 0, f"numbers still plausible: down={d:.0f} up={u:.0f}")
    check(p.shutdown() == 0, "shutdown exits cleanly")
    srv.shutdown()


def t_unreachable(tmp):
    print("unreachable server")
    p = Plugin(tmp, env={"SPEEDTEST_BASE_URL": "http://127.0.0.1:9",
                         "SPEEDTEST_SECS": "0.6", "SPEEDTEST_WARMUP_SECS": "0.3"})
    hello(p, tmp)
    p.wait_for(lambda m: m.get("type") == "page", "page")
    p.send({"type": "event", "id": "run", "kind": "click"})
    err = None
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        m = p.next_msg(5)
        if m.get("type") == "page":
            texts = [c.get("text", "") for c in m["view"].get("children", [])
                     if c.get("type") == "text"]
            err = next((t for t in texts if t and t not in
                        ("–", "Download history", "Run test", "Testing…")
                        and "Mbps" not in t and "ms" not in t), err)
            btn = next((c for c in m["view"].get("children", []) if c.get("id") == "run"), None)
            if err and btn and btn.get("label") == "Run test":
                break
    check(err is not None, f"a network error becomes a message, not a crash (got: {err!r})")
    check(p.shutdown() == 0, "shutdown exits cleanly")


def live():
    """One real test against Cloudflare, with a per-request log on stderr."""
    tmp = tempfile.mkdtemp(prefix="fenn-live-")
    p = Plugin(tmp, env={"SPEEDTEST_VERBOSE": "1"})
    hello(p, tmp)
    p.wait_for(lambda m: m.get("type") == "page", "page")
    print("running one full test against speed.cloudflare.com ...")
    t0 = time.monotonic()
    p.send({"type": "event", "id": "run", "kind": "click"})
    hist = None
    while time.monotonic() - t0 < 120:
        try:
            m = p.next_msg(15)
        except queue.Empty:
            continue  # phases run silent for 10+ seconds
        except EOFError:
            break
        if m.get("type") == "page":
            btn = next((c for c in m["view"].get("children", []) if c.get("id") == "run"), None)
            if btn and btn.get("label") == "Run test" and os.path.exists(
                    os.path.join(tmp, "history.json")):
                hist = json.load(open(os.path.join(tmp, "history.json")))
                break
    p.send({"type": "shutdown"})
    p.proc.wait(timeout=5)
    err = p.proc.stderr.read()
    counts = {}
    for line in err.splitlines():
        if line.startswith("probe HEAD"):
            counts["probe HEAD"] = counts.get("probe HEAD", 0) + 1
        elif line.startswith("up "):
            counts["upload POST"] = counts.get("upload POST", 0) + 1
    downloads = sum(1 for line in err.splitlines() if line.startswith("download:"))
    print(f"\nlive result: {hist[-1] if hist else 'FAILED'}")
    print(f"wall time: {time.monotonic() - t0:.0f}s")
    print(f"requests (from stderr): {counts}, download phases: {downloads}")
    print("stderr tail:")
    for line in err.splitlines()[-12:]:
        print("   ", line)
    shutil.rmtree(tmp, ignore_errors=True)


def main():
    if LIVE:
        live()
        return
    tests = [
        t_protocol_and_layout,
        t_success,
        t_rate_limited,
        t_size_fallback,
        t_byte_cap,
        t_unreachable,
    ]
    for t in tests:
        tmp = tempfile.mkdtemp(prefix="fenn-test-")
        try:
            t(tmp)
        except Exception as e:  # noqa: BLE001 - report and keep going
            check(False, f"{t.__name__} raised {type(e).__name__}: {e}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S):")
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("all tests passed")


if __name__ == "__main__":
    main()
