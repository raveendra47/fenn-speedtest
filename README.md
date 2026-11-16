# Speedtest for Fenn

Download, upload and ping test, right from the notch. Tap the tile — or press **▶** on the 2×2 tile, or **Run test** on the page — and a real test runs, its numbers filling in live in every view as each phase finishes. Tapping the tile also opens the page, which shows the same live numbers.

- Tile sizes: 1×1 (download speed), 2×1 (download and upload), 2×2 (download, upload, ping, history and a ▶ button).
- Starts from anywhere: a tap on the tile, the ▶ on the 2×2 tile (runs without opening the page), Run test or the header ▶ on the page. While a test runs, ping, download and upload appear the moment their phases finish.
- Keeps your last 20 results and draws them as a graph.

## What you need

Nothing. The plugin is a compiled Windows program (`bin/speedtest.exe`). You do not need Python or Node.
The source code is in `src/main.rs`. To build it yourself: `cargo build --release`, then copy `target/release/speedtest.exe` to `bin/`.

## How it measures

- **Ping** — five timed empty requests; the median round trip minus the processing time Cloudflare itself reports in the `Server-Timing` header.
- **Download** — six parallel streams, starting a quarter second apart (a same-instant burst of large requests draws rate-limit 429s). Each stream warms up until its connection has ramped (two seconds at most), then counts bytes for up to eight seconds. Each stream starts with a 10 MB chunk, doubles it while requests finish in under a second (up to 100 MB) and halves it when they crawl, like the browser test does; a test moves at most 300 MB, so a fast line needs only a handful of requests and stays inside Cloudflare's per-IP byte budget.
- **Upload** — six parallel streams. The warm-up ramps the connections and sizes the request bodies (about two seconds of data each, 250 KB to 100 MB — short enough that every stream finishes a request inside the 500 MB budget); the counted phase then runs up to eight seconds. A stream's rate is the bytes of its finished requests over their duration, so a request still in flight when the clock runs out cannot skew the number; the result is the sum of the stream rates.
- If Cloudflare answers 429, every stream stops at once — hammering a limited endpoint would only make the limit worse. Whatever was already measured still counts: the run finishes with a partial result and an orange note saying why. Only a 429 before anything was measured fails the run with a message. A 429 means the per-IP byte budget is spent — running tests back to back spends it (it refills over a few minutes); wait and try again. A 403 (chunk size refused) just makes the streams ask for smaller chunks.

## What it sends, and where

- It talks only to `speed.cloudflare.com` (Cloudflare's public speed test).
- It downloads and uploads test data for about 10 seconds each way. Cloudflare sees your IP address, as with any website.
- **Data use:** a test moves at most about 300 MB down and 500 MB up, and less on slower lines (there the eight-second window binds first). At 100 Mbps a full test is roughly 250 MB. Do not run it on a metered connection.
- Nothing is sent to the plugin author.

## What it saves

`history.json` (your last 20 results) and `layout.json` (the tile size Fenn last reported) in Fenn's plugin data folder, `%APPDATA%\Fenn\plugin-data\speedtest`.

## Accuracy

The method matches the engine behind [speed.cloudflare.com](https://speed.cloudflare.com) (same endpoints; six parallel streams with adaptive chunk sizes instead of its sequential ramp). Expect numbers in the same range as that test run at the same moment; any speed test can differ from another by 10–15% depending on which server it picks and when. Ping here is an HTTP round trip with Cloudflare's own processing time subtracted, so it reads a few ms higher than an ICMP ping.

## Development

`tests/fake_fenn.py` is a fake Fenn plus a mock speed-test server. It checks the protocol (tile at all three sizes, layout persistence across restarts), a full run, a run started from the tile (its ▶ button and a tap on the tile body, with the live numbers on the tile), a 429 abort, a mid-run 429 that keeps its partial result, the adaptive chunk sizing on 403 and an unreachable server — all without touching the network:

```
cargo build --release
python3 tests/fake_fenn.py target/release/speedtest
```

Testing hooks (meant for the test suite): `SPEEDTEST_BASE_URL` redirects every request, `SPEEDTEST_SECS` and `SPEEDTEST_WARMUP_SECS` shrink the phases, `SPEEDTEST_DOWN_CAP` and `SPEEDTEST_UP_CAP` shrink the byte budgets, and `SPEEDTEST_VERBOSE=1` logs each request to stderr.

Note for development: Fenn's plugin dev mode re-reads `plugin.toml` on every file save, which resets the tile to the manifest's default size. That is Fenn's dev mode, not the plugin losing your choice; outside dev mode the size you pick sticks.

## License

MIT — see [LICENSE](LICENSE). © 2026 【Author name】
