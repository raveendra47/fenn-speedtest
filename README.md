# Speedtest for Fenn

Download, upload and ping test, right from the notch. Click the tile to open the page and press **Run test**.

- Tile sizes: 1x1 (download speed), 2x1 (download and upload), 2x2 (download, upload, ping and history).
- Keeps your last 20 results and draws them as a graph.

## What you need

Nothing. The plugin is a compiled Windows program (`bin/speedtest.exe`). You do not need Python or Node.
The source code is in `src/main.rs`. To build it yourself: `cargo build --release`, then copy `target/release/speedtest.exe` to `bin/`.

## What it sends, and where

- It talks only to `speed.cloudflare.com` (Cloudflare's public speed test).
- It downloads and uploads random test data for about 8 seconds each way. Cloudflare sees your IP address, as with any website.
- **Data use:** a test uses as much data as your speed allows. At 100 Mbps that is roughly 200 MB per test. Do not run it on a metered connection.
- Nothing is sent to the plugin author.

## What it saves

`history.json` (your last 20 results) in Fenn's plugin data folder, `%APPDATA%\Fenn\plugin-data\speedtest`.

## License

(add a license here)
