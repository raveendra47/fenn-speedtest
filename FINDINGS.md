# Findings: the Cloudflare speed endpoint, measured

Everything below was measured from this sandbox (IP 47.57.242.119, Cloudflare colo HKG)
on 2026-11-16, with curl and the plugin itself. The endpoint is undocumented; these are
observations, not promises. Scripts: `scripts/probe_cf.py`, `ab_concurrency.py`,
`origin_test.py`, `diag_limited.py`, `upload_calc.py`; raw logs in `probe-data/`
(mirrors of `scripts/*_results.txt` and `upload_ground_truth.txt`).

## 1. Request-size caps

- `GET/HEAD /__down?bytes=N` from a plain (non-browser) client:
  - 25–90 MB: **200 OK**
  - 100 MB and above (100e6, 104857600, 150 MB, 209715200, 262144000): **403**
  - So the plain-client cap sits between 90 MB and 100 MB. The plugin probes
    50 MB → 25 MB and uses the first accepted size (it no longer asks for 100 MB).
- With a browser-context header (`Origin: https://speed.cloudflare.com` or
  `Referer: https://speed.cloudflare.com/`), HEAD 250 MB returns **200** — the size
  gate is header-dependent, which matches Cloudflare's own engine requesting 2.5e8
  bytes in its final rounds. The plugin deliberately does not spoof browser headers;
  it sizes requests to what a plain client is allowed.
- `POST /__up`: no cap found — 1 MB through 150 MB all returned 200.

## 2. Rate limiting (the main blocker, now understood)

- The limit is **not** a per-minute request count and **not** a client fingerprint:
  curl and the plugin (ureq) fail identically.
- It behaves like a **rolling byte budget per IP on `__down` body bytes**, roughly
  200–400 MB, refilling over minutes. Observations:
  - Six simultaneous 75 MB GETs: 4 of 6 got 200, 2 got 429 (450 MB burst → partial refusal).
  - Six simultaneous 25 MB GETs: 4 × 200, 2 × 429 (the budget was partly spent minutes earlier).
  - Seconds later, single 25 MB GETs went through again; after ~10 minutes, single
    75 MB GETs went through again.
- While "limited", small requests still pass: `GET bytes=0` → 200, HEAD → 200,
  `POST /__up` (25 MB) → 200. The limit hits large downloads selectively.
- A 429 can also hit `POST /__up` right after a download phase (observed:
  `retry-after: 48` seconds after a capped 40 MB download on an already-drained
  shared IP). On a residential line the plugin's own footprint (≤ 300 MB down +
  ≤ 500 MB up, staggered, ~10–20 requests) stays inside the budget; on a shared
  datacenter IP other tenants can drain it at any moment.
- The `Retry-After` header on these 429s is overstated (~1 hour); actual recovery is
  minutes. The plugin now quotes "a few minutes" unless Retry-After says something short
  (≤ 120 s), in which case it quotes that.

## 3. What the plugin changed because of this

- Download per-test byte budget: **300 MB** (was unbounded). At 300 Mbps+ the test
  now moves ~300 MB instead of 8 s × 6 × line rate.
- Streams start **staggered 250 ms apart**; six same-instant large GETs draw 429s.
- Chunks: probe 50 MB → 25 MB (first accepted wins; cached per process).
- A 429 aborts the test cleanly (by decision); the request-count fix alone cannot
  make 429s impossible when the budget is already spent, but the budget + stagger +
  big chunks make the plugin's own footprint per test ~4–12 requests instead of dozens.
- Upload aggregation bug fixed: the old code divided total bytes by total duration
  across six parallel streams — i.e. the per-stream rate, ~1/6 of the true number.
  The mock test missed it because loopback had no ground truth. Now: sum of per-stream
  rates. Loopback mock: down 51 Gbps, up 24.5 Gbps (previously "up" read 2.4 Gbps).
- Upload body sizing: two seconds of data per stream (was four). With 4 s bodies a
  1 Gbps line hits the 500 MB up budget mid-round and loses the streams that never
  finish a request; 2 s bodies fit every stream inside the budget (live run: five
  86 MB requests completed, 491.5/500 MB used, sixth stream sat out cleanly).
- Warm-up no longer burns the budget on fast lines (it ends after 2 finished requests
  and 8 MB per stream, or the clock), and ping subtracts Cloudflare's own
  `Server-Timing` processing time from the round trip.

## 4. Open questions, answered as far as this sandbox can

1. **Largest request size?** Plain client: 90–100 MB boundary (403 at 100 MB).
   With browser headers there is no sub-250 MB cap, but spoofing them is not the
   plugin's business. The plugin uses ≤ 50 MB chunks.
2. **Requests per minute to 429?** Not the mechanism — it is a byte budget
   (~200–400 MB rolling, refills in minutes), so "a test every few minutes" is safe
   by design now: one test ≤ 300 MB down + ≤ 500 MB up, staggered, few requests.
3. **Tile size reset?** Fenn's plugin dev mode re-reads `plugin.toml` on every file
   save, which includes `size` — so saving any file resets the tile to the manifest's
   default (confirmed in fenn.click/docs "Develop and debug"). Outside dev mode the
   size sticks; the plugin additionally persists the last reported layout in
   `layout.json` and draws it immediately after a restart, so even a dev-mode reload
   shows the right tile until Fenn re-sends layout.
4. **Accuracy vs speedtest.net?** Not measurable from this sandbox (speedtest.net
   picks different servers). But the plugin's upload number now has a direct check:
   on the same IP, minutes apart, the plugin measured 1044 Mbps against 1113 Mbps
   from six parallel curl POSTs — within 6%. The method matches the official
   Cloudflare engine's approach (parallel streams, warm-up excluded). Verify once
   against speedtest.net on the target machine with the README's Accuracy checklist.
5. **Fallback server?** Out of scope by decision ("not now — revisit"). If Cloudflare
   stays unreliable for some users, options are a `__down`-compatible alternative or
   LibreSpeed-style backends; the README says the endpoint is undocumented and may change.
6. **License and author?** MIT (chosen), author placeholder `【Author name】` in
   LICENSE and README — the one thing to fill in before publishing.

## 5. Marketplace review readiness

Fenn's publish rules (fenn.click/docs/publish): plugin.toml at repo root ✓, README
stating what it sends and where ✓ (talks only to speed.cloudflare.com, data-use note),
license ✓ (MIT), data in FENN_DATA_DIR ✓, run command ✓. Fenn installs the exact
commit the listing was made from, so `bin/speedtest.exe` must be committed to the repo
before listing — a GitHub Actions workflow builds it on every push (artifact
`speedtest-windows`); the .gitignore keeps bin/ tracked. Source is in the repo, which
is what reviewers inspect.

## 6. Numbers at a glance (sandbox line, 2026-11-16)

| measurement | value |
|---|---|
| single GET 25 MB | 667 Mbps (probe round), 588–667 Mbps range |
| single GET 90 MB | 588 Mbps |
| plugin download phase (uncapped attempt) | 1923 Mbps on 164 MB before the shared-IP budget stopped 2 of 6 streams |
| plugin download phase (capped at 40 MB) | 563 Mbps, completed at the cap exactly as designed |
| upload POST 25 MB (single) | ~683 Mbps (probe round) |
| upload, 6 parallel 25 MB POSTs (curl ground truth) | 1113 Mbps aggregate; 166–213 Mbps per stream |
| plugin upload phase (capped at 500 MB) | 1044 Mbps — within 6% of the curl ground truth |
| plugin full live run (diagnostic knobs: 25 MB down cap) | down 1025 / up 1044 / ping 10.1 ms, history saved, 6 s wall |
| ping (HTTP RTT minus Server-Timing, HKG colo) | ~10 ms net (~40–60 ms raw round trip) |
