// Speedtest for Fenn: download, upload and ping against speed.cloudflare.com,
// drawn on a tile and a page. A tap on the tile, the ▶ on the 2x2 tile or
// Run test on the page starts a real test; the numbers fill in live in every
// view as each phase finishes. See README.md for what it sends and saves.
//
// Measurement design:
// - ping: one warm-up request, then five timed empty requests; the median
//   round trip minus the processing time Cloudflare reports (Server-Timing).
// - download: six parallel streams, started a quarter second apart (a burst
//   of same-instant large GETs draws 429s). Each stream warms up until its
//   connection has ramped (at most two seconds), then counts bytes for up to
//   eight seconds. Each stream starts with a 10 MB chunk, doubles it while
//   requests finish in under a second (up to 100 MB) and halves it when they
//   crawl; a test moves at most 300 MB so fast lines stay inside Cloudflare's
//   per-IP byte budget.
// - upload: six parallel streams, two seconds of warm-up (which also sizes
//   the request bodies to about four seconds of data each), then eight
//   seconds counted, at most 500 MB. A stream's rate is the bytes of its
//   finished requests over their total duration, so a request still in
//   flight when the clock runs out cannot skew the number; the test's rate
//   is the sum of the stream rates.
// - a 429 stops every stream at once (hammering a limited endpoint would only
//   make the limit worse); what was already measured still counts, shown with
//   a warning rather than failing the run. A 429 before anything was measured
//   fails the run. A 403 (chunk size refused) makes the streams ask for
//   smaller chunks and carry on.

use serde_json::{json, Value};
use std::io::{BufRead, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs, thread};

const STREAMS: usize = 6;
const KEEP: usize = 20;
const PING_COUNT: usize = 5;
/// Download chunk sizes adapt per stream, like the browser test's: start at
/// 10 MB, double while a request finishes in under a second, halve when one
/// takes seconds. Asking for the maximum size right away draws rate-limit 429s.
const DOWN_START: u64 = 10_000_000;
const DOWN_MAX: u64 = 100_000_000;
const DOWN_MIN: u64 = 5_000_000;
/// Per-test budgets. Cloudflare meters __down body bytes per IP (a burst of
/// 450 MB had requests refused while ~300 MB went through), so a test moves
/// at most DOWN_CAP down. __up was never seen limited but is bounded too.
const DOWN_CAP: u64 = 300_000_000;
const UP_CAP: u64 = 500_000_000;
/// A stream's warm-up ends after this many bytes even if the clock allows
/// more, so a very fast line cannot spend the whole budget while warming up.
const DOWN_WARM_BYTES: u64 = 12_000_000;
/// First requests start staggered: six large GETs in the same instant look
/// like a download farm to the rate limiter.
const STAGGER_MS: u64 = 250;
/// __up takes bodies far larger than we need (150 MB verified); 100 MB is
/// about two seconds per stream on a 1.2 Gbps line.
const UP_MAX: u64 = 100_000_000;
const UP_MIN: u64 = 250_000;
/// Sized so every stream finishes at least one request inside the up budget:
/// with 2 s bodies a 6-stream test uses ~6 x 2 s x rate <= 500 MB, while 4 s
/// bodies can hit the cap mid-round and lose the streams that never finish.
const UP_TARGET_SECS: f64 = 2.0;
/// A stream's warm-up is done once it has sent this much and finished two
/// requests: the connection has ramped and the samples give its rate.
const UP_WARM_MIN_BYTES: u64 = 8_000_000;

// Testing hooks, see README: point at a local mock server, shrink the phases.
fn env_f64(name: &str, default: f64) -> f64 {
    env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn warmup_secs() -> f64 {
    env_f64("SPEEDTEST_WARMUP_SECS", 2.0)
}

fn measure_secs() -> f64 {
    env_f64("SPEEDTEST_SECS", 8.0)
}

fn down_cap() -> u64 {
    env_f64("SPEEDTEST_DOWN_CAP", DOWN_CAP as f64) as u64
}

fn up_cap() -> u64 {
    env_f64("SPEEDTEST_UP_CAP", UP_CAP as f64) as u64
}

fn down_url() -> String {
    format!("{}/__down?bytes=", base_url())
}

fn up_url() -> String {
    format!("{}/__up", base_url())
}

fn base_url() -> String {
    env::var("SPEEDTEST_BASE_URL").unwrap_or_else(|_| "https://speed.cloudflare.com".to_string())
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn format_wait(secs: u64) -> String {
    let mins = secs / 60;
    let secs = secs % 60;
    if mins > 0 {
        format!("about {}m {}s", mins, secs)
    } else {
        format!("about {}s", secs)
    }
}

fn rate_limit_wait(e: &ureq::Error) -> Option<u64> {
    match e {
        ureq::Error::Status(429, r) => Some(
            r.header("retry-after")
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(|s| s.min(120))
                .unwrap_or(180),
        ),
        _ => None,
    }
}

fn cooldown_message(until: i64) -> String {
    let remaining = (until.saturating_sub(now_unix()) as u64).max(1);
    format!(
        "Cloudflare is limiting requests. Try again in {}.",
        format_wait(remaining)
    )
}

fn note(msg: String) {
    static ON: OnceLock<bool> = OnceLock::new();
    if *ON.get_or_init(|| env::var("SPEEDTEST_VERBOSE").is_ok()) {
        eprintln!("{msg}");
    }
}

#[derive(Clone)]
struct Run {
    down: f64,
    up: f64,
    ping: f64,
    /// Set when Cloudflare cut a phase short with a 429: the partial result
    /// is kept and this is shown next to it. Not stored in history.json.
    warn: Option<String>,
    /// Unix timestamp after which the user may try again.
    cooldown_until: Option<i64>,
}

enum Msg {
    Fenn(Value),
    /// Phase label, ring progress, and the results finished so far (ping,
    /// download), so every view can fill its numbers in live.
    Phase(&'static str, f64, Option<f64>, Option<f64>),
    Done(Result<Run, String>),
}

#[derive(Clone, Copy, PartialEq)]
enum TileSize {
    Small,
    Wide,
    Large,
}

impl TileSize {
    fn name(self) -> &'static str {
        match self {
            TileSize::Small => "small",
            TileSize::Wide => "wide",
            TileSize::Large => "large",
        }
    }

    /// Prefer the size name Fenn reports; fall back to point sizes, which
    /// also covers a layout message from an older Fenn without "size".
    fn from_layout(size: Option<&str>, w: f64, h: f64) -> TileSize {
        match size {
            Some("small") => TileSize::Small,
            Some("large") => TileSize::Large,
            Some("wide") => TileSize::Wide,
            _ if w < 60.0 => TileSize::Small,
            _ if h > 60.0 => TileSize::Large,
            _ => TileSize::Wide,
        }
    }
}

struct State {
    hist: Vec<Run>,
    phase: Option<&'static str>,
    /// Results of the running test as they land, for the live views.
    live_ping: Option<f64>,
    live_down: Option<f64>,
    err: Option<String>,
    /// A phase was cut short by a 429; its partial result was kept.
    warn: Option<String>,
    /// Cloudflare cooldown; block any new test until this time.
    cooldown_until: Option<i64>,
    tile_size: TileSize,
    tile_w: f64,
    tile_h: f64,
    dir: PathBuf,
}

fn send(v: Value) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{v}");
    let _ = out.flush();
}

// ---------- test ----------

fn agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(10))
        .max_idle_connections(STREAMS as usize)
        .max_idle_connections_per_host(STREAMS as usize)
        .build()
}

fn is_limited(e: &ureq::Error) -> bool {
    matches!(e, ureq::Error::Status(429, _))
}

fn is_size_refused(e: &ureq::Error) -> bool {
    matches!(e, ureq::Error::Status(403, _))
}

fn http_err(e: ureq::Error) -> String {
    match e {
        ureq::Error::Status(429, r) => {
            let wait = r
                .header("retry-after")
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(|s| s.min(120))
                .unwrap_or(180);
            format!(
                "Cloudflare is limiting requests. Try again in {}.",
                format_wait(wait)
            )
        }
        ureq::Error::Status(403, _) => {
            "Cloudflare refused the request size (403). The endpoint may have changed.".to_string()
        }
        other => other.to_string(),
    }
}

// "cfSpeedEdge;dur=43, cfSpeedWorker;dur=38" -> 81.0 ms of Cloudflare's time.
fn server_time(header: Option<&str>) -> f64 {
    let Some(h) = header else { return 0.0 };
    h.split(',')
        .filter_map(|p| p.split_once("dur="))
        .filter_map(|(_, d)| d.trim_matches('"').parse::<f64>().ok())
        .sum()
}

fn ping(ag: &ureq::Agent) -> Result<f64, String> {
    let url = format!("{}0", down_url());
    let round = |warm: bool| -> Result<f64, String> {
        let mut sink = Vec::new();
        let t = Instant::now();
        let r = ag.get(&url).call().map_err(http_err)?;
        let st = server_time(r.header("Server-Timing"));
        r.into_reader().read_to_end(&mut sink).map_err(|e| e.to_string())?;
        note(format!("ping round: {:.1} ms", t.elapsed().as_secs_f64() * 1000.0));
        if warm {
            return Ok(0.0); // opens the connection; not counted
        }
        Ok((t.elapsed().as_secs_f64() * 1000.0 - st).max(0.1))
    };
    round(true)?;
    let mut xs = Vec::with_capacity(PING_COUNT);
    for _ in 0..PING_COUNT {
        xs.push(round(false)?);
    }
    xs.sort_by(|a, b| a.total_cmp(b));
    Ok(xs[xs.len() / 2]) // median: one slow outlier must not move the number
}

/// A phase's measured Mbps, plus the warning to show when Cloudflare cut it
/// short with a 429 mid-phase (the partial result is kept, not failed).
type PhaseResult = Result<(f64, Option<String>, Option<i64>), String>;

/// One download stream's outcome: (counted bytes, seconds while counting,
/// last error). Counted bytes over their own span is the stream's rate; the
/// test's result is the sum of the stream rates, which is the aggregate
/// throughput of the parallel streams.
type DownResult = (u64, f64, Option<String>, Option<i64>);

fn span_secs(first: Option<Instant>, last: Option<Instant>) -> f64 {
    match (first, last) {
        (Some(f), Some(l)) if l > f => l.saturating_duration_since(f).as_secs_f64(),
        _ => 0.0,
    }
}

fn stream_down(
    ag: &ureq::Agent,
    base: &str, // "...__down?bytes=" — the chunk size is appended per request
    idx: usize,
    warm_end: Instant,
    end: Instant,
    budget: &AtomicU64,
    cap: u64,
    stop: &AtomicBool,
) -> DownResult {
    let mut served = 0u64; // this stream's bytes, warm-up included
    let mut counted = 0u64;
    let mut first_count = None;
    let mut last_count = None;
    let mut last_err = None;
    let mut buf = [0u8; 65536];
    let mut size = DOWN_START;
    let mut max_size = DOWN_MAX; // lowered when a size is refused (403)
    if idx > 0 {
        thread::sleep(Duration::from_millis(STAGGER_MS * idx as u64));
    }
    while Instant::now() < end {
        if stop.load(Ordering::Relaxed) || budget.load(Ordering::Relaxed) >= cap {
            break;
        }
        let t0 = Instant::now();
        let url = format!("{base}{size}");
        let mut r = match ag.get(&url).call() {
            Ok(r) => r.into_reader(),
            Err(e) => {
                if is_limited(&e) {
                    // the per-IP byte budget is spent: stop every stream at once
                    stop.store(true, Ordering::Relaxed);
                    return (
                        counted,
                        span_secs(first_count, last_count),
                        Some(http_err(e)),
                        rate_limit_wait(&e),
                    );
                }
                if is_size_refused(&e) {
                    if size <= DOWN_MIN {
                        // even the smallest chunk is refused: endpoint changed
                        stop.store(true, Ordering::Relaxed);
                        return (
                            counted,
                            span_secs(first_count, last_count),
                            Some(http_err(e)),
                            None,
                        );
                    }
                    max_size = (size / 2).max(DOWN_MIN);
                    size = max_size;
                    note(format!("chunk refused, shrinking to {size}"));
                    continue;
                }
                let msg = http_err(e);
                eprintln!("stream error: {msg}");
                last_err = Some(msg);
                continue; // one failed request is not a failed stream
            }
        };
        loop {
            match r.read(&mut buf) {
                Ok(0) => break,
                Ok(k) => {
                    let now = Instant::now();
                    served += k as u64;
                    budget.fetch_add(k as u64, Ordering::Relaxed);
                    // warm-up ends with the clock, or once the connection
                    // has ramped, whichever comes first
                    if now >= warm_end || served >= DOWN_WARM_BYTES {
                        counted += k as u64;
                        first_count.get_or_insert(now);
                        last_count = Some(now);
                    }
                    if now >= end || budget.load(Ordering::Relaxed) >= cap {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("stream error: {e}");
                    last_err = Some(e.to_string());
                    break;
                }
            }
        }
        // adapt the chunk size to how fast that request came back
        let took = t0.elapsed();
        if took < Duration::from_millis(800) && size < max_size {
            size = (size * 2).min(max_size);
            note(format!("chunk grows to {size}"));
        } else if took > Duration::from_millis(3000) && size > DOWN_MIN {
            size = (size / 2).max(DOWN_MIN);
            note(format!("chunk shrinks to {size}"));
        }
    }
    (counted, span_secs(first_count, last_count), last_err, None)
}

/// (bytes, seconds) of one finished upload request.
type UpSample = (u64, f64);

/// One upload stream: finished requests as (bytes, seconds) samples, plus
/// the stream's last error. Every finished request counts; the warm-up and
/// the counted phase differ only in when they stop.
fn stream_up(
    ag: &ureq::Agent,
    url: &str,
    body: &[u8],
    end: Instant,
    budget: &AtomicU64,
    cap: u64,
    wanted: usize,
    wanted_bytes: u64,
    stop: &AtomicBool,
) -> (Vec<UpSample>, Option<String>, Option<i64>) {
    let mut samples = Vec::new();
    let mut own = 0u64;
    let mut last_err = None;
    while Instant::now() < end {
        if stop.load(Ordering::Relaxed)
            || budget.load(Ordering::Relaxed) + body.len() as u64 > cap
            || (samples.len() >= wanted && own >= wanted_bytes)
        {
            break;
        }
        let t = Instant::now();
        budget.fetch_add(body.len() as u64, Ordering::Relaxed);
        match ag
            .post(url)
            .set("Content-Type", "application/octet-stream")
            .timeout(Duration::from_secs(45))
            .send_bytes(body)
        {
            Ok(_) => {
                let dur = t.elapsed().as_secs_f64();
                samples.push((body.len() as u64, dur));
                own += body.len() as u64;
                note(format!("up {} bytes in {:.2}s", body.len(), dur));
            }
            Err(e) => {
                let fatal = is_limited(&e) || is_size_refused(&e);
                let msg = http_err(e);
                let cooldown = rate_limit_wait(&e);
                if fatal {
                    stop.store(true, Ordering::Relaxed);
                    return (samples, Some(msg), cooldown);
                }
                eprintln!("stream error: {msg}");
                last_err = Some(msg);
                continue; // one failed request is not a failed stream
            }
        }
    }
    (samples, last_err, None)
}

fn download(ag: &ureq::Agent) -> PhaseResult {
    let base = down_url();
    let base: &str = &base;
    let stop = Arc::new(AtomicBool::new(false));
    let stop = &stop;
    let start = Instant::now();
    let warm_end = start + Duration::from_secs_f64(warmup_secs());
    let end = start + Duration::from_secs_f64(warmup_secs() + measure_secs());
    let cap = down_cap();
    let budget = Arc::new(AtomicU64::new(0));
    let budget = &budget;
    let results: Vec<DownResult> = thread::scope(|s| {
        let hs: Vec<_> = (0..STREAMS)
            .map(|i| s.spawn(move || stream_down(ag, base, i, warm_end, end, budget, cap, stop)))
            .collect();
        hs.into_iter()
            .map(|h| h.join().unwrap_or((0, 0.0, Some("download thread crashed".to_string()), None)))
            .collect()
    });
    let bytes: u64 = results.iter().map(|r| r.0).sum();
    let rate: f64 = results.iter().filter(|r| r.1 > 0.0).map(|r| r.0 as f64 / r.1).sum();
    let cooldown = results.iter().filter_map(|r| r.3).max();
    note(format!(
        "download: {bytes} bytes, {:.0} Mbps (budget {}/{cap})",
        rate * 8.0 / 1e6,
        budget.load(Ordering::Relaxed)
    ));
    if bytes == 0 {
        // nothing was measured: the honest answer is the error itself
        return Err(results
            .iter()
            .find_map(|r| r.2.clone())
            .unwrap_or_else(|| "no data received".to_string()));
    }
    if stop.load(Ordering::Relaxed) {
        // limited mid-phase: keep the partial measurement, with the reason
        return Ok((
            rate * 8.0 / 1e6,
            results.iter().find_map(|r| r.2.clone()),
            cooldown,
        ));
    }
    Ok((rate * 8.0 / 1e6, None, cooldown))
}

fn upload(ag: &ureq::Agent, down_mbps: f64) -> PhaseResult {
    let url = up_url();
    let stop = Arc::new(AtomicBool::new(false));
    let stop = &stop;
    let start = Instant::now();
    let warm_end = start + Duration::from_secs_f64(warmup_secs());
    let end = warm_end + Duration::from_secs_f64(measure_secs());
    let cap = up_cap();
    let budget = Arc::new(AtomicU64::new(0));

    // Body size: about UP_TARGET_SECS of data per stream. First guess from
    // the download result, then re-measure during the warm-up.
    let per_stream_bps = down_mbps * 1e6 / 8.0 / STREAMS as f64; // bytes/s
    let body0 = (per_stream_bps * 1.0).clamp(UP_MIN as f64, 10_000_000.0) as u64;

    // Warm-up: ramps the connections and sizes the measured bodies. It ends
    // after two finished requests and 8 MB per stream, so a fast line cannot
    // spend the whole budget before the counted phase begins.
    let warm_body = vec![0u8; body0 as usize];
    let warm: Vec<UpSample> = thread::scope(|s| {
        let hs: Vec<_> = (0..STREAMS)
            .map(|_| {
                s.spawn(|| {
                    stream_up(ag, &url, &warm_body, warm_end, &budget, cap, 2, UP_WARM_MIN_BYTES, stop).0
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap_or_default()).flatten().collect()
    });
    // Best per-stream rate seen; the first request of each stream pays for
    // the TLS handshake, so the average would undershoot.
    let est = warm
        .iter()
        .map(|&(b, d)| if d > 0.0 { b as f64 / d } else { 0.0 })
        .fold(0.0f64, f64::max);
    let body_len = if est > 0.0 {
        (est * UP_TARGET_SECS).clamp(UP_MIN as f64, UP_MAX as f64) as u64
    } else {
        // Nothing finished during warm-up: the line is slower than the
        // download suggested; shrink until a body fits the window.
        (body0 / 8).max(UP_MIN)
    };
    note(format!("upload bodies: {body_len} bytes"));

    let up_body = vec![0u8; body_len as usize];
    let results: Vec<(Vec<UpSample>, Option<String>, Option<i64>)> = thread::scope(|s| {
        let hs: Vec<_> = (0..STREAMS)
            .map(|_| {
                s.spawn(|| stream_up(ag, &url, &up_body, end, &budget, cap, usize::MAX, u64::MAX, stop))
            })
            .collect();
        hs.into_iter()
            .map(|h| h.join().unwrap_or((Vec::new(), Some("upload thread crashed".to_string()), None)))
            .collect()
    });

    let bytes: u64 = results.iter().flat_map(|r| r.0.iter()).map(|s| s.0).sum();
    let cooldown = results.iter().filter_map(|r| r.2).max();
    // six parallel streams: the aggregate is the sum of the per-stream rates
    let rate: f64 = results
        .iter()
        .filter_map(|r| {
            let b: u64 = r.0.iter().map(|s| s.0).sum();
            let d: f64 = r.0.iter().map(|s| s.1).sum();
            (d > 0.0).then(|| b as f64 / d)
        })
        .sum();
    note(format!(
        "upload: {bytes} bytes, {:.0} Mbps (budget {}/{cap})",
        rate * 8.0 / 1e6,
        budget.load(Ordering::Relaxed)
    ));
    if bytes == 0 {
        return Err(
            results
                .iter()
                .find_map(|r| r.1.clone())
                .unwrap_or_else(|| "no data uploaded".to_string()),
        );
    }
    if stop.load(Ordering::Relaxed) {
        // limited mid-phase: keep the partial measurement, with the reason
        return Ok((
            rate * 8.0 / 1e6,
            results.iter().find_map(|r| r.1.clone()),
            cooldown,
        ));
    }
    Ok((rate * 8.0 / 1e6, None, cooldown))
}

fn run_test(tx: mpsc::Sender<Msg>) {
    let ag = agent();
    let r = (|| -> Result<Run, String> {
        let _ = tx.send(Msg::Phase("Ping", 0.05, None, None));
        let p = ping(&ag)?;
        let mut warn = None;
        let mut cooldown_until = None;

        let _ = tx.send(Msg::Phase("Download", 0.2, Some(p), None));
        let (d, w, c) = download(&ag)?;
        warn = warn.or(w);
        cooldown_until = cooldown_until.or(c);

        let _ = tx.send(Msg::Phase("Upload", 0.65, Some(p), Some(d)));
        let (u, w, c) = upload(&ag, d)?;
        warn = warn.or(w);
        cooldown_until = cooldown_until.or(c);

        if let Some(secs) = cooldown_until {
            cooldown_until = Some(now_unix() + secs as i64);
        }

        Ok(Run {
            down: d,
            up: u,
            ping: p,
            warn,
            cooldown_until,
        })
    })();
    let _ = tx.send(Msg::Done(r));
}

/// One test at a time: a click while a test runs is ignored. Runs start from
/// the page's button or header ▶, the 2x2 tile's ▶, or a tap on the tile body.
fn start_test(st: &mut State, tx: &mpsc::Sender<Msg>) {
    if st.phase.is_some() {
        return;
    }

    if let Some(until) = st.cooldown_until {
        if until > now_unix() {
            st.warn = Some(cooldown_message(until));
            st.err = None;
            draw(st);
            return;
        }
        st.cooldown_until = None;
        clear_cooldown(&st.dir);
    }

    st.phase = Some("Starting");
    st.err = None;
    st.warn = None;
    st.live_ping = None;
    st.live_down = None;
    send(json!({"type":"active","active":true}));
    let t = tx.clone();
    thread::spawn(move || run_test(t));
    draw(st);
}

// ---------- storage ----------

/// hello's data_dir wins; the env var covers the time before hello.
fn resolve_dir(v: Option<&Value>) -> PathBuf {
    if let Some(d) = v.and_then(|v| v["data_dir"].as_str()) {
        return PathBuf::from(d);
    }
    if let Ok(d) = env::var("FENN_DATA_DIR") {
        return PathBuf::from(d);
    }
    PathBuf::from(".")
}

fn write_atomic(path: PathBuf, contents: String) {
    if let Some(d) = path.parent() {
        let _ = fs::create_dir_all(d);
    }
    let tmp = path.with_extension("tmp");
    if fs::write(&tmp, contents).is_ok() {
        let _ = fs::rename(tmp, path);
    }
}

fn load_cooldown(dir: &PathBuf) -> Option<i64> {
    let Ok(s) = fs::read_to_string(dir.join("cooldown.json")) else {
        return None;
    };
    let Ok(v) = serde_json::from_str::<Value>(&s) else {
        return None;
    };
    v.as_i64()
}

fn save_cooldown(dir: &PathBuf, until: i64) {
    write_atomic(dir.join("cooldown.json"), until.to_string());
}

fn clear_cooldown(dir: &PathBuf) {
    let _ = fs::remove_file(dir.join("cooldown.json"));
}

fn load_hist(dir: &PathBuf) -> Vec<Run> {
    let Ok(s) = fs::read_to_string(dir.join("history.json")) else { return vec![] };
    let Ok(Value::Array(a)) = serde_json::from_str::<Value>(&s) else { return vec![] };
    a.iter()
        .filter_map(|v| {
            Some(Run {
                down: v[0].as_f64()?,
                up: v[1].as_f64()?,
                ping: v[2].as_f64()?,
                warn: None,
                cooldown_until: None,
            })
        })
        .collect()
}

fn save_hist(dir: &PathBuf, h: &[Run]) {
    let v: Vec<Value> = h.iter().map(|r| json!([r.down, r.up, r.ping])).collect();
    write_atomic(dir.join("history.json"), Value::Array(v).to_string());
}

fn load_layout(dir: &PathBuf) -> Option<(TileSize, f64, f64)> {
    let v: Value = serde_json::from_str(&fs::read_to_string(dir.join("layout.json")).ok()?).ok()?;
    let w = v["w"].as_f64()?;
    let h = v["h"].as_f64()?;
    Some((TileSize::from_layout(v["size"].as_str(), w, h), w, h))
}

fn save_layout(dir: &PathBuf, size: TileSize, w: f64, h: f64) {
    write_atomic(
        dir.join("layout.json"),
        json!({"size": size.name(), "w": w, "h": h}).to_string(),
    );
}

// ---------- views ----------

fn fmt(x: f64) -> String {
    if x >= 100.0 {
        format!("{x:.0}")
    } else {
        format!("{x:.1}")
    }
}

fn stat(label: &str, val: String) -> Value {
    json!({"type":"card","gap":2,"children":[
        {"type":"text","text":label,"size":"small","color":"dim"},
        {"type":"text","text":val,"size":"big"}]})
}

fn tile_view(st: &State) -> Value {
    let last = st.hist.last();

    // small (1x1, 44x44): one big number
    if st.tile_size == TileSize::Small {
        let (top, bottom) = match (st.phase, last) {
            (Some(_), _) => ("…".to_string(), "Test".to_string()),
            (None, Some(r)) => (fmt(r.down), "Mbps".to_string()),
            _ => ("–".to_string(), "Mbps".to_string()),
        };
        return json!({"type":"column","align":"center","justify":"center","gap":0,"children":[
            {"type":"text","text":top,"size":"title"},
            {"type":"text","text":bottom,"size":"small","color":"dim"}]});
    }

    // large (2x2, 104x104): all three numbers and the history
    if st.tile_size == TileSize::Large {
        let (l1, l2, l3) = if let Some(p) = st.phase {
            // a run in progress: each number appears as its phase finishes
            let d = st
                .live_down
                .map(|d| format!("↓ {} Mbps", fmt(d)))
                .unwrap_or_else(|| "↓ …".to_string());
            let u = (p == "Upload").then(|| "↑ …".to_string()).unwrap_or_default();
            let g = st
                .live_ping
                .map(|g| format!("ping {} ms", fmt(g)))
                .unwrap_or_else(|| "ping …".to_string());
            (d, u, g)
        } else {
            match last {
                Some(r) => (
                    format!("↓ {} Mbps", fmt(r.down)),
                    format!("↑ {} Mbps", fmt(r.up)),
                    format!("ping {} ms", fmt(r.ping)),
                ),
                _ => ("Tap to test".to_string(), String::new(), String::new()),
            }
        };
        let mut kids = vec![
            json!({"type":"row","children":[
                {"type":"text","text":"Speedtest","size":"small","color":"dim"},
                {"type":"spacer"},
                {"type":"button","id":"run","icon":"play"}]}),
            json!({"type":"text","text":l1,"size":"body"}),
            json!({"type":"text","text":l2,"size":"body"}),
            json!({"type":"text","text":l3,"size":"small","color":"dim"}),
        ];
        if st.hist.len() > 1 {
            let vals: Vec<f64> = st.hist.iter().map(|r| r.down).collect();
            kids.push(json!({"type":"sparkline","values":vals,"height":20}));
        }
        return json!({"type":"column","gap":2,"children":kids});
    }

    // wide (2x1, 104x36): icon and two lines
    let line = if let Some(p) = st.phase {
        match (p, st.live_down) {
            ("Ping", _) => "Ping…".to_string(),
            ("Download", _) => "↓ …".to_string(),
            ("Upload", Some(d)) => format!("↓{} ↑…", fmt(d)),
            _ => "…".to_string(),
        }
    } else {
        match last {
            Some(r) => format!("↓{} ↑{}", fmt(r.down), fmt(r.up)),
            None => "Tap to test".to_string(),
        }
    };
    json!({"type":"row","align":"center","gap":8,"children":[
        {"type":"icon","icon":"speedometer","size":22,"color":"accent"},
        {"type":"column","gap":0,"children":[
            {"type":"text","text":"Speedtest","size":"small","color":"dim"},
            {"type":"text","text":line,"size":"body"}]}]})
}

fn draw(st: &State) {
    let last = st.hist.last();
    send(json!({"type":"tile","view":tile_view(st)}));

    let dash = || "–".to_string();
    let live = || "…".to_string();
    let (d, u, p) = if st.phase.is_some() {
        // live run: each card shows its number the moment its phase ends
        (
            st.live_down.map(fmt).unwrap_or_else(live),
            live(),
            st.live_ping.map(fmt).unwrap_or_else(live),
        )
    } else {
        match last {
            Some(r) => (fmt(r.down), fmt(r.up), fmt(r.ping)),
            None => (dash(), dash(), dash()),
        }
    };
    let label = if st.phase.is_some() { "Testing…" } else { "Run test" };
    let mut kids = vec![json!({"type":"grid","columns":3,"gap":8,"children":[
        stat("Download Mbps", d),
        stat("Upload Mbps", u),
        stat("Ping ms", p)]})];
    if st.hist.len() > 1 {
        let vals: Vec<f64> = st.hist.iter().map(|r| r.down).collect();
        kids.push(json!({"type":"text","text":"Download history","size":"small","color":"dim"}));
        kids.push(json!({"type":"sparkline","values":vals,"height":80}));
    }
    if let Some(until) = st.cooldown_until {
        if until > now_unix() {
            kids.push(json!({
                "type":"text",
                "text": cooldown_message(until),
                "color":"orange",
                "size":"small"
            }));
        }
    }
    if let Some(w) = &st.warn {
        kids.push(json!({"type":"text","text":w,"color":"orange","size":"small"}));
    }
    if let Some(e) = &st.err {
        kids.push(json!({"type":"text","text":e,"color":"red","size":"small"}));
    }
    kids.push(json!({"type":"button","id":"run","label":label,"icon":"play","primary":true,"width":0}));
    send(json!({"type":"page",
        "actions":[{"id":"run","icon":"play","tooltip":"Run test"}],
        "view":{"type":"column","gap":8,"scroll":true,"children":kids}}));
}

// ---------- main ----------

fn main() {
    let (tx, rx) = mpsc::channel::<Msg>();

    let tx_in = tx.clone();
    thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(l) = line else { break };
            if let Ok(v) = serde_json::from_str::<Value>(&l) {
                if tx_in.send(Msg::Fenn(v)).is_err() {
                    break;
                }
            }
        }
        // stdin closed: Fenn is gone
        let _ = tx_in.send(Msg::Fenn(json!({"type":"shutdown"})));
    });

    let mut st = State {
        hist: vec![],
        phase: None,
        live_ping: None,
        live_down: None,
        err: None,
        warn: None,
        cooldown_until: None,
        tile_size: TileSize::Wide,
        tile_w: 104.0,
        tile_h: 36.0,
        dir: resolve_dir(None),
    };
    st.hist = load_hist(&st.dir);
    st.cooldown_until = load_cooldown(&st.dir);
    if let Some(until) = st.cooldown_until {
        if until <= now_unix() {
            st.cooldown_until = None;
            clear_cooldown(&st.dir);
        }
    }
    if let Some((size, w, h)) = load_layout(&st.dir) {
        st.tile_size = size;
        st.tile_w = w;
        st.tile_h = h;
    }

    for m in rx {
        match m {
            Msg::Fenn(v) => match v["type"].as_str() {
                Some("hello") => {
                    // hello carries the data dir; reload what we store there.
                    st.dir = resolve_dir(Some(&v));
                    st.hist = load_hist(&st.dir);
                    st.cooldown_until = load_cooldown(&st.dir);
                    if let Some(until) = st.cooldown_until {
                        if until <= now_unix() {
                            st.cooldown_until = None;
                            clear_cooldown(&st.dir);
                        }
                    }
                    if let Some((size, w, h)) = load_layout(&st.dir) {
                        st.tile_size = size;
                        st.tile_w = w;
                        st.tile_h = h;
                    }
                    draw(&st);
                }
                Some("layout") if v["target"] == "tile" => {
                    st.tile_w = v["width"].as_f64().unwrap_or(st.tile_w);
                    st.tile_h = v["height"].as_f64().unwrap_or(st.tile_h);
                    st.tile_size = TileSize::from_layout(v["size"].as_str(), st.tile_w, st.tile_h);
                    // Remember it: a plugin restart (dev mode reloads on every
                    // save) then draws the right size before any layout
                    // message arrives.
                    save_layout(&st.dir, st.tile_size, st.tile_w, st.tile_h);
                    draw(&st);
                }
                Some("event") if v["id"] == "run" && v["kind"] == "click" => {
                    start_test(&mut st, &tx);
                }
                Some("tile_click") => {
                    // A tap on the tile body runs a test; Fenn opens the page
                    // beside it, which then shows the live numbers too.
                    start_test(&mut st, &tx);
                }
                Some("shutdown") => {
                    save_hist(&st.dir, &st.hist);
                    break;
                }
                _ => {}
            },
            Msg::Phase(p, progress, ping, down) => {
                st.phase = Some(p);
                st.live_ping = ping;
                st.live_down = down;
                send(json!({"type":"badge","badge":{"kind":"ring","progress":progress,"label":&p[..1]}}));
                draw(&st);
            }
            Msg::Done(r) => {
                st.phase = None;
                st.live_ping = None;
                st.live_down = None;
                match r {
                    Ok(run) => {
                        st.warn = run.warn.clone();
                        st.cooldown_until = run.cooldown_until;
                        if let Some(until) = st.cooldown_until {
                            if until > now_unix() {
                                save_cooldown(&st.dir, until);
                            } else {
                                st.cooldown_until = None;
                                clear_cooldown(&st.dir);
                            }
                        } else {
                            clear_cooldown(&st.dir);
                        }
                        st.hist.push(run);
                        if st.hist.len() > KEEP {
                            st.hist.remove(0);
                        }
                        save_hist(&st.dir, &st.hist);
                    }
                    Err(e) => {
                        eprintln!("speedtest failed: {e}");
                        st.err = Some(e);
                    }
                }
                send(json!({"type":"badge","badge":null}));
                send(json!({"type":"active","active":false}));
                draw(&st);
            }
        }
    }
}
