use serde_json::{json, Value};
use std::io::{BufRead, Read, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use std::{env, fs, thread};

const DOWN: &str = "https://speed.cloudflare.com/__down?bytes=";
const UP: &str = "https://speed.cloudflare.com/__up";
const STREAMS: usize = 4;
const TEST_SECS: u64 = 8;
const KEEP: usize = 20;

#[derive(Clone, Copy)]
struct Run {
    down: f64,
    up: f64,
    ping: f64,
}

enum Msg {
    Fenn(Value),
    Phase(&'static str, f64),
    Done(Result<Run, String>),
}

struct State {
    hist: Vec<Run>,
    phase: Option<&'static str>,
    err: Option<String>,
    tile_w: f64,
    tile_h: f64,
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
        .build()
}

fn ping(ag: &ureq::Agent) -> Result<f64, String> {
    let url = format!("{DOWN}0");
    let fetch = || -> Result<(), String> {
        let mut sink = Vec::new();
        ag.get(&url)
            .call()
            .map_err(|e| e.to_string())?
            .into_reader()
            .read_to_end(&mut sink)
            .map_err(|e| e.to_string())?;
        Ok(())
    };
    fetch()?; // warm-up: opens the connection, not counted
    let mut total = 0.0;
    for _ in 0..5 {
        let s = Instant::now();
        fetch()?;
        total += s.elapsed().as_secs_f64() * 1000.0;
    }
    Ok(total / 5.0)
}

fn mbps(bytes: u64, secs: f64) -> f64 {
    bytes as f64 * 8.0 / secs / 1e6
}

fn download(ag: &ureq::Agent) -> Result<f64, String> {
    let url = format!("{DOWN}25000000");
    let start = Instant::now();
    let end = start + Duration::from_secs(TEST_SECS);
    let total: Result<u64, String> = thread::scope(|s| {
        let hs: Vec<_> = (0..STREAMS)
            .map(|_| {
                s.spawn(|| -> Result<u64, String> {
                    let mut n = 0u64;
                    let mut buf = [0u8; 65536];
                    while Instant::now() < end {
                        let mut r = ag.get(&url).call().map_err(|e| e.to_string())?.into_reader();
                        loop {
                            let k = r.read(&mut buf).map_err(|e| e.to_string())?;
                            if k == 0 {
                                break;
                            }
                            n += k as u64;
                            if Instant::now() >= end {
                                break;
                            }
                        }
                    }
                    Ok(n)
                })
            })
            .collect();
        hs.into_iter()
            .map(|h| h.join().map_err(|_| "download thread crashed".to_string()).and_then(|r| r))
            .sum()
    });
    Ok(mbps(total?, start.elapsed().as_secs_f64()))
}

fn upload(ag: &ureq::Agent) -> Result<f64, String> {
    let body = vec![0u8; 5_000_000];
    let start = Instant::now();
    let end = start + Duration::from_secs(TEST_SECS);
    let total: Result<u64, String> = thread::scope(|s| {
        let hs: Vec<_> = (0..STREAMS)
            .map(|_| {
                s.spawn(|| -> Result<u64, String> {
                    let mut n = 0u64;
                    while Instant::now() < end {
                        ag.post(UP)
                            .set("Content-Type", "application/octet-stream")
                            .send_bytes(&body)
                            .map_err(|e| e.to_string())?;
                        n += body.len() as u64;
                    }
                    Ok(n)
                })
            })
            .collect();
        hs.into_iter()
            .map(|h| h.join().map_err(|_| "upload thread crashed".to_string()).and_then(|r| r))
            .sum()
    });
    Ok(mbps(total?, start.elapsed().as_secs_f64()))
}

fn run_test(tx: mpsc::Sender<Msg>) {
    let ag = agent();
    let r = (|| -> Result<Run, String> {
        let _ = tx.send(Msg::Phase("Ping", 0.05));
        let ping = ping(&ag)?;
        let _ = tx.send(Msg::Phase("Download", 0.15));
        let down = download(&ag)?;
        let _ = tx.send(Msg::Phase("Upload", 0.6));
        let up = upload(&ag)?;
        Ok(Run { down, up, ping })
    })();
    let _ = tx.send(Msg::Done(r));
}

// ---------- history ----------

fn hist_path() -> PathBuf {
    PathBuf::from(env::var("FENN_DATA_DIR").unwrap_or_else(|_| ".".into())).join("history.json")
}

fn load() -> Vec<Run> {
    let Ok(s) = fs::read_to_string(hist_path()) else { return vec![] };
    let Ok(Value::Array(a)) = serde_json::from_str::<Value>(&s) else { return vec![] };
    a.iter()
        .filter_map(|v| {
            Some(Run { down: v[0].as_f64()?, up: v[1].as_f64()?, ping: v[2].as_f64()? })
        })
        .collect()
}

fn save(h: &[Run]) {
    let p = hist_path();
    if let Some(d) = p.parent() {
        let _ = fs::create_dir_all(d);
    }
    let v: Vec<Value> = h.iter().map(|r| json!([r.down, r.up, r.ping])).collect();
    let tmp = p.with_extension("tmp");
    if fs::write(&tmp, Value::Array(v).to_string()).is_ok() {
        let _ = fs::rename(tmp, p);
    }
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
    if st.tile_w < 60.0 {
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
    if st.tile_h > 60.0 {
        let (l1, l2, l3) = match (st.phase, last) {
            (Some(p), _) => (format!("{p}…"), String::new(), String::new()),
            (None, Some(r)) => (
                format!("↓ {} Mbps", fmt(r.down)),
                format!("↑ {} Mbps", fmt(r.up)),
                format!("ping {} ms", fmt(r.ping)),
            ),
            _ => ("Tap to test".to_string(), String::new(), String::new()),
        };
        let mut kids = vec![
            json!({"type":"text","text":"Speedtest","size":"small","color":"dim"}),
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
    let line = match (st.phase, last) {
        (Some(p), _) => format!("{p}…"),
        (None, Some(r)) => format!("↓{} ↑{}", fmt(r.down), fmt(r.up)),
        _ => "Tap to test".to_string(),
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
    let (d, u, p) = match last {
        Some(r) => (fmt(r.down), fmt(r.up), fmt(r.ping)),
        None => (dash(), dash(), dash()),
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
    if let Some(e) = &st.err {
        kids.push(json!({"type":"text","text":e,"color":"red","size":"small"}));
    }
    kids.push(json!({"type":"button","id":"run","label":label,"icon":"play","primary":true,"width":0}));
    send(json!({"type":"page","view":{"type":"column","gap":8,"scroll":true,"children":kids}}));
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

    let mut st = State { hist: load(), phase: None, err: None, tile_w: 104.0, tile_h: 36.0 };

    for m in rx {
        match m {
            Msg::Fenn(v) => match v["type"].as_str() {
                Some("hello") => draw(&st),
                Some("layout") if v["target"] == "tile" => {
                    st.tile_w = v["width"].as_f64().unwrap_or(104.0);
                    st.tile_h = v["height"].as_f64().unwrap_or(36.0);
                    draw(&st);
                }
                Some("event") if v["id"] == "run" && v["kind"] == "click" => {
                    if st.phase.is_none() {
                        st.phase = Some("Starting");
                        st.err = None;
                        send(json!({"type":"active","active":true}));
                        let t = tx.clone();
                        thread::spawn(move || run_test(t));
                        draw(&st);
                    }
                }
                Some("shutdown") => {
                    save(&st.hist);
                    break;
                }
                _ => {}
            },
            Msg::Phase(p, progress) => {
                st.phase = Some(p);
                send(json!({"type":"badge","badge":{"kind":"ring","progress":progress,"label":&p[..1]}}));
                draw(&st);
            }
            Msg::Done(r) => {
                st.phase = None;
                match r {
                    Ok(run) => {
                        st.hist.push(run);
                        if st.hist.len() > KEEP {
                            st.hist.remove(0);
                        }
                        save(&st.hist);
                    }
                    Err(e) => st.err = Some(e),
                }
                send(json!({"type":"badge","badge":null}));
                send(json!({"type":"active","active":false}));
                draw(&st);
            }
        }
    }
}
