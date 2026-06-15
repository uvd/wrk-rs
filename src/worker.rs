// Per-thread worker. Each worker is an OS thread running a current_thread
// tokio runtime (mirroring wrk's per-thread ae event loop). It owns a slice of
// the total connections and a rate recorder that ticks every 100ms.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use tokio::task::LocalSet;

use crate::config::Config;
use crate::connection::{Counters, run_connection};
use crate::stats::Stats;

pub const RECORD_INTERVAL_MS: u64 = 100;

// Per-worker counters are shared directly across workers via Arc<Counters>.

/// Run a worker on the current OS thread with a current_thread runtime.
/// Returns when `stop` is set (signalled by the rate recorder exiting the loop).
pub fn run_worker(
    cfg: Arc<Config>,
    connections: u64,
    latency: Arc<Stats>,
    requests_stats: Arc<Stats>,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
    tls: Option<tokio_rustls::TlsConnector>,
    server_name: Option<rustls::pki_types::ServerName<'static>>,
) {
    let runtime = match Runtime::new() {
        Ok(r) => r,
        Err(_) => return,
    };
    let local = LocalSet::new();
    local.block_on(&runtime, async move {
        // Spawn one task per connection.
        for _ in 0..connections {
            let cfg = cfg.clone();
            let counters = counters.clone();
            let latency = latency.clone();
            let stop = stop.clone();
            let tls = tls.clone();
            let sni = server_name.clone();
            tokio::task::spawn_local(async move {
                run_connection(cfg, counters, latency, stop, tls, sni).await;
            });
        }

        // Rate recorder: every RECORD_INTERVAL_MS, convert this worker's
        // request count into req/s and record it into the global requests
        // histogram (mirrors wrk's record_rate in wrk.c:273).
        let mut start = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(RECORD_INTERVAL_MS)).await;

            let reqs = counters.requests.swap(0, Ordering::Relaxed);
            if reqs > 0 {
                let elapsed_ms = start.elapsed().as_millis().max(1) as u64;
                let rate = ((reqs as f64 / elapsed_ms as f64) * 1000.0) as u64;
                requests_stats.record(rate);
            }
            start = Instant::now();

            if stop.load(Ordering::Relaxed) {
                break;
            }
        }
    });
}
