// Per-thread worker. Each worker is an OS thread running a current_thread
// tokio runtime (mirroring wrk's per-thread ae event loop). It owns a slice of
// the total connections and a rate recorder that ticks every 100ms.
//
// IMPORTANT: wrk gives each thread its own `thread->requests` counter that
// `record_rate` samples and resets every 100ms. Sharing a single global
// counter across workers would make each 100ms sample see either the full
// burst or nothing (depending on which worker swaps first), producing the
// huge Req/Sec variance seen in an earlier revision. So we keep a per-worker
// rate counter here and fold only `complete`/`bytes`/errors into the global
// Counters.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use quinn::Endpoint;
use tokio::runtime::Builder;
use tokio::task::LocalSet;

/// Build a single-threaded current_thread runtime. This is the crux of wrk's
/// model: ONE event loop per OS thread, giving perfect cache locality and zero
/// cross-thread scheduling. Using `Runtime::new()` here instead would spawn a
/// *multi-thread* runtime (num_cpus worker threads) per worker — so `-t N`
/// quietly creates `N × num_cpus` tokio threads contending for the cores, and
/// throughput *falls* as `-t` rises. We measured exactly that regression.
fn worker_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    Builder::new_current_thread()
        .enable_all()
        // event_interval(1): for a pure-IO keep-alive loop that awaits every
        // iteration, poll the I/O driver every tick instead of every 61 (the
        // default). This cuts wakeup latency for the tight request loop.
        // max_blocking_threads(1): we never spawn_blocking, so trim the
        // blocking pool's structures to the minimum.
        .event_interval(1)
        .max_blocking_threads(1)
        .build()
}

use crate::config::Config;
use crate::connection::{Counters, CounterSnapshot, run_connection};
use crate::h3::run_h3_connection;
use crate::stats::Stats;

pub const RECORD_INTERVAL_MS: u64 = 100;

/// Run a worker on the current OS thread with a current_thread runtime.
/// Returns this worker's counter snapshot when `stop` is set (signalled by the
/// rate recorder exiting the loop). The snapshot is summed across workers in
/// main; the counters themselves are never shared across cores.
#[allow(clippy::too_many_arguments)]
pub fn run_worker(
    cfg: Arc<Config>,
    connections: u64,
    latency: Arc<Stats>,
    requests_stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    tls: Option<tokio_rustls::TlsConnector>,
    server_name: Option<rustls::pki_types::ServerName<'static>>,
) -> CounterSnapshot {
    let runtime = match worker_runtime() {
        Ok(r) => r,
        Err(_) => return CounterSnapshot::default(),
    };
    let local = LocalSet::new();

    // This worker's own counters — never shared with other workers, so the
    // connection tasks' relaxed `fetch_add`s stay on one core (no cacheline
    // bounce). See the comment on `Counters`.
    let counters = Arc::new(Counters::default());

    // Per-worker rate counter, mirroring wrk's thread-local thread->requests.
    // Connection tasks bump this; the rate recorder samples+resets it every
    // 100ms. Not shared across workers, so no cross-worker swap contention.
    let rate_counter = Arc::new(AtomicU64::new(0));

    let counters_handle = counters.clone();
    local.block_on(&runtime, async move {
        // Spawn one task per connection.
        for _ in 0..connections {
            let cfg = cfg.clone();
            let counters = counters.clone();
            let latency = latency.clone();
            let stop = stop.clone();
            let tls = tls.clone();
            let sni = server_name.clone();
            let rate_counter = rate_counter.clone();
            tokio::task::spawn_local(async move {
                run_connection(cfg, counters, latency, stop, tls, sni, rate_counter).await;
            });
        }

        run_rate_recorder(&rate_counter, &requests_stats, &stop).await;
    });

    counters_handle.snapshot()
}

/// HTTP/3 variant: each worker opens `connections` QUIC connections, each with
/// `streams_per_conn` concurrent streams (multiplexed). Otherwise identical
/// bookkeeping to run_worker.
#[allow(clippy::too_many_arguments)]
pub fn run_h3_worker(
    cfg: Arc<Config>,
    connections: u64,
    latency: Arc<Stats>,
    requests_stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    endpoint: Arc<Endpoint>,
) -> CounterSnapshot {
    let runtime = match worker_runtime() {
        Ok(r) => r,
        Err(_) => return CounterSnapshot::default(),
    };
    let local = LocalSet::new();

    let counters = Arc::new(Counters::default());
    let rate_counter = Arc::new(AtomicU64::new(0));

    let counters_handle = counters.clone();
    local.block_on(&runtime, async move {
        for _ in 0..connections {
            let cfg = cfg.clone();
            let counters = counters.clone();
            let latency = latency.clone();
            let stop = stop.clone();
            let endpoint = endpoint.clone();
            let rate_counter = rate_counter.clone();
            tokio::task::spawn_local(async move {
                run_h3_connection(cfg, counters, latency, stop, endpoint, rate_counter).await;
            });
        }

        run_rate_recorder(&rate_counter, &requests_stats, &stop).await;
    });

    counters_handle.snapshot()
}

/// HTTP/2 variant: each worker opens `connections` TLS connections, each with
/// `streams_per_conn` concurrent multiplexed streams. Same bookkeeping as the
/// other workers.
#[allow(clippy::too_many_arguments)]
pub fn run_h2_worker(
    cfg: Arc<Config>,
    connections: u64,
    latency: Arc<Stats>,
    requests_stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    tls: tokio_rustls::TlsConnector,
    server_name: rustls::pki_types::ServerName<'static>,
) -> CounterSnapshot {
    let runtime = match worker_runtime() {
        Ok(r) => r,
        Err(_) => return CounterSnapshot::default(),
    };
    let local = LocalSet::new();

    let counters = Arc::new(Counters::default());
    let rate_counter = Arc::new(AtomicU64::new(0));

    let counters_handle = counters.clone();
    local.block_on(&runtime, async move {
        for _ in 0..connections {
            let cfg = cfg.clone();
            let counters = counters.clone();
            let latency = latency.clone();
            let stop = stop.clone();
            let tls = tls.clone();
            let server_name = server_name.clone();
            let rate_counter = rate_counter.clone();
            tokio::task::spawn_local(async move {
                crate::h2::run_h2_connection(
                    cfg, counters, latency, stop, tls, server_name, rate_counter,
                )
                .await;
            });
        }

        run_rate_recorder(&rate_counter, &requests_stats, &stop).await;
    });

    counters_handle.snapshot()
}

/// Shared 100ms rate recorder loop. Mirrors wrk's record_rate (wrk.c:273):
/// every tick, convert this worker's request count into req/s and record it.
async fn run_rate_recorder(
    rate_counter: &Arc<AtomicU64>,
    requests_stats: &Arc<Stats>,
    stop: &Arc<AtomicBool>,
) {
    let mut start = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(RECORD_INTERVAL_MS)).await;

        let reqs = rate_counter.swap(0, Ordering::Relaxed);
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
}
