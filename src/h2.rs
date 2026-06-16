// HTTP/2 benchmark connection.
//
// HTTP/2 multiplexes many streams over a single TLS connection (no head-of-line
// blocking between streams), the same model as HTTP/3. Each worker opens
// `-c/threads` TLS connections and runs `--streams` concurrent stream-tasks
// per connection. This mirrors h3.rs but over TLS+TCP using hyper's http2
// client.
//
// Hot-path optimisations mirror connection.rs (HTTP/1): per-task local
// counters flushed on exit, byte counting via DrainBody (no CountingStream
// wrapper layer), spawn_local for the connection driver.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::Full;
use hyper::client::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::config::Config;
use crate::connection::{Counters, drain_body, estimate_response_bytes};

/// Run one HTTP/2 connection: open a TLS+H2 connection and drive
/// `streams_per_conn` concurrent stream-tasks until `stop` or conn failure.
pub async fn run_h2_connection(
    cfg: Arc<Config>,
    counters: Arc<Counters>,
    latency: Arc<crate::stats::Stats>,
    stop: Arc<AtomicBool>,
    tls: TlsConnector,
    server_name: rustls::pki_types::ServerName<'static>,
    rate_counter: Arc<std::sync::atomic::AtomicU64>,
) {
    let body_bytes: Bytes = if cfg.body.is_empty() {
        Bytes::new()
    } else {
        Bytes::copy_from_slice(&cfg.body)
    };
    let method: http::Method = cfg.method.parse().unwrap_or(http::Method::GET);
    let host_header = cfg.host_header();
    let timeout_dur = Duration::from_millis(cfg.timeout);

    // h2 derives :scheme/:authority/:path pseudo-headers from the request URI.
    let full_uri: http::Uri = format!("https://{}{}", host_header, cfg.path)
        .parse()
        .unwrap_or_else(|_| http::Uri::from_static("https://localhost/"));

    // Pre-build static headers once per connection.
    let mut static_headers: http::HeaderMap = http::HeaderMap::with_capacity(cfg.headers.len() + 2);
    let _ = static_headers.insert(
        http::HeaderName::from_static("user-agent"),
        http::HeaderValue::from_static("wrk-rs/0.1.0"),
    );
    for (k, v) in &cfg.headers {
        if let (Ok(name), Ok(val)) = (
            http::HeaderName::from_bytes(k.as_bytes()),
            http::HeaderValue::from_str(v),
        ) {
            let _ = static_headers.append(name, val);
        }
    }
    let static_headers = Arc::new(static_headers);

    while !stop.load(Ordering::Relaxed) {
        // ---- connect TCP + TLS (with h2 ALPN) ----
        let addr = format!("{}:{}", cfg.host, cfg.port);
        let tcp = match TcpStream::connect(&addr).await {
            Ok(t) => t,
            Err(_) => {
                counters.errors_connect.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        let _ = tcp.set_nodelay(true);
        #[cfg(target_os = "linux")]
        let _ = tcp.set_quickack(true);
        let tls = match tls.clone().connect(server_name.clone(), tcp).await {
            Ok(t) => t,
            Err(_) => {
                counters.errors_connect.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        // ---- H2 handshake ----
        let (sender, conn) = match http2::handshake(TokioExecutor::new(), TokioIo::new(tls)).await {
            Ok(v) => v,
            Err(_) => {
                counters.errors_connect.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        // Drive the H2 connection in the background. spawn_local keeps the
        // driver on the same LocalSet queue as the stream tasks.
        tokio::task::spawn_local(async move {
            let _ = conn.await;
        });

        // Spawn `streams_per_conn` concurrent stream-tasks sharing the sender.
        let mut stream_handles = Vec::new();
        for _ in 0..cfg.streams_per_conn {
            let counters = counters.clone();
            let latency = latency.clone();
            let stop = stop.clone();
            let sender = sender.clone();
            let rate_counter = rate_counter.clone();
            let static_headers = static_headers.clone();
            let full_uri = full_uri.clone();
            let body_bytes = body_bytes.clone();
            let method = method.clone();
            stream_handles.push(tokio::task::spawn_local(run_h2_stream(
                counters,
                latency,
                stop,
                sender,
                static_headers,
                full_uri,
                body_bytes,
                method,
                rate_counter,
                timeout_dur,
            )));
        }
        for h in stream_handles {
            let _ = h.await;
        }
    }
}

/// Run one HTTP/2 stream: send requests back-to-back over the shared
/// multiplexed connection until `stop` or the connection errors.
#[allow(clippy::too_many_arguments)]
async fn run_h2_stream(
    counters: Arc<Counters>,
    latency: Arc<crate::stats::Stats>,
    stop: Arc<AtomicBool>,
    mut sender: http2::SendRequest<Full<Bytes>>,
    static_headers: Arc<http::HeaderMap>,
    full_uri: http::Uri,
    body_bytes: Bytes,
    method: http::Method,
    rate_counter: Arc<std::sync::atomic::AtomicU64>,
    timeout_dur: Duration,
) {
    let mut local_complete: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        let req = build_h2_request(&method, &full_uri, &static_headers, body_bytes.clone());

        let start = Instant::now();
        rate_counter.fetch_add(1, Ordering::Relaxed);

        let resp = match sender.send_request(req).await {
            Ok(r) => r,
            Err(_) => {
                counters.errors_write.fetch_add(1, Ordering::Relaxed);
                counters.complete.fetch_add(local_complete, Ordering::Relaxed);
                return;
            }
        };
        let status = resp.status().as_u16();
        let resp_headers = resp.headers().clone();
        // Zero-alloc drain; tally body bytes. No CountingStream wrapper.
        let body_bytes_recv = match drain_body(resp.into_body()).await {
            Ok(n) => n,
            Err(_) => {
                counters.errors_read.fetch_add(1, Ordering::Relaxed);
                counters.complete.fetch_add(local_complete, Ordering::Relaxed);
                return;
            }
        };
        let header_bytes = estimate_response_bytes(status, &resp_headers);
        counters.bytes.fetch_add(header_bytes + body_bytes_recv, Ordering::Relaxed);

        let elapsed = start.elapsed();
        let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        local_complete += 1;
        if status > 399 {
            counters.errors_status.fetch_add(1, Ordering::Relaxed);
        }
        if elapsed > timeout_dur {
            counters.errors_timeout.fetch_add(1, Ordering::Relaxed);
            counters.complete.fetch_add(local_complete, Ordering::Relaxed);
            return;
        }
        if !latency.record(elapsed_us) {
            counters.errors_timeout.fetch_add(1, Ordering::Relaxed);
        }
    }
    // Flush local accumulator on clean exit.
    counters.complete.fetch_add(local_complete, Ordering::Relaxed);
}

fn build_h2_request(
    method: &http::Method,
    full_uri: &http::Uri,
    headers: &http::HeaderMap,
    body: Bytes,
) -> http::Request<Full<Bytes>> {
    let body_len = body.len();
    let mut builder = http::Request::builder().method(method.clone()).uri(full_uri.clone());
    for (name, val) in headers.iter() {
        builder = builder.header(name.clone(), val.clone());
    }
    if body_len > 0 {
        builder = builder.header(http::header::CONTENT_LENGTH, http::HeaderValue::from(body_len));
    }
    builder.body(Full::new(body)).expect("valid h2 request")
}
