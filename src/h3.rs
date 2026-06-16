// HTTP/3 (over QUIC) benchmark connection.
//
// One QUIC connection (via quinn) carries multiple concurrent HTTP/3 streams
// (multiplexed), which is HTTP/3's core advantage over HTTP/1.1. Each worker
// opens `-c/threads` QUIC connections, and each connection runs
// `--streams` concurrent stream-tasks. This mirrors the high-performance
// multiplexing model the user requested.
//
// Latency, bytes, and error accounting reuse the same `Counters` and `Stats`
// as the HTTP/1 path, so the report is consistent.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use h3::client::SendRequest;
use h3::quic::OpenStreams;
use quinn::{ClientConfig, Endpoint};

use crate::config::Config;
use crate::connection::Counters;

/// Build a quinn client `Endpoint` with a rustls config set up for HTTP/3.
///
/// `insecure` skips certificate verification (mirrors the TLS path). Returns
/// the endpoint; the caller connects with it.
pub fn build_endpoint(insecure: bool) -> std::io::Result<Endpoint> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut crypto = if insecure {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(crate::NoVerifier))
            .with_no_client_auth()
    } else {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    };
    // ALPN: advertise HTTP/3 so the server picks h3.
    crypto.alpn_protocols = vec![b"h3".to_vec()];

    let quic_cfg = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).map_err(std::io::Error::other)?,
    ));
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
        .map_err(std::io::Error::other)?;
    endpoint.set_default_client_config(quic_cfg);
    Ok(endpoint)
}

/// Resolve host:port to a SocketAddr (first A/AAAA record). QUIC connect needs
/// a concrete address.
async fn resolve(host: &str, port: u16) -> std::io::Result<std::net::SocketAddr> {
    use tokio::net::lookup_host;
    let mut it = lookup_host((host, port)).await?;
    it.next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "no addr"))
}

/// Run one QUIC connection: drive `streams_per_conn` concurrent stream-tasks
/// until `stop` is set or the connection fails (then reconnect).
pub async fn run_h3_connection(
    cfg: Arc<Config>,
    counters: Arc<Counters>,
    latency: Arc<crate::stats::Stats>,
    stop: Arc<AtomicBool>,
    endpoint: Arc<Endpoint>,
    rate_counter: Arc<AtomicU64>,
) {
    let body_bytes: Bytes = if cfg.body.is_empty() {
        Bytes::new()
    } else {
        Bytes::copy_from_slice(&cfg.body)
    };
    let method: http::Method = cfg.method.parse().unwrap_or(http::Method::GET);
    let host_header = cfg.host_header();
    let timeout_dur = Duration::from_millis(cfg.timeout);

    // h3 derives the :scheme/:authority/:path pseudo-headers from the request
    // URI, so we build a full URI (scheme://host[:port]/path) once. The Host
    // header is also kept for compatibility with servers that read it.
    let full_uri: http::Uri = format!("https://{}{}", host_header, cfg.path)
        .parse()
        .unwrap_or_else(|_| http::Uri::from_static("https://localhost/"));

    // Pre-build static headers once per connection (same optimisation as h1).
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
    let addr = match resolve(&cfg.host, cfg.port).await {
        Ok(a) => a,
        Err(_) => {
            counters.errors_connect.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    while !stop.load(Ordering::Relaxed) {
        // ---- open a QUIC connection ----
        let quinn_conn = match endpoint.connect(addr, &cfg.host) {
            Ok(connecting) => match connecting.await {
                Ok(c) => c,
                Err(_) => {
                    counters.errors_connect.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            },
            Err(_) => {
                counters.errors_connect.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };

        // Wrap in h3-quinn and build the HTTP/3 client driver.
        let h3_conn = h3_quinn::Connection::new(quinn_conn);
        let (mut drive_conn, send_request) = match h3::client::new(h3_conn).await {
            Ok(v) => v,
            Err(_) => {
                counters.errors_connect.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        // Drive the HTTP/3 control connection in the background. spawn_local
        // keeps the driver on the same LocalSet queue as the stream tasks.
        tokio::task::spawn_local(async move {
            let _ = drive_conn.wait_idle().await;
        });

        // Spawn `streams_per_conn` concurrent stream-tasks. Build the request
        // template once; stream tasks clone it per request (cheap — Request<()>
        // and HeaderMap are Bytes-backed). spawn_local to stay on the LocalSet.
        let req_template = build_h3_request(&method, &full_uri, &static_headers, &body_bytes);
        let mut stream_handles = Vec::new();
        for _ in 0..cfg.streams_per_conn {
            let counters = counters.clone();
            let latency = latency.clone();
            let stop = stop.clone();
            let send_request = send_request.clone();
            let rate_counter = rate_counter.clone();
            let req_template = req_template.clone();
            stream_handles.push(tokio::task::spawn_local(run_h3_stream(
                counters,
                latency,
                stop,
                send_request,
                req_template,
                rate_counter,
                timeout_dur,
            )));
        }
        // Wait for all stream tasks to finish (they exit on stop or conn error).
        for h in stream_handles {
            let _ = h.await;
        }
    }
}

/// Run one HTTP/3 stream: send requests back-to-back over a single
/// multiplexed stream slot until `stop` or the connection errors.
#[allow(clippy::too_many_arguments)]
async fn run_h3_stream<B>(
    counters: Arc<Counters>,
    latency: Arc<crate::stats::Stats>,
    stop: Arc<AtomicBool>,
    mut send_request: SendRequest<B, Bytes>,
    req_template: http::Request<()>,
    rate_counter: Arc<AtomicU64>,
    timeout_dur: Duration,
)
// The OpenStreams bound comes from h3's SendRequest generics; using a concrete
// type alias is awkward, so we accept the generic.
where
    B: OpenStreams<Bytes> + Clone + Send + Sync + 'static,
{
    let mut local_complete: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        let req = req_template.clone();

        let start = Instant::now();
        rate_counter.fetch_add(1, Ordering::Relaxed);

        let (status, body_bytes_recv) = match h3_request_response(&mut send_request, req).await {
            Ok(v) => v,
            Err(H3ExchangeError::Send) => {
                counters.errors_write.fetch_add(1, Ordering::Relaxed);
                counters.complete.fetch_add(local_complete, Ordering::Relaxed);
                return; // connection is broken; the connection task will reconnect
            }
            Err(H3ExchangeError::Recv) => {
                counters.errors_read.fetch_add(1, Ordering::Relaxed);
                counters.complete.fetch_add(local_complete, Ordering::Relaxed);
                return;
            }
        };

        let elapsed = start.elapsed();
        let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        local_complete += 1;
        if status > 399 {
            counters.errors_status.fetch_add(1, Ordering::Relaxed);
        }
        if elapsed > timeout_dur {
            counters.errors_timeout.fetch_add(1, Ordering::Relaxed);
            continue; // a stream timeout need not kill the QUIC connection
        }
        if !latency.record(elapsed_us) {
            counters.errors_timeout.fetch_add(1, Ordering::Relaxed);
        }
        // For HTTP/3 we count response body bytes (QUIC framing/QPACK headers
        // are not exposed by h3). This is a faithful "transfer" measurement of
        // the application payload.
        counters.bytes.fetch_add(body_bytes_recv, Ordering::Relaxed);
    }
    counters.complete.fetch_add(local_complete, Ordering::Relaxed);
}

enum H3ExchangeError {
    Send,
    Recv,
}

/// Send one request and drain its response. Returns (status, body bytes).
async fn h3_request_response<B>(
    send_request: &mut SendRequest<B, Bytes>,
    req: http::Request<()>,
) -> Result<(u16, u64), H3ExchangeError>
where
    B: OpenStreams<Bytes> + Clone + Send + Sync + 'static,
{
    let mut stream =
        send_request.send_request(req).await.map_err(|_| H3ExchangeError::Send)?;
    stream.finish().await.map_err(|_| H3ExchangeError::Send)?;

    let resp = stream.recv_response().await.map_err(|_| H3ExchangeError::Recv)?;
    let status = resp.status().as_u16();

    // Drain the body frame by frame (zero-alloc), tallying bytes.
    let mut total = 0u64;
    while let Some(chunk) = stream.recv_data().await.map_err(|_| H3ExchangeError::Recv)? {
        total += chunk.chunk().len() as u64;
    }
    Ok((status, total))
}

fn build_h3_request(
    method: &http::Method,
    full_uri: &http::Uri,
    headers: &http::HeaderMap,
    _body: &Bytes,
) -> http::Request<()> {
    // h3 derives :method/:path/:scheme/:authority from the request's method
    // and URI (which must carry scheme+authority). We only add the regular
    // (non-pseudo) headers from the shared HeaderMap.
    let mut builder = http::Request::builder().method(method.clone()).uri(full_uri.clone());
    for (name, val) in headers.iter() {
        builder = builder.header(name.clone(), val.clone());
    }
    builder.body(()).expect("valid h3 request")
}

// (No further suppressed-import helpers needed.)
