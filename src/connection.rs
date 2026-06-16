// Per-connection benchmark loop.
//
// Each connection is a tokio task that:
//   1. Opens a TCP socket (TCP_NODELAY); for https wraps with rustls (SNI=host).
//   2. Runs an HTTP/1.1 handshake to obtain a per-connection SendRequest.
//   3. Loops: send request -> await response -> record latency/bytes/errors ->
//      (on error) reconnect.
//
// This mirrors wrk's connect_socket / socket_writeable / socket_readable /
// response_complete / reconnect_socket flow, but expressed as async/await.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use http_body_util::Full;
use hyper::body::{Body, Frame};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::config::{Config, Scheme};

/// Counters accumulated by a single worker's connections. Each worker owns its
/// own instance (passed by `Arc` to that worker's connection tasks); main sums
/// them after join. Keeping these per-worker — instead of one global
/// `Arc<Counters>` hit by every connection on every core — removes the
/// contended-atomic cacheline bounce that was the dominant per-request cost at
/// high `-t`. On a single core an uncontended relaxed `fetch_add` is ~1ns, so
/// sharing within a worker costs nothing; only *cross-core* sharing is
/// expensive, and we no longer do that.
#[derive(Default)]
pub struct Counters {
    pub complete: AtomicU64,
    pub bytes: AtomicU64,
    pub errors_connect: AtomicU64,
    pub errors_read: AtomicU64,
    pub errors_write: AtomicU64,
    pub errors_status: AtomicU64,
    pub errors_timeout: AtomicU64,
}

/// A point-in-time snapshot of a worker's counters, taken once the worker
/// finishes. Cheap to sum across workers (plain integer adds, no atomics).
#[derive(Default, Clone, Copy)]
pub struct CounterSnapshot {
    pub complete: u64,
    pub bytes: u64,
    pub errors_connect: u64,
    pub errors_read: u64,
    pub errors_write: u64,
    pub errors_status: u64,
    pub errors_timeout: u64,
}

impl CounterSnapshot {
    /// Sum a slice of per-worker snapshots into one aggregate.
    pub fn sum(snapshots: &[CounterSnapshot]) -> CounterSnapshot {
        let mut t = CounterSnapshot::default();
        for s in snapshots {
            t.complete += s.complete;
            t.bytes += s.bytes;
            t.errors_connect += s.errors_connect;
            t.errors_read += s.errors_read;
            t.errors_write += s.errors_write;
            t.errors_status += s.errors_status;
            t.errors_timeout += s.errors_timeout;
        }
        t
    }
}

impl Counters {
    /// Relaxed load every field once. Called once per worker at join time.
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            complete: self.complete.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            errors_connect: self.errors_connect.load(Ordering::Relaxed),
            errors_read: self.errors_read.load(Ordering::Relaxed),
            errors_write: self.errors_write.load(Ordering::Relaxed),
            errors_status: self.errors_status.load(Ordering::Relaxed),
            errors_timeout: self.errors_timeout.load(Ordering::Relaxed),
        }
    }
}

/// A transport: either a plain TCP stream or a TLS-over-TCP stream.
/// The TLS variant is inherently much larger than the plain variant; this is
/// fundamental (a TLS session carries cipher state), so we allow the
/// `large_enum_variant` lint here.
#[allow(clippy::large_enum_variant)]
pub enum Transport {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl tokio::io::AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Safety: we project to the inner stream without moving it.
        unsafe {
            match self.get_unchecked_mut() {
                Transport::Plain(s) => Pin::new_unchecked(s).poll_read(cx, buf),
                Transport::Tls(s) => Pin::new_unchecked(s).poll_read(cx, buf),
            }
        }
    }
}

impl tokio::io::AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        unsafe {
            match self.get_unchecked_mut() {
                Transport::Plain(s) => Pin::new_unchecked(s).poll_write(cx, buf),
                Transport::Tls(s) => Pin::new_unchecked(s).poll_write(cx, buf),
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        unsafe {
            match self.get_unchecked_mut() {
                Transport::Plain(s) => Pin::new_unchecked(s).poll_flush(cx),
                Transport::Tls(s) => Pin::new_unchecked(s).poll_flush(cx),
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        unsafe {
            match self.get_unchecked_mut() {
                Transport::Plain(s) => Pin::new_unchecked(s).poll_shutdown(cx),
                Transport::Tls(s) => Pin::new_unchecked(s).poll_shutdown(cx),
            }
        }
    }
}

/// A Future that drains a body frame-by-frame, dropping each frame without
/// collecting — zero allocation per response. Returns the total body bytes
/// drained, so callers can fold it into the byte counter without a separate
/// CountingStream wrapper layer. Registers the waker correctly so it never
/// busy-loops. Shared by the HTTP/1 and HTTP/2 paths.
pub(crate) struct DrainBody<B> {
    body: B,
    bytes: u64,
}

impl<B: Body + Unpin> Future for DrainBody<B> {
    type Output = Result<u64, B::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match Pin::new(&mut this.body).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    // Tally data-frame bytes; ignore trailers.
                    if let Some(data) = frame.data_ref() {
                        this.bytes += data.remaining() as u64;
                    }
                    continue;
                }
                Poll::Ready(None) => return Poll::Ready(Ok(this.bytes)),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Construct a draining future over a body. `pub(crate)` so the H2 path reuses
/// the same zero-alloc drain instead of `BodyExt::collect` (which buffers the
/// entire response body into a `Bytes` and then drops it). Returns the total
/// body bytes drained on success.
pub(crate) fn drain_body<B: Body + Unpin>(body: B) -> DrainBody<B> {
    DrainBody { body, bytes: 0 }
}

#[allow(dead_code)]
fn _frame_type_assert(_: &Frame<Bytes>) {}

/// Run one connection's benchmark loop until `stop` is set.
/// `rate_counter` is the per-worker counter sampled every 100ms by the rate
/// recorder in worker.rs.
pub async fn run_connection(
    cfg: Arc<Config>,
    counters: Arc<Counters>,
    latency: Arc<crate::stats::Stats>,
    stop: Arc<AtomicBool>,
    tls: Option<TlsConnector>,
    server_name: Option<rustls::pki_types::ServerName<'static>>,
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

    // Pre-parse the static header names/values once per connection so the
    // per-request build path avoids re-parsing strings into HeaderName/
    // HeaderValue. (Mirrors wrk's !cfg.dynamic path, which renders the
    // request a single time.)
    //
    // NOTE: we deliberately do NOT cache a full `http::Request` template and
    // clone it per request. `Request::clone` deep-copies the `HeaderMap`
    // (its indices `Box<[Pos]>` + entries `Vec` + extras `Vec` = 3+ heap
    // allocs per request). Rebuilding the Request each time with a single
    // pre-sized `HeaderMap::with_capacity` allocation is cheaper, and the
    // `HeaderName`/`HeaderValue` clones are just Bytes refcount incs.
    let mut static_headers: Vec<(http::HeaderName, http::HeaderValue)> =
        Vec::with_capacity(cfg.headers.len() + 2);
    static_headers.push((
        http::HeaderName::from_static("host"),
        http::HeaderValue::from_str(&host_header).unwrap(),
    ));
    static_headers.push((
        http::HeaderName::from_static("user-agent"),
        http::HeaderValue::from_static("wrk-rs/0.1.0"),
    ));
    for (k, v) in &cfg.headers {
        if let (Ok(name), Ok(val)) = (
            http::HeaderName::from_bytes(k.as_bytes()),
            http::HeaderValue::from_str(v),
        ) {
            static_headers.push((name, val));
        }
    }
    let body_len = body_bytes.len();
    let n_headers = static_headers.len() + (body_len > 0) as usize;

    while !stop.load(Ordering::Relaxed) {
        // ---- connect ----
        let transport = match connect(&cfg, tls.clone(), server_name.clone()).await {
            Ok(t) => t,
            Err(_) => {
                counters.errors_connect.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        // Use the handshake Builder to tune buffer behaviour for our workload:
        // tiny, fast responses over keep-alive TCP. writev(false) flattens to
        // a single write() (no iovec setup) which is faster for single-buffer
        // GET requests; read_buf_exact_size(8192) avoids adaptive-buffer cost.
        let (mut sender, conn) = match http1::Builder::new()
            .writev(false)
            .read_buf_exact_size(Some(8192))
            .handshake(TokioIo::new(transport))
            .await
        {
            Ok(v) => v,
            Err(_) => {
                counters.errors_connect.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        // Drive the connection in the background until it closes/errors.
        // spawn_local (not tokio::spawn) keeps the driver on the same LocalSet
        // queue as the request loop, avoiding a scheduler-path mismatch on
        // every cross-wakeup between send_request and conn.
        tokio::task::spawn_local(async move {
            let _ = conn.await;
        });

        // ---- request/response loop on this connection ----
        //
        // Hot-path design notes:
        //  - No per-request `tokio::time::timeout` wrapper. The Sleep future
        //    it allocates/registers/deregisters is the single biggest per-
        //    request cost on fast servers. Instead we stamp start time and
        //    only treat a request as timed-out if it actually overshoots the
        //    deadline (rare on a healthy server).
        //  - Request is rebuilt each iteration with a pre-sized HeaderMap
        //    (1 alloc) rather than cloning a template (3+ allocs).
        //  - `Instant::now()` is taken once per request (not twice).
        //  - Per-request counters (complete, bytes) are accumulated in local
        //    `u64`s (plain integer adds, ~0 cost) and flushed to the shared
        //    `Arc<Counters>` only on reconnect/exit — mirroring wrk's
        //    `thread->complete++`. rate_counter stays per-request because the
        //    100ms rate recorder must sample it for the Req/Sec histogram.
        let mut local_complete: u64 = 0;
        loop {
            if stop.load(Ordering::Relaxed) {
                // Flush local accumulators before exiting.
                counters.complete.fetch_add(local_complete, Ordering::Relaxed);
                return;
            }

            let req = build_request(&method, &cfg.path, &static_headers, n_headers, body_bytes.clone());

            let start = Instant::now();
            rate_counter.fetch_add(1, Ordering::Relaxed);

            let resp = match sender.send_request(req).await {
                Ok(r) => r,
                Err(_) => {
                    counters.errors_write.fetch_add(1, Ordering::Relaxed);
                    // Flush before reconnecting so these aren't lost.
                    counters.complete.fetch_add(local_complete, Ordering::Relaxed);
                    break;
                }
            };
            let status = resp.status().as_u16();
            let resp_headers = resp.headers().clone();
            // Zero-alloc drain: drop frames one at a time. Bytes are counted
            // here (not by CountingStream, which we removed) by estimating
            // header + body bytes once the response is fully read.
            let mut drain = drain_body(resp.into_body());
            let body_bytes_recv = match (&mut drain).await {
                Ok(n) => n,
                Err(_) => {
                    counters.errors_read.fetch_add(1, Ordering::Relaxed);
                    counters.complete.fetch_add(local_complete, Ordering::Relaxed);
                    break;
                }
            };
            // Estimate total response bytes: status line + headers + body.
            // This approximates the on-wire byte count (the original wrk counts
            // raw socket bytes including headers). For tiny responses the
            // header portion dominates, so this keeps Transfer/sec faithful.
            let header_bytes = estimate_response_bytes(status, &resp_headers);
            counters.bytes.fetch_add(header_bytes + body_bytes_recv, Ordering::Relaxed);

            let elapsed = start.elapsed();
            let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;

            local_complete += 1;
            if status > 399 {
                counters.errors_status.fetch_add(1, Ordering::Relaxed);
            }
            if elapsed > timeout_dur {
                // Request exceeded the timeout — count it and reconnect.
                counters.errors_timeout.fetch_add(1, Ordering::Relaxed);
                counters.complete.fetch_add(local_complete, Ordering::Relaxed);
                break;
            }
            if !latency.record(elapsed_us) {
                counters.errors_timeout.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let _ = body_len;
}

/// Estimate the on-wire byte count of an HTTP response's status line + headers.
/// Approximates what a raw socket byte counter would see, so Transfer/sec
/// stays comparable to the original wrk (which counts all socket bytes).
/// Format: "HTTP/1.1 200 OK\r\n" + "Name: Value\r\n" per header + "\r\n".
pub(crate) fn estimate_response_bytes(status: u16, headers: &http::HeaderMap) -> u64 {
    // Status line: "HTTP/1.1 " (8) + status digits (3) + " " (1) + reason +
    // "\r\n" (2). Reason phrases are short; approximate as ~16 bytes.
    let mut n: u64 = 16;
    for (name, val) in headers {
        // "Name: Value\r\n"
        n += name.as_str().len() as u64 + 2 + val.len() as u64 + 2;
    }
    n += 2; // final "\r\n"
    let _ = status;
    n
}

fn build_request(
    method: &http::Method,
    path: &str,
    headers: &[(http::HeaderName, http::HeaderValue)],
    n_headers: usize,
    body: Bytes,
) -> http::Request<Full<Bytes>> {
    let body_len = body.len();
    let mut req = http::Request::builder()
        .method(method.clone())
        .uri(path)
        .body(Full::new(body))
        .expect("valid request");
    let map = req.headers_mut();
    // Pre-size to avoid reallocation: the map allocates one Vec of the right
    // capacity rather than the 3+ allocs that Request::clone() incurs.
    map.reserve(n_headers);
    for (name, val) in headers {
        map.append(name.clone(), val.clone());
    }
    if body_len > 0 {
        map.insert(
            http::header::CONTENT_LENGTH,
            http::HeaderValue::from(body_len),
        );
    }
    req
}

async fn connect(
    cfg: &Config,
    tls: Option<TlsConnector>,
    server_name: Option<rustls::pki_types::ServerName<'static>>,
) -> std::io::Result<Transport> {
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let tcp = TcpStream::connect(addr).await?;
    let _ = tcp.set_nodelay(true);
    // On Linux, TCP_QUICKACK reduces ACK latency for small request/response
    // patterns. It's a hint the kernel may reset, but sticks for bursts.
    #[cfg(target_os = "linux")]
    let _ = tcp.set_quickack(true);

    match cfg.scheme {
        Scheme::Http => Ok(Transport::Plain(tcp)),
        Scheme::Https => {
            let tls = tls.ok_or_else(|| std::io::Error::other("tls connector missing"))?;
            let sni = server_name.ok_or_else(|| std::io::Error::other("server name missing"))?;
            let tls_stream = tls
                .connect(sni, tcp)
                .await
                .map_err(std::io::Error::other)?;
            Ok(Transport::Tls(tls_stream))
        }
    }
}
