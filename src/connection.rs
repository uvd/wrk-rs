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

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::{Body, Frame};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::config::{Config, Scheme};
use crate::counting::CountingStream;

/// Counters shared across a worker's connections (and aggregated globally).
/// Note: the per-100ms rate counter is kept per-worker in worker.rs, not here
/// (see the comment in worker.rs for why).
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
/// collecting — zero allocation per response. Bytes are already counted
/// upstream by CountingStream. Registers the waker correctly so it never
/// busy-loops.
struct DrainBody<B> {
    body: B,
}

impl<B: Body + Unpin> Future for DrainBody<B> {
    type Output = Result<(), B::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match Pin::new(&mut this.body).poll_frame(cx) {
                Poll::Ready(Some(Ok(_frame))) => continue,
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
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
        let bytes_read = Arc::new(AtomicU64::new(0));
        let counting = TokioIo::new(CountingStream::new(transport, bytes_read.clone()));

        // Use the handshake Builder to tune buffer behaviour for our workload:
        // tiny, fast responses over keep-alive TCP. A fixed small read buffer
        // avoids the adaptive-buffer bookkeeping, and keeping writev on auto.
        let (mut sender, conn) = match http1::Builder::new()
            .read_buf_exact_size(Some(8192))
            .handshake(counting)
            .await
        {
            Ok(v) => v,
            Err(_) => {
                counters.errors_connect.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        // Drive the connection in the background until it closes/errors.
        tokio::spawn(async move {
            let _ = conn.await;
        });

        // ---- request/response loop on this connection ----
        loop {
            if stop.load(Ordering::Relaxed) {
                return;
            }

            let req = build_request(&method, &cfg.path, &static_headers, body_bytes.clone());

            let start = Instant::now();
            rate_counter.fetch_add(1, Ordering::Relaxed);

            // One timeout bounds the whole request+response+drain.
            let outcome =
                tokio::time::timeout(timeout_dur, request_response(&mut sender, req)).await;

            let status = match outcome {
                Ok(Ok(status)) => status,
                Ok(Err(())) => {
                    counters.errors_write.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(_) => {
                    counters.errors_timeout.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            };

            let elapsed_us = start.elapsed().as_micros().min(u64::MAX as u128) as u64;
            counters.complete.fetch_add(1, Ordering::Relaxed);
            if status > 399 {
                counters.errors_status.fetch_add(1, Ordering::Relaxed);
            }
            if !latency.record(elapsed_us) {
                counters.errors_timeout.fetch_add(1, Ordering::Relaxed);
            }
            let b = bytes_read.swap(0, Ordering::Relaxed);
            counters.bytes.fetch_add(b, Ordering::Relaxed);

            // Loop back to send_request; send_request internally awaits
            // readiness if the connection isn't ready, so we don't need a
            // separate ready() probe (which would add an extra await per
            // request). A failed send on the next iteration is caught by the
            // error branch above and triggers a reconnect.
        }
    }

    // Reference unused vars to keep them in scope for clarity.
    let _ = body_len;
}

/// Send one request and drain its response body without allocating (bytes are
/// already counted by CountingStream). Returns the HTTP status code.
async fn request_response(
    sender: &mut http1::SendRequest<Full<Bytes>>,
    req: http::Request<Full<Bytes>>,
) -> Result<u16, ()> {
    let resp = sender.send_request(req).await.map_err(|_| ())?;
    let status = resp.status().as_u16();
    // Zero-alloc drain: drop frames one at a time; bytes already counted.
    DrainBody {
        body: resp.into_body(),
    }
    .await
    .map_err(|_| ())?;
    Ok(status)
}

fn build_request(
    method: &http::Method,
    path: &str,
    headers: &[(http::HeaderName, http::HeaderValue)],
    body: Bytes,
) -> http::Request<Full<Bytes>> {
    let body_len = body.len();
    let mut req = http::Request::builder()
        .method(method.clone())
        .uri(path)
        .body(Full::new(body))
        .expect("valid request");
    let map = req.headers_mut();
    map.reserve(headers.len() + 1);
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
