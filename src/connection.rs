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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::config::{Config, Scheme};
use crate::counting::CountingStream;

/// Counters shared across a worker's connections (and aggregated globally).
#[derive(Default)]
pub struct Counters {
    pub complete: AtomicU64,
    pub requests: AtomicU64, // requests since last rate-record tick
    pub bytes: AtomicU64,
    pub errors_connect: AtomicU64,
    pub errors_read: AtomicU64,
    pub errors_write: AtomicU64,
    pub errors_status: AtomicU64,
    pub errors_timeout: AtomicU64,
}

/// A transport: either a plain TCP stream or a TLS-over-TCP stream.
pub enum Transport {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl tokio::io::AsyncRead for Transport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Safety: we project to the inner stream without moving it.
        unsafe {
            match self.get_unchecked_mut() {
                Transport::Plain(s) => std::pin::Pin::new_unchecked(s).poll_read(cx, buf),
                Transport::Tls(s) => std::pin::Pin::new_unchecked(s).poll_read(cx, buf),
            }
        }
    }
}

impl tokio::io::AsyncWrite for Transport {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        unsafe {
            match self.get_unchecked_mut() {
                Transport::Plain(s) => std::pin::Pin::new_unchecked(s).poll_write(cx, buf),
                Transport::Tls(s) => std::pin::Pin::new_unchecked(s).poll_write(cx, buf),
            }
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        unsafe {
            match self.get_unchecked_mut() {
                Transport::Plain(s) => std::pin::Pin::new_unchecked(s).poll_flush(cx),
                Transport::Tls(s) => std::pin::Pin::new_unchecked(s).poll_flush(cx),
            }
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        unsafe {
            match self.get_unchecked_mut() {
                Transport::Plain(s) => std::pin::Pin::new_unchecked(s).poll_shutdown(cx),
                Transport::Tls(s) => std::pin::Pin::new_unchecked(s).poll_shutdown(cx),
            }
        }
    }
}

/// Run one connection's benchmark loop until `stop` is set.
pub async fn run_connection(
    cfg: Arc<Config>,
    counters: Arc<Counters>,
    latency: Arc<crate::stats::Stats>,
    stop: Arc<AtomicBool>,
    tls: Option<TlsConnector>,
    server_name: Option<rustls::pki_types::ServerName<'static>>,
) {
    let body_bytes: Bytes = if cfg.body.is_empty() {
        Bytes::new()
    } else {
        Bytes::copy_from_slice(&cfg.body)
    };
    let method: http::Method = cfg.method.parse().unwrap_or(http::Method::GET);
    let host_header = cfg.host_header();
    let timeout_dur = Duration::from_millis(cfg.timeout);

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

        let (mut sender, conn) = match http1::handshake(counting).await {
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

            let req = build_request(
                &method,
                &cfg.path,
                &host_header,
                &cfg.headers,
                body_bytes.clone(),
            );

            let start = Instant::now();
            counters.requests.fetch_add(1, Ordering::Relaxed);

            let resp = match tokio::time::timeout(timeout_dur, sender.send_request(req)).await {
                Ok(Ok(r)) => r,
                Ok(Err(_e)) => {
                    // Write/send error — reconnect.
                    counters.errors_write.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(_) => {
                    counters.errors_timeout.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            };

            let status = resp.status().as_u16();

            // Drain the body so the connection is reusable; count via bytes_read.
            let collected =
                match tokio::time::timeout(timeout_dur, resp.into_body().collect()).await {
                    Ok(Ok(c)) => c,
                    Ok(Err(_)) => {
                        counters.errors_read.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Err(_) => {
                        counters.errors_timeout.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                };
            let _ = collected.to_bytes();

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

            // If the sender is no longer ready (peer closed), reconnect.
            if !sender.is_ready() {
                if sender.ready().await.is_err() {
                    break;
                }
            }
        }
    }
}

fn build_request(
    method: &http::Method,
    path: &str,
    host: &str,
    headers: &[(String, String)],
    body: Bytes,
) -> http::Request<Full<Bytes>> {
    let mut builder = http::Request::builder().method(method.clone()).uri(path);
    builder = builder.header("Host", host);
    builder = builder.header("User-Agent", "wrk-rs/0.1.0");
    for (k, v) in headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    if !body.is_empty() {
        builder = builder.header("Content-Length", body.len());
    }
    builder.body(Full::new(body)).expect("valid request")
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
            let tls = tls.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::Other, "tls connector missing")
            })?;
            let sni = server_name.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::Other, "server name missing")
            })?;
            let tls_stream = tls
                .connect(sni, tcp)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            Ok(Transport::Tls(tls_stream))
        }
    }
}
