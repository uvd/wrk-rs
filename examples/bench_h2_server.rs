// A minimal HTTP/2 (over TLS) server for benchmarking wrk-rs.
// Listens on 127.0.0.1:18444, serves "OK\n" for every request. Uses a
// self-signed certificate (rcgen) so you must run wrk-rs with --insecure.
//
//   cargo run --release --example bench_h2_server
//   ./target/release/wrk --http2 --insecure -t2 -c10 --streams 10 -d5s https://127.0.0.1:18444/

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use rcgen::KeyPair;
use tokio::net::TcpListener;

const BODY: &[u8] = b"OK\n";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Self-signed certificate for 127.0.0.1.
    let cert_params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()])?;
    let key_pair = KeyPair::generate()?;
    let cert = cert_params.self_signed(&key_pair)?;
    let cert_der = cert.der().clone();
    let key_der = key_pair.serialize_der();

    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert_der],
            rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
                key_der,
            )),
        )?;
    // Advertise HTTP/2 (and HTTP/1.1 for compatibility) via ALPN.
    server_crypto.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let tls_acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_crypto));

    let addr: SocketAddr = "127.0.0.1:18444".parse().unwrap();
    let listener = TcpListener::bind(addr).await?;
    eprintln!("bench_h2_server listening on https://{addr} (HTTP/2 + HTTP/1.1)");

    loop {
        let (stream, _) = listener.accept().await?;
        let tls_acceptor = tls_acceptor.clone();
        tokio::spawn(async move {
            let tls = match tls_acceptor.accept(stream).await {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("tls accept error: {e}");
                    return;
                }
            };
            let io = TokioIo::new(tls);
            // Check negotiated ALPN to pick HTTP/2 vs HTTP/1.1.
            let alpn = io.inner().get_ref().1.alpn_protocol();
            let is_h2 = matches!(alpn, Some(b"h2"));
            let svc = service_fn(|_req: Request<Incoming>| async move {
                Ok::<_, std::convert::Infallible>(
                    Response::builder()
                        .status(200)
                        .header("content-length", BODY.len())
                        .body(Full::new(Bytes::from_static(BODY)))
                        .unwrap(),
                )
            });
            if is_h2 {
                let _ = http2::Builder::new(TokioExecutor::new())
                    .timer(TokioTimer::new())
                    .serve_connection(io, svc)
                    .await;
            } else {
                // Fall back to HTTP/1.1 for clients that don't offer h2.
                let _ = hyper::server::conn::http1::Builder::new()
                    .timer(TokioTimer::new())
                    .keep_alive(true)
                    .serve_connection(io, svc)
                    .await;
            }
        });
    }
}
