// A minimal multi-threaded HTTP/3 (QUIC) server for benchmarking wrk-rs.
// Listens on 127.0.0.1:18443, serves "OK\n" for every request. Uses a
// self-signed certificate (rcgen) so you must run wrk-rs with --insecure.
//
//   cargo run --release --example bench_h3_server
//   ./target/release/wrk --http3 --insecure -t2 -c10 -d5s https://127.0.0.1:18443/

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use h3_quinn::Connection;
use quinn::Endpoint;
use rcgen::KeyPair;

const BODY: &[u8] = b"OK\n";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Self-signed certificate for 127.0.0.1.
    let cert_params =
        rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()])?;
    let key_pair = KeyPair::generate()?;
    let cert = cert_params.self_signed(&key_pair)?;
    let cert_der = cert.der().clone();
    let key_der = key_pair.serialize_der();

    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
        ))?;
    server_crypto.alpn_protocols = vec![b"h3".to_vec()];

    let quic_server_cfg =
        quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)?,
        ));
    let addr: SocketAddr = "127.0.0.1:18443".parse().unwrap();
    let endpoint = Endpoint::server(quic_server_cfg, addr)?;
    eprintln!("bench_h3_server listening on https://{addr} (HTTP/3)");

    while let Some(incoming) = endpoint.accept().await {
        let conn = incoming.await?;
        tokio::spawn(async move {
            if let Err(e) = handle_conn(conn).await {
                eprintln!("h3 conn error: {e}");
            }
        });
    }
    Ok(())
}

async fn handle_conn(conn: quinn::Connection) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let h3_conn = Connection::new(conn);
    let mut h3 = h3::server::Connection::<Connection, Bytes>::new(h3_conn).await?;
    loop {
        match h3.accept().await {
            Ok(Some(resolver)) => {
                let (_req, stream) = resolver.resolve_request().await?;
                tokio::spawn(async move {
                    if let Err(e) = handle_req(stream).await {
                        eprintln!("h3 req error: {e}");
                    }
                });
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("h3 accept error: {e}");
                break;
            }
        }
    }
    Ok(())
}

async fn handle_req<S>(
    mut req_stream: h3::server::RequestStream<S, Bytes>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: h3::quic::BidiStream<Bytes>,
{
    // Drain request body if any.
    while req_stream.recv_data().await?.is_some() {}
    let resp = http::Response::builder()
        .status(200)
        .header("content-length", BODY.len())
        .body(())?;
    req_stream.send_response(resp).await?;
    req_stream.send_data(Bytes::from_static(BODY)).await?;
    req_stream.finish().await?;
    Ok(())
}
