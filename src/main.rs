// wrk-rs main entry point.
//
// Mirrors the high-level flow of wrk's main() in wrk.c:
//   1. Parse args + URL.
//   2. Allocate global latency (timeout_ms*1000 buckets) and requests (10M)
//      histograms.
//   3. Spawn one OS thread per -t, each running a worker.
//   4. Sleep for duration, set stop, join all workers.
//   5. Aggregate counters, run coordinated-omission correction, print report.

mod config;
mod connection;
mod counting;
mod h2;
mod h3;
mod report;
mod stats;
mod units;
mod worker;

use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use config::Config;
use connection::Counters;
use stats::Stats;

pub const MAX_THREAD_RATE_S: u64 = 10_000_000;

fn main() {
    let mut cfg = config::parse_args();

    // Resolve the protocol. For Auto + https, probe whether the server speaks
    // HTTP/3 (QUIC); otherwise fall back to HTTP/1.1. For Auto + http, always
    // HTTP/1.1. For an explicit --http3/--http http3, H3 is required.
    let resolved = resolve_protocol(&cfg);
    cfg.resolved = Some(resolved);
    let cfg = Arc::new(cfg);

    // Allocate global histograms.
    let latency_max = cfg.timeout.saturating_mul(1000);
    let latency = Arc::new(Stats::alloc(latency_max));
    let requests_stats = Arc::new(Stats::alloc(MAX_THREAD_RATE_S));

    // Global shared counters (summed across all workers).
    let counters = Arc::new(Counters::default());
    let stop = Arc::new(AtomicBool::new(false));

    // Print the run header (mirrors wrk.c:137-139), annotated with the
    // resolved protocol and (for H3) the per-connection stream count.
    let time = units::format_time_s(cfg.duration as f64);
    print_run_header(&cfg, &time);

    // Distribute connections across threads (integer division, like wrk.c:107).
    let per_thread = cfg.connections / cfg.threads;

    let mut handles = Vec::with_capacity(cfg.threads as usize);
    match resolved {
        config::ResolvedVersion::Http3 => {
            // Quinn endpoints must be created inside a tokio runtime, and that
            // runtime must outlive the endpoint (quinn spawns IO tasks on it).
            // We create a persistent multi_thread runtime that lives for the
            // whole run and leak it — fine for a short-lived CLI tool.
            let shared_rt = Box::leak(Box::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap_or_else(|e| {
                        eprintln!("unable to create QUIC runtime: {e}");
                        exit(1);
                    }),
            ));
            let endpoint = Arc::new(
                shared_rt
                    .block_on(async { h3::build_endpoint(cfg.insecure) })
                    .unwrap_or_else(|e| {
                        eprintln!("unable to create QUIC endpoint: {e}");
                        exit(1);
                    }),
            );
            for _ in 0..cfg.threads {
                let cfg = cfg.clone();
                let latency = latency.clone();
                let requests_stats = requests_stats.clone();
                let counters = counters.clone();
                let stop = stop.clone();
                let endpoint = endpoint.clone();
                handles.push(std::thread::spawn(move || {
                    worker::run_h3_worker(
                        cfg,
                        per_thread,
                        latency,
                        requests_stats,
                        counters,
                        stop,
                        endpoint,
                    );
                }));
            }
        }
        config::ResolvedVersion::Http2 => {
            // Build a TLS connector + server name (HTTP/2 runs over TLS here).
            let (tls, server_name) = build_tls(&cfg, resolved);
            let tls = tls.expect("HTTP/2 requires TLS");
            let server_name = server_name.expect("HTTP/2 requires a server name");
            for _ in 0..cfg.threads {
                let cfg = cfg.clone();
                let latency = latency.clone();
                let requests_stats = requests_stats.clone();
                let counters = counters.clone();
                let stop = stop.clone();
                let tls = tls.clone();
                let server_name = server_name.clone();
                handles.push(std::thread::spawn(move || {
                    worker::run_h2_worker(
                        cfg,
                        per_thread,
                        latency,
                        requests_stats,
                        counters,
                        stop,
                        tls,
                        server_name,
                    );
                }));
            }
        }
        config::ResolvedVersion::Http1 => {
            // Build a TLS connector + server name once, if needed.
            let (tls, server_name) = build_tls(&cfg, resolved);
            for _ in 0..cfg.threads {
                let cfg = cfg.clone();
                let latency = latency.clone();
                let requests_stats = requests_stats.clone();
                let counters = counters.clone();
                let stop = stop.clone();
                let tls = tls.clone();
                let server_name = server_name.clone();
                handles.push(std::thread::spawn(move || {
                    worker::run_worker(
                        cfg,
                        per_thread,
                        latency,
                        requests_stats,
                        counters,
                        stop,
                        tls,
                        server_name,
                    );
                }));
            }
        }
    }

    let wall_start = Instant::now();
    std::thread::sleep(std::time::Duration::from_secs(cfg.duration));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        let _ = h.join();
    }

    let runtime_us = wall_start.elapsed().as_micros() as u64;
    let complete = counters.complete.load(Ordering::Relaxed);

    // Coordinated-omission correction (mirrors wrk.c:168-171):
    // expected = runtime_us / (complete / connections)
    if let Some(per_conn) = complete.checked_div(cfg.connections)
        && let Some(interval) = runtime_us.checked_div(per_conn)
    {
        latency.correct(interval as i64);
    }

    let r = report::Report {
        complete,
        bytes: counters.bytes.load(Ordering::Relaxed),
        runtime_us,
        protocol: resolved,
        errors_connect: counters.errors_connect.load(Ordering::Relaxed),
        errors_read: counters.errors_read.load(Ordering::Relaxed),
        errors_write: counters.errors_write.load(Ordering::Relaxed),
        errors_timeout: counters.errors_timeout.load(Ordering::Relaxed),
        errors_status: counters.errors_status.load(Ordering::Relaxed),
        latency: &latency,
        requests: &requests_stats,
        print_latency: cfg.latency,
    };
    report::print_report(&r);

    let _ = exit;
}

/// Resolve the effective protocol version. For Auto+https, probe in priority
/// order: HTTP/3 (QUIC) → HTTP/2 (TLS ALPN) → HTTP/1.1.
fn resolve_protocol(cfg: &config::Config) -> config::ResolvedVersion {
    use config::{HttpVersion, ResolvedVersion, Scheme};
    match cfg.http_version {
        HttpVersion::Http1 => ResolvedVersion::Http1,
        HttpVersion::Http2 => ResolvedVersion::Http2,
        HttpVersion::Http3 => ResolvedVersion::Http3,
        HttpVersion::Auto => {
            if cfg.scheme != Scheme::Https {
                return ResolvedVersion::Http1;
            }
            // 1. Try HTTP/3 (QUIC).
            if probe_quic(cfg) {
                eprintln!("Negotiated HTTP/3 (QUIC)");
                return ResolvedVersion::Http3;
            }
            // 2. Try HTTP/2 via TLS ALPN.
            match probe_alpn(cfg) {
                Some("h2") => {
                    eprintln!("Negotiated HTTP/2");
                    ResolvedVersion::Http2
                }
                _ => {
                    eprintln!("Falling back to HTTP/1.1");
                    ResolvedVersion::Http1
                }
            }
        }
    }
}

/// Attempt a QUIC handshake to the target. Used for auto-negotiation.
fn probe_quic(cfg: &config::Config) -> bool {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(_) => return false,
    };
    runtime.block_on(async {
        let endpoint = match h3::build_endpoint(cfg.insecure) {
            Ok(e) => e,
            Err(_) => return false,
        };
        // Resolve host:port to a SocketAddr.
        let addr = match tokio::net::lookup_host((cfg.host.as_str(), cfg.port)).await {
            Ok(mut it) => match it.next() {
                Some(a) => a,
                None => return false,
            },
            Err(_) => return false,
        };
        // Try to connect with a 1s budget.
        let connecting = match endpoint.connect(addr, &cfg.host) {
            Ok(c) => c,
            Err(_) => return false,
        };
        matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), connecting).await,
            Ok(Ok(_conn))
        )
    })
}

/// Attempt a TLS handshake to the target, advertising both h2 and http/1.1,
/// and return the negotiated ALPN protocol. Used for auto-negotiation.
fn probe_alpn(cfg: &config::Config) -> Option<&'static str> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    runtime.block_on(async {
        // Build a TLS config that advertises h2 + http/1.1.
        let mut roots = rustls::RootCertStore::empty();
        if cfg.insecure {
            // For --insecure, use the no-verifier path; ALPN still works.
        } else {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        let mut client_crypto = if cfg.insecure {
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth()
        } else {
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        };
        client_crypto.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_crypto));
        let tcp = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::net::TcpStream::connect((cfg.host.as_str(), cfg.port)),
        )
        .await
        .ok()?
        .ok()?;
        let server_name = rustls::pki_types::ServerName::try_from(cfg.host.clone()).ok()?;
        let tls = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            connector.connect(server_name, tcp),
        )
        .await
        .ok()?
        .ok()?;
        let alpn = tls.get_ref().1.alpn_protocol();
        match alpn {
            Some(b"h2") => Some("h2"),
            Some(b"http/1.1") => Some("http/1.1"),
            _ => None,
        }
    })
}

/// Print the "Running ..." header, annotated with the resolved protocol.
fn print_run_header(cfg: &config::Config, time: &str) {
    let proto = cfg
        .resolved
        .map(|r| r.as_str())
        .unwrap_or("HTTP/1.1");
    let streams = if matches!(
        cfg.resolved,
        Some(config::ResolvedVersion::Http3) | Some(config::ResolvedVersion::Http2)
    ) {
        format!(", {} streams/conn", cfg.streams_per_conn)
    } else {
        String::new()
    };
    println!("Running {} test @ {} [{}{}]", time, cfg.url, proto, streams);
    println!(
        "  {} threads and {} connections",
        cfg.threads, cfg.connections
    );
}

fn build_tls(
    cfg: &Config,
    resolved: config::ResolvedVersion,
) -> (
    Option<tokio_rustls::TlsConnector>,
    Option<rustls::pki_types::ServerName<'static>>,
) {
    if cfg.scheme != config::Scheme::Https {
        return (None, None);
    }
    // Install the ring crypto provider as the process default (rustls 0.23
    // requires an explicit CryptoProvider choice).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut config = if cfg.insecure {
        // Mirror wrk's SSL_VERIFY_NONE: accept any certificate.
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth()
    } else {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };
    // Advertise the matching ALPN so the server negotiates the right protocol.
    config.alpn_protocols = match resolved {
        config::ResolvedVersion::Http2 => vec![b"h2".to_vec()],
        config::ResolvedVersion::Http1 => vec![b"http/1.1".to_vec()],
        _ => vec![],
    };
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let server_name: rustls::pki_types::ServerName<'static> =
        match rustls::pki_types::ServerName::try_from(cfg.host.clone()) {
            Ok(n) => n,
            Err(_) => {
                eprintln!("invalid TLS server name: {}", cfg.host);
                exit(1);
            }
        };
    (Some(connector), Some(server_name.to_owned()))
}

/// A certificate verifier that accepts everything (used by --insecure).
/// Public so the HTTP/3 (h3.rs) path can share the same verifier.
#[derive(Debug)]
pub struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA1,
            rustls::SignatureScheme::ECDSA_SHA1_Legacy,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::ED25519,
        ]
    }
}
