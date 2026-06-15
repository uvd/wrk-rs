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
    let cfg = config::parse_args();
    let cfg = Arc::new(cfg);

    // Allocate global histograms.
    let latency_max = cfg.timeout.saturating_mul(1000);
    let latency = Arc::new(Stats::alloc(latency_max));
    let requests_stats = Arc::new(Stats::alloc(MAX_THREAD_RATE_S));

    // Global shared counters (summed across all workers).
    let counters = Arc::new(Counters::default());
    let stop = Arc::new(AtomicBool::new(false));

    // Build a TLS connector + server name once, if needed.
    let (tls, server_name) = build_tls(&cfg);

    // Print the run header (mirrors wrk.c:137-139).
    let time = units::format_time_s(cfg.duration as f64);
    println!("Running {} test @ {}", time, cfg.url);
    println!(
        "  {} threads and {} connections",
        cfg.threads, cfg.connections
    );

    // Distribute connections across threads (integer division, like wrk.c:107).
    let per_thread = cfg.connections / cfg.threads;

    let mut handles = Vec::with_capacity(cfg.threads as usize);
    for _ in 0..cfg.threads {
        let cfg = cfg.clone();
        let latency = latency.clone();
        let requests_stats = requests_stats.clone();
        let counters = counters.clone();
        let stop = stop.clone();
        let tls = tls.clone();
        let server_name = server_name.clone();
        let handle = std::thread::spawn(move || {
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
        });
        handles.push(handle);
    }

    let wall_start = Instant::now();
    std::thread::sleep(std::time::Duration::from_secs(cfg.duration));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        let _ = h.join();
    }

    let runtime_us = wall_start.elapsed().as_micros() as u64;

    // Coordinated-omission correction (mirrors wrk.c:168-171).
    let complete = counters.complete.load(Ordering::Relaxed);
    let conns_per_thread = if cfg.threads > 0 {
        cfg.connections / cfg.threads
    } else {
        0
    };
    // expected = runtime_us / (complete / connections)
    if complete > 0 && cfg.connections > 0 {
        let per_conn = complete / cfg.connections;
        if per_conn > 0 {
            let interval = (runtime_us / per_conn) as i64;
            latency.correct(interval);
        }
    }
    let _ = conns_per_thread;

    let r = report::Report {
        complete,
        bytes: counters.bytes.load(Ordering::Relaxed),
        runtime_us,
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

fn build_tls(
    cfg: &Config,
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

    let config = if cfg.insecure {
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
#[derive(Debug)]
struct NoVerifier;

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
