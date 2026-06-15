// Command-line argument parsing and configuration for wrk-rs.
// Mirrors wrk's parse_args semantics, but with --script (-s) removed and
// --method/-X, --path, --body added to provide the request-shaping that Lua
// scripts previously did (pure-Rust implementation).
//
// Uses clap's derive API (#[derive(Parser)]) for the bulk of option handling,
// with two exceptions kept manual to match wrk exactly:
//   - -v/--version prints "wrk <ver> [async]" + the original copyright line
//     (not clap's default "name version" format).
//   - -h/--help and any parse error print wrk's own usage block and exit 1
//     (not clap's default exit-0 help).

use std::process::exit;

use clap::{ArgAction, Parser, ValueEnum};

pub const DEFAULT_THREADS: u64 = 2;
pub const DEFAULT_CONNECTIONS: u64 = 10;
pub const DEFAULT_DURATION_SECS: u64 = 10;
pub const SOCKET_TIMEOUT_MS: u64 = 2000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scheme {
    Http,
    Https,
}

/// Which HTTP protocol version to use.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum HttpVersion {
    /// HTTP/1.1 (over TCP, or over TLS for https).
    Http1,
    /// HTTP/3 over QUIC. Forces QUIC transport regardless of scheme.
    Http3,
    /// Negotiate: for https, try HTTP/3 and fall back to HTTP/1.1 on failure.
    /// For plain http, always uses HTTP/1.1.
    #[default]
    Auto,
}

/// The protocol actually used for the run (resolved after negotiation).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedVersion {
    Http1,
    Http3,
}

impl ResolvedVersion {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResolvedVersion::Http1 => "HTTP/1.1",
            ResolvedVersion::Http3 => "HTTP/3",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub threads: u64,
    pub connections: u64,
    pub duration: u64, // seconds
    pub timeout: u64,  // milliseconds
    pub latency: bool,
    pub insecure: bool, // skip TLS cert verification (wrk's default behaviour)
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub scheme: Scheme,
    pub host: String,
    pub port: u16,
    pub url: String,
    /// Requested protocol selection (default: Auto).
    pub http_version: HttpVersion,
    /// Concurrent streams per connection (HTTP/3 only; HTTP/1 is always 1).
    pub streams_per_conn: u64,
    /// Resolved after negotiation in main; the report prints this.
    pub resolved: Option<ResolvedVersion>,
}

impl Config {
    /// Default port for the chosen scheme.
    pub fn default_port(&self) -> u16 {
        match self.scheme {
            Scheme::Http => 80,
            Scheme::Https => 443,
        }
    }

    /// The Host header value (host[:port] when port is non-default).
    pub fn host_header(&self) -> String {
        if self.port == self.default_port() {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

// ---------------------------------------------------------------------------
// clap derive definition
// ---------------------------------------------------------------------------

/// A metric-prefixed integer (1, 1k, 1M, 1G). Parsed via scan_metric.
#[derive(Clone, Debug)]
pub struct MetricArg(pub u64);

impl std::str::FromStr for MetricArg {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::units::scan_metric(s)
            .map(MetricArg)
            .ok_or_else(|| format!("invalid SI value: '{s}' (expected e.g. 1, 1k, 1M, 1G)"))
    }
}

/// A time value in seconds (10, 2s, 2m, 2h). Parsed via scan_time.
#[derive(Clone, Debug)]
pub struct TimeArg(pub u64);

impl std::str::FromStr for TimeArg {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::units::scan_time(s)
            .map(TimeArg)
            .ok_or_else(|| format!("invalid time value: '{s}' (expected e.g. 2s, 2m, 2h)"))
    }
}

/// A single -H header, split on the first ':' into (name, value).
#[derive(Clone, Debug)]
pub struct HeaderArg(pub String, pub String);

impl std::str::FromStr for HeaderArg {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once(':') {
            Some((k, v)) => Ok(HeaderArg(k.trim().to_string(), v.trim().to_string())),
            None => Err(format!("invalid header: '{s}' (expected 'Name: Value')")),
        }
    }
}

/// HTTP method for -X. Accepts both uppercase (POST) and lowercase (post)
/// via aliases, since users naturally type method names in uppercase.
#[derive(Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum MethodArg {
    #[value(name = "GET", aliases = ["get"])]
    Get,
    #[value(name = "POST", aliases = ["post"])]
    Post,
    #[value(name = "PUT", aliases = ["put"])]
    Put,
    #[value(name = "PATCH", aliases = ["patch"])]
    Patch,
    #[value(name = "DELETE", aliases = ["delete"])]
    Delete,
    #[value(name = "HEAD", aliases = ["head"])]
    Head,
    #[value(name = "OPTIONS", aliases = ["options"])]
    Options,
}

impl MethodArg {
    pub fn as_str(&self) -> &'static str {
        match self {
            MethodArg::Get => "GET",
            MethodArg::Post => "POST",
            MethodArg::Put => "PUT",
            MethodArg::Patch => "PATCH",
            MethodArg::Delete => "DELETE",
            MethodArg::Head => "HEAD",
            MethodArg::Options => "OPTIONS",
        }
    }
}

/// CLI-facing HTTP version selector.
#[derive(Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum VersionArg {
    /// Negotiate: https tries HTTP/3 and falls back to HTTP/1.1; http is HTTP/1.1.
    Auto,
    /// HTTP/1.1.
    Http1,
    /// HTTP/3 over QUIC.
    Http3,
}

impl VersionArg {
    pub fn to_http_version(&self) -> HttpVersion {
        match self {
            VersionArg::Auto => HttpVersion::Auto,
            VersionArg::Http1 => HttpVersion::Http1,
            VersionArg::Http3 => HttpVersion::Http3,
        }
    }
}

/// HTTP benchmarking tool — a Rust reimplementation of wrk.
#[derive(Parser, Debug)]
#[command(
    name = "wrk",
    version,
    // We handle -v/-h ourselves to match wrk's exact output and exit codes.
    disable_help_flag = true,
    disable_version_flag = true,
)]
pub struct Cli {
    /// Connections to keep open
    #[arg(short = 'c', long = "connections", value_name = "N")]
    connections: Option<MetricArg>,

    /// Duration of test
    #[arg(short = 'd', long = "duration", value_name = "T")]
    duration: Option<TimeArg>,

    /// Number of threads to use
    #[arg(short = 't', long = "threads", value_name = "N")]
    threads: Option<MetricArg>,

    /// Add header to request (repeatable)
    #[arg(short = 'H', long = "header", value_name = "H")]
    header: Vec<HeaderArg>,

    /// Print latency statistics
    #[arg(short = 'L', long = "latency", action = ArgAction::SetTrue)]
    latency: bool,

    /// Skip TLS certificate verification
    #[arg(long = "insecure", action = ArgAction::SetTrue)]
    insecure: bool,

    /// Socket/request timeout
    #[arg(short = 'T', long = "timeout", value_name = "T")]
    timeout: Option<TimeArg>,

    /// HTTP method (default GET)
    #[arg(short = 'X', long = "method", value_name = "METHOD", default_value = "get")]
    method: MethodArg,

    /// Request path (default /)
    #[arg(long = "path", value_name = "P", default_value = "/")]
    path: String,

    /// Request body
    #[arg(short = 'b', long = "body", value_name = "BODY")]
    body: Option<String>,

    /// HTTP protocol version: auto (negotiate), http1, or http3
    #[arg(long = "http", value_name = "VER", default_value = "auto")]
    http_version: VersionArg,

    /// Force HTTP/3 over QUIC (shorthand for --http http3)
    #[arg(long = "http3", action = ArgAction::SetTrue)]
    http3: bool,

    /// Concurrent streams per connection (HTTP/3 only)
    #[arg(long = "streams", value_name = "N", default_value = "1")]
    streams_per_conn: MetricArg,

    /// Print version details
    #[arg(short = 'v', long = "version", action = ArgAction::SetTrue)]
    version: bool,

    /// Print usage
    #[arg(short = 'h', long = "help", action = ArgAction::SetTrue)]
    help: bool,

    /// Target URL
    url: Option<String>,
}

// ---------------------------------------------------------------------------
// Usage / version output (matches wrk exactly)
// ---------------------------------------------------------------------------

pub fn print_usage() {
    println!(
        "Usage: wrk <options> <url>                            \n\
         \x20 Options:                                            \n\
         \x20     -c, --connections <N>  Connections to keep open   \n\
         \x20     -d, --duration    <T>  Duration of test           \n\
         \x20     -t, --threads     <N>  Number of threads to use   \n\
         \x20                                                       \n\
         \x20     -H, --header      <H>  Add header to request      \n\
         \x20         --latency          Print latency statistics   \n\
         \x20         --timeout     <T>  Socket/request timeout     \n\
         \x20         --insecure         Skip TLS cert verification \n\
         \x20     -X, --method   <METH>  HTTP method (default GET)  \n\
         \x20         --path         <P>  Request path (default /)  \n\
         \x20     -b, --body        <B>  Request body               \n\
         \x20         --http <VER>       auto | http1 | http3        \n\
         \x20         --http3            Force HTTP/3 over QUIC      \n\
         \x20         --streams <N>      Concurrent streams (HTTP/3) \n\
         \x20     -v, --version          Print version details      \n\
         \x20                                                       \n\
         \x20 Numeric arguments may include a SI unit (1k, 1M, 1G)  \n\
         \x20 Time arguments may include a time unit (2s, 2m, 2h)   "
    );
}

pub fn usage() -> ! {
    print_usage();
    exit(1);
}

fn version_line() -> String {
    format!("wrk {} [async]", env!("CARGO_PKG_VERSION"))
}

// ---------------------------------------------------------------------------
// Parse entry point
// ---------------------------------------------------------------------------

/// Parse the CLI. Exits on error or --help/--version.
pub fn parse_args() -> Config {
    // clap derive, with errors converted to wrk-style "print usage, exit 1".
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(_) => usage(),
    };

    // -v: print version + copyright (like wrk) and exit 0.
    if cli.version {
        println!("{} ", version_line());
        println!("Copyright (C) 2012 Will Glozer");
        exit(0);
    }
    // -h, no URL, or any parse error: print usage and exit 1 (like wrk).
    if cli.help {
        usage();
    }

    let url = match cli.url {
        Some(u) => u,
        None => usage(),
    };

    let threads = cli.threads.map(|m| m.0).unwrap_or(DEFAULT_THREADS);
    let connections = cli.connections.map(|m| m.0).unwrap_or(DEFAULT_CONNECTIONS);
    let duration = cli.duration.map(|t| t.0).unwrap_or(DEFAULT_DURATION_SECS);
    // -T is in seconds; wrk multiplies by 1000 to get milliseconds.
    let timeout = cli
        .timeout
        .map(|t| t.0 * 1000)
        .unwrap_or(SOCKET_TIMEOUT_MS);
    let latency = cli.latency;
    let insecure = cli.insecure;
    let method = cli.method.as_str().to_string();
    let path = cli.path;
    let body = cli.body.map(|s| s.into_bytes()).unwrap_or_default();
    let headers: Vec<(String, String)> = cli.header.into_iter().map(|h| (h.0, h.1)).collect();

    // Protocol selection. --http3 is a shorthand that forces http3.
    let http_version = if cli.http3 {
        HttpVersion::Http3
    } else {
        cli.http_version.to_http_version()
    };
    let streams_per_conn = cli.streams_per_conn.0;

    // Validation mirroring wrk.c:528-538.
    if threads == 0 || duration == 0 {
        eprintln!("invalid number of threads or duration");
        exit(1);
    }
    if connections == 0 || connections < threads {
        eprintln!("number of connections must be >= threads");
        exit(1);
    }
    if streams_per_conn == 0 {
        eprintln!("--streams must be >= 1");
        exit(1);
    }

    let (scheme, host, port) = match parse_url(&url) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("invalid URL: {e}");
            exit(1);
        }
    };

    // HTTP/3 requires QUIC/UDP, which only makes sense for https:// targets.
    if matches!(http_version, HttpVersion::Http3) && scheme == Scheme::Http {
        eprintln!("--http3 / --http http3 requires an https:// URL");
        exit(1);
    }

    Config {
        threads,
        connections,
        duration,
        timeout,
        latency,
        insecure,
        method,
        path,
        body,
        headers,
        scheme,
        host,
        port,
        url,
        http_version,
        streams_per_conn,
        resolved: None,
    }
}

/// Minimal URL parser: scheme://host[:port][/path].
/// Returns (scheme, host, port). Path is taken from --path in Config.
fn parse_url(url: &str) -> Result<(Scheme, String, u16), String> {
    let (scheme_str, rest) = url
        .split_once("://")
        .ok_or_else(|| "missing scheme".to_string())?;
    let scheme = match scheme_str.to_ascii_lowercase().as_str() {
        "http" => Scheme::Http,
        "https" => Scheme::Https,
        other => return Err(format!("unsupported scheme: {other}")),
    };
    // authority ends at the first '/', '?', or '#'
    let auth_end = rest
        .find(['/', '?', '#'])
        .unwrap_or(rest.len());
    let authority = &rest[..auth_end];
    let (host, port) = if let Some(idx) = authority.rfind(':') {
        // Only treat as port if what follows is all digits and no bracket issues.
        let possible_port = &authority[idx + 1..];
        if possible_port.chars().all(|c| c.is_ascii_digit()) && !possible_port.is_empty() {
            let p: u16 = possible_port
                .parse()
                .map_err(|_| "invalid port".to_string())?;
            (authority[..idx].to_string(), p)
        } else {
            (authority.to_string(), default_port_for(scheme))
        }
    } else {
        (authority.to_string(), default_port_for(scheme))
    };
    if host.is_empty() {
        return Err("missing host".to_string());
    }
    Ok((scheme, host, port))
}

fn default_port_for(s: Scheme) -> u16 {
    match s {
        Scheme::Http => 80,
        Scheme::Https => 443,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parse_http_default_port() {
        let (s, h, p) = parse_url("http://example.com/path").unwrap();
        assert_eq!(s, Scheme::Http);
        assert_eq!(h, "example.com");
        assert_eq!(p, 80);
    }

    #[test]
    fn parse_http_explicit_port() {
        let (s, h, p) = parse_url("http://127.0.0.1:8080/index.html").unwrap();
        assert_eq!(s, Scheme::Http);
        assert_eq!(h, "127.0.0.1");
        assert_eq!(p, 8080);
    }

    #[test]
    fn parse_https_default_port() {
        let (s, h, p) = parse_url("https://example.com/").unwrap();
        assert_eq!(s, Scheme::Https);
        assert_eq!(h, "example.com");
        assert_eq!(p, 443);
    }

    #[test]
    fn parse_https_explicit_port() {
        let (s, h, p) = parse_url("https://example.com:8443/").unwrap();
        assert_eq!(s, Scheme::Https);
        assert_eq!(h, "example.com");
        assert_eq!(p, 8443);
    }

    #[test]
    fn parse_bad_url() {
        assert!(parse_url("not-a-url").is_err());
        assert!(parse_url("ftp://x/").is_err());
    }

    #[test]
    fn host_header_collapses_default_port() {
        let mut cfg = Config {
            threads: 1,
            connections: 1,
            duration: 1,
            timeout: 2000,
            latency: false,
            insecure: false,
            method: "GET".into(),
            path: "/".into(),
            body: vec![],
            headers: vec![],
            scheme: Scheme::Http,
            host: "example.com".into(),
            port: 80,
            url: "http://example.com/".into(),
            http_version: HttpVersion::Auto,
            streams_per_conn: 1,
            resolved: None,
        };
        assert_eq!(cfg.host_header(), "example.com");
        cfg.port = 8080;
        assert_eq!(cfg.host_header(), "example.com:8080");
    }

    #[test]
    fn metric_arg_parses_si() {
        assert_eq!(MetricArg::from_str("1").unwrap().0, 1);
        assert_eq!(MetricArg::from_str("1k").unwrap().0, 1000);
        assert_eq!(MetricArg::from_str("1M").unwrap().0, 1_000_000);
        assert!(MetricArg::from_str("abc").is_err());
    }

    #[test]
    fn time_arg_parses_units() {
        assert_eq!(TimeArg::from_str("10").unwrap().0, 10);
        assert_eq!(TimeArg::from_str("2m").unwrap().0, 120);
        assert_eq!(TimeArg::from_str("1h").unwrap().0, 3600);
        assert!(TimeArg::from_str("1x").is_err());
    }

    #[test]
    fn header_arg_splits_on_colon() {
        let h = HeaderArg::from_str("X-Test: foo").unwrap();
        assert_eq!(h.0, "X-Test");
        assert_eq!(h.1, "foo");
    }
}
