# wrk-rs

A modern HTTP benchmarking tool — a pure-Rust reimplementation of
[wrk](https://github.com/wg/wrk), built on **tokio** + **hyper** + **rustls**,
with **HTTP/2** and **HTTP/3 (QUIC)** support.

The command-line interface, benchmarking behaviour, statistics, and output
format are **byte-for-byte compatible** with the original `wrk`. Lua scripting
is replaced by built-in flags (`-X`/`--method`, `--path`, `-b`/`--body`).

- [Features](#features)
- [Install](#install)
- [Quick start](#quick-start)
- [Command-line reference](#command-line-reference)
- [Choosing the protocol (HTTP/3)](#choosing-the-protocol-http3)
- [Reading the output](#reading-the-output)
- [Common scenarios](#common-scenarios)
- [Local test servers](#local-test-servers)
- [Comparison with the original wrk](#comparison-with-the-original-wrk)
- [Implementation notes](#implementation-notes)
- [License](#license)

## Features

- HTTP, HTTPS, **HTTP/2**, and **HTTP/3** benchmarking (TLS via rustls, QUIC
  via quinn/h3)
- **Protocol negotiation**: `https://` auto-detects the best protocol in
  priority order HTTP/3 → HTTP/2 → HTTP/1.1; force any with `--http`
- **HTTP/2 & HTTP/3 multiplexing**: one connection carries multiple concurrent
  streams (`--streams N`), eliminating head-of-line blocking
- Multithreaded — one OS thread per `-t`, each running a current-thread tokio
  runtime (mirrors wrk's per-thread event loop)
- Fixed connection count with automatic reconnect on error
- Direct-addressed latency histogram (1 µs resolution) — same algorithm as `wrk`
- Coordinated-omission correction, sample standard deviation, nearest-rank
  percentiles, 100 ms rate sampling
- Output format identical to `wrk` (column widths, the 0.85 unit-promotion
  threshold, precisions) plus a `[protocol]` annotation

## Install

From source:

```sh
git clone <this repo> wrk-rs
cd wrk-rs
cargo build --release
# binary: target/release/wrk
```

Copy it onto your `PATH` (optional):

```sh
cp target/release/wrk ~/.local/bin/wrk
```

Verify it runs:

```sh
wrk -v
# wrk 0.1.0 [async]
# Copyright (C) 2012 Will Glozer
```

## Quick start

Benchmark a local server for 10 seconds with 2 threads and 50 connections:

```sh
wrk -t2 -c50 -d10s http://127.0.0.1:8080/
```

You'll see output like:

```
Running 10s test @ http://127.0.0.1:8080/ [HTTP/1.1]
  2 threads and 50 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   490.59us   73.35us   1.91ms   82.65%
    Req/Sec    50.70k     3.66k   52.60k    96.00%
  509217 requests in 5.05s, 37.88MB read [HTTP/1.1]
Requests/sec:100788.49
Transfer/sec:     7.50MB
```

Add `-L` to see the latency percentile distribution:

```sh
wrk -t2 -c50 -d10s -L http://127.0.0.1:8080/
```

That's the basics. Read on for every flag, the HTTP/3 options, and how to
interpret the report.

## Command-line reference

```
Usage: wrk <options> <url>
  Options:
    -c, --connections <N>  Connections to keep open
    -d, --duration    <T>  Duration of test
    -t, --threads     <N>  Number of threads to use

    -H, --header      <H>  Add header to request
        --latency          Print latency statistics
        --timeout     <T>  Socket/request timeout
        --insecure         Skip TLS cert verification
    -X, --method   <METH>  HTTP method (default GET)
        --path         <P>  Request path (default /)
    -b, --body        <B>  Request body
        --http <VER>       auto | http1 | http2 | http3
        --http2            Force HTTP/2
        --http3            Force HTTP/3 over QUIC
        --streams <N>      Concurrent streams (HTTP/2, HTTP/3)
    -v, --version          Print version details

  Numeric arguments may include a SI unit (1k, 1M, 1G)
  Time arguments may include a time unit (2s, 2m, 2h)
```

### Core options

| Flag | Default | Description |
|---|---|---|
| `-t, --threads <N>` | `2` | Number of OS threads (event loops). Each thread owns `connections / threads` connections. |
| `-c, --connections <N>` | `10` | Total connections kept open across all threads. Must be ≥ threads. |
| `-d, --duration <T>` | `10s` | How long to run before signalling stop. |
| `-H, --header <H>` | *(none)* | Add a request header, `Name: Value`. Repeatable. |
| `-L, --latency` | off | Print the latency percentile distribution (50/75/90/99%). |
| `-T, --timeout <T>` | `2s` | Per-request timeout. Requests exceeding this are counted as timeouts and dropped from the latency histogram. |
| `-v, --version` | — | Print version + copyright and exit. |
| `-h, --help` | — | Print usage and exit 1. |

### Request-shaping options (replaces wrk's Lua scripts)

| Flag | Default | Description |
|---|---|---|
| `-X, --method <METH>` | `GET` | HTTP method. Case-insensitive: `POST`, `post`, `Put`, etc. (`get`/`post`/`put`/`patch`/`delete`/`head`/`options`). |
| `--path <P>` | `/` | Request path (e.g. `/api/users`). |
| `-b, --body <B>` | *(none)* | Request body (sent with a `Content-Length`). |

These three replace the most common uses of wrk's Lua scripts (custom method,
path, body) with plain CLI flags.

### TLS options

| Flag | Default | Description |
|---|---|---|
| `--insecure` | off | Skip TLS certificate verification. Use for self-signed certs (mirrors wrk's `SSL_VERIFY_NONE`). |

Without `--insecure`, HTTPS connections verify the server certificate against
the system/webpki root store. For HTTP/3, `--insecure` applies to the QUIC
handshake too.

### Numeric units

`-c` and `-t` accept SI suffixes:

```sh
wrk -c1k -t4 http://...      # 1000 connections, 4 threads
wrk -c2M -t8 http://...      # 2,000,000 connections, 8 threads
```

`-d` and `-T` accept time suffixes (`s`/`m`/`h`):

```sh
wrk -d2m http://...          # 2 minutes
wrk -d1h http://...          # 1 hour
wrk -T5s http://...          # 5 second timeout
```

> **Note:** Like the original `wrk`, `-T` only accepts `s`/`m`/`h` — not `ms`.
> `-T 2s` becomes a 2000 ms timeout internally.

### Validation rules

- A `<url>` is required and must include a scheme (`http://` or `https://`).
- `threads` and `duration` must be non-zero.
- `connections` must be ≥ `threads`.
- `--http3` / `--http http3` requires an `https://` URL (QUIC is TLS-only).
- `--streams` must be ≥ 1.

## Choosing the protocol (HTTP/2 / HTTP/3)

| Flag | Behaviour |
|---|---|
| *(default)* `--http auto` | `https://` → negotiate HTTP/3 → HTTP/2 → HTTP/1.1 (in priority order); `http://` → HTTP/1.1 |
| `--http http1` | always HTTP/1.1, even for `https://` |
| `--http http2` | always HTTP/2 (requires `https://`) |
| `--http2` | shorthand for `--http http2` |
| `--http http3` | always HTTP/3 (requires `https://`) |
| `--http3` | shorthand for `--http http3` |
| `--streams <N>` | concurrent streams per connection (HTTP/2 and HTTP/3) |

### How auto-negotiation works

For an `https://` URL with `--http auto` (the default), wrk-rs probes in
priority order:

1. **HTTP/3** — opens a QUIC handshake (1s budget). Success → HTTP/3.
2. **HTTP/2** — performs a TLS handshake advertising `h2`+`http/1.1` via ALPN;
   if the server picks `h2` → HTTP/2.
3. **HTTP/1.1** — fallback.

Each step prints what it negotiated (e.g. `Negotiated HTTP/2`). The actual
protocol is also shown in the report's `[...]` annotation.

### Multiplexing (HTTP/2 & HTTP/3)

HTTP/2 and HTTP/3 multiplex many streams over one connection, avoiding the
head-of-line blocking that limits HTTP/1.1. For these protocols, `-c` is the
number of connections and `--streams` is the concurrent stream count per
connection, so total in-flight requests is `-c × --streams`.

```sh
# 10 connections × 20 streams = 200 concurrent requests over HTTP/2
wrk --http2 --streams 20 -t4 -c10 -d10s https://example.com/

# Same idea over HTTP/3
wrk --http3 --streams 20 -t4 -c10 -d10s https://example.com/
```

## Reading the output

A typical run:

```
Running 5s test @ http://127.0.0.1:18081/ [HTTP/1.1]
  2 threads and 50 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   490.59us   73.35us   1.91ms   82.65%
    Req/Sec    50.70k     3.66k   52.60k    96.00%
  509217 requests in 5.05s, 37.88MB read [HTTP/1.1]
Requests/sec:100788.49
Transfer/sec:     7.50MB
```

- **Header line** — duration, URL, resolved protocol.
- **`Latency` row** — per-request latency (the round trip). `Avg`/`Stdev`/`Max`
  use the [0.85 unit-promotion rule](#implementation-notes) (so `490us` stays in
  µs but `850us` shows as `0.85ms`). `+/- Stdev` is the % of samples within one
  standard deviation of the mean.
- **`Req/Sec` row** — request rate per thread, sampled every 100 ms. This
  reflects throughput stability; a high `+/- Stdev` means bursty throughput.
- **`N requests in T, XB read`** — total completed requests, wall-clock runtime,
  total bytes received.
- **`Requests/sec`** — overall throughput (`requests / runtime`).
- **`Transfer/sec`** — overall received bandwidth.

With `-L`, a latency distribution block is added:

```
  Latency Distribution
     50%  484.00us
     75%  520.00us
     90%  560.00us
     99%  668.00us
```

These are nearest-rank percentiles from the latency histogram.

### Error lines (conditional)

These lines only appear when non-zero:

- **`Socket errors: connect N, read N, write N, timeout N`** — transport-level
  errors. `timeout` counts requests that exceeded `-T`.
- **`Non-2xx or 3xx responses: N`** — responses with status > 399 (4xx/5xx).

## Common scenarios

### Basic HTTP benchmark

```sh
wrk -t2 -c50 -d10s http://127.0.0.1:8080/
```

### HTTPS with verified certificate

```sh
wrk -t4 -c100 -d30s -L https://example.com/
```

### HTTPS with self-signed certificate

```sh
wrk -t4 -c100 -d30s --insecure https://127.0.0.1:8443/
```

### Auto-negotiate the best protocol (the default for https)

```sh
# Tries HTTP/3 → HTTP/2 → HTTP/1.1 in order and uses the best available.
wrk -t4 -c100 -d30s https://cloudflare.com/
```

### Force HTTP/2

```sh
wrk --http2 --insecure -t2 -c10 --streams 20 -d10s https://127.0.0.1:18444/
```

### Force HTTP/3

```sh
wrk --http3 --insecure -t2 -c10 --streams 20 -d10s https://127.0.0.1:18443/
```

### Custom method, headers, body (replaces wrk Lua scripts)

```sh
wrk -t2 -c50 -d10s \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer secret-token" \
    -b '{"hello":"world"}' \
    --path /api/v1/echo \
    http://127.0.0.1:8080/
```

### Long-running soak test

```sh
wrk -t8 -c1000 -d30m http://127.0.0.1:8080/
```

### High-connection stress test

```sh
wrk -t16 -c50k -d60s http://127.0.0.1:8080/
```

### Short timeout to surface slow responses

```sh
wrk -t2 -c100 -T1s -d10s http://127.0.0.1:8080/
```

Requests slower than 1s are counted as timeouts and excluded from the latency
histogram.

## Local test servers

Three example servers are included for benchmarking (in `examples/`):

```sh
# HTTP/1.1 server on port 18081 (plain TCP, fast)
cargo run --release --example bench_server

# HTTP/2 server on port 18444 (TLS + h2 via ALPN, self-signed cert — use --insecure)
cargo run --release --example bench_h2_server

# HTTP/3 server on port 18443 (QUIC, self-signed cert — use --insecure)
cargo run --release --example bench_h3_server
```

Then point wrk-rs at them:

```sh
# HTTP/1.1
wrk -t2 -c50 -d5s http://127.0.0.1:18081/

# HTTP/2 (self-signed)
wrk --http2 --insecure -t2 -c10 --streams 10 -d5s https://127.0.0.1:18444/

# HTTP/3 (self-signed)
wrk --http3 --insecure -t2 -c10 --streams 10 -d5s https://127.0.0.1:18443/
```

## Comparison with the original wrk

| | wrk | wrk-rs |
|---|---|---|
| Language | C | Rust |
| I/O model | Redis `ae` event loop (epoll/kqueue) | tokio async/await |
| HTTP/1 | vendored joyent http-parser | hyper 1.x |
| HTTP/2 | — | hyper 1.x (h2) |
| HTTP/3 | — | quinn + h3 |
| TLS | OpenSSL | rustls |
| Scripting | LuaJIT (`-s`) | not supported; use `-X`/`--path`/`-b` instead |
| Cert verification | none (`SSL_VERIFY_NONE`) | verified by default; `--insecure` to disable |
| Protocol selection | — | `--http auto/http1/http2/http3` with auto-negotiation |
| Output format | — | **identical** (plus a `[protocol]` annotation) |

The CLI flags `-c`/`-d`/`-t`/`-H`/`-L`/`-T`/`-v`/`-h` are unchanged from the
original, so any existing `wrk` command line works as-is against an `http://`
target. `https://` adds auto HTTP/3 negotiation on top.

## Implementation notes

The statistics and formatting logic are faithful ports of `wrk`'s `stats.c`
and `units.c`:

- Latency histogram is **direct-addressed** (1 bucket per µs, up to
  `timeout_ms × 1000`).
- Unit promotion threshold is **`scale × 0.85`** (e.g. 850 µs → `0.85ms`).
- Standard deviation uses **Bessel's correction** (`n − 1`).
- Percentile rank is `round(p/100 × count + 0.5)` (round half away from zero).
- `status > 399` is counted as a non-2xx/3xx response.
- A latency sample `≥ timeout` is discarded and counted as a timeout error.
- Coordinated-omission correction uses
  `expected = runtime_us / (complete / connections)`.
- The Req/Sec statistic is sampled every 100 ms per worker (per-worker counter,
  so multi-thread runs don't show cross-thread sampling variance).

## License

Apache-2.0.
