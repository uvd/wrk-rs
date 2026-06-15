# wrk-rs

A modern HTTP benchmarking tool — a pure-Rust reimplementation of
[wrk](https://github.com/wg/wrk), built on **tokio** + **hyper** + **rustls**.

The command-line interface, benchmarking behaviour, statistics, and output
format are **byte-for-byte compatible** with the original `wrk`, so existing
automation and tooling that consumes `wrk` output works unchanged. Lua scripting
is replaced by built-in flags (`-X`/`--method`, `--path`, `-b`/`--body`).

## Features

- HTTP and HTTPS benchmarking (TLS via rustls)
- Multithreaded (one OS thread per `-t`, each running a current-thread tokio runtime)
- Fixed connection count, automatic reconnect on error
- Direct-addressed latency histogram (1 µs resolution) — same algorithm as `wrk`
- Coordinated-omission correction (`stats_correct`)
- Sample standard deviation, nearest-rank percentiles
- 100 ms rate sampling for the Req/Sec statistic
- Output format identical to `wrk` (column widths, unit promotion threshold, precisions)

## Build

```sh
cargo build --release
# binary: target/release/wrk
```

## Usage

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
    -v, --version          Print version details

  Numeric arguments may include a SI unit (1k, 1M, 1G)
  Time arguments may include a time unit (2s, 2m, 2h)
```

Defaults: `2` threads, `10` connections, `10s` duration, `2000ms` timeout,
`GET /`.

## Examples

Basic benchmark:

```sh
wrk -t2 -c10 -d10s http://127.0.0.1:8080/
```

With latency percentiles:

```sh
wrk -t4 -c100 -d30s -L https://example.com/
```

Custom request (method, headers, body):

```sh
wrk -t2 -c50 -d10s -X POST -H "Content-Type: application/json" \
    -b '{"hello":"world"}' --path /api http://127.0.0.1:8080/
```

Self-signed certificate (mirrors wrk's default `SSL_VERIFY_NONE`):

```sh
wrk -t1 -c10 -d10s --insecure https://127.0.0.1:8443/
```

## Sample output

```
Running 10s test @ http://127.0.0.1:8080/
  2 threads and 10 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   635.91us    0.89ms  12.92ms   93.69%
    Req/Sec    56.20k     8.07k   62.00k    86.54%
  22464657 requests in 30.00s, 17.76GB read
Requests/sec: 748868.53
Transfer/sec:    606.33MB
```

With `--latency`:

```
  Latency Distribution
     50%    1.00ms
     75%    2.00ms
     90%    5.00ms
     99%   10.00ms
```

## Differences from the original wrk

| | wrk | wrk-rs |
|---|---|---|
| Language | C | Rust |
| I/O model | Redis `ae` event loop (epoll/kqueue) | tokio async/await |
| HTTP | vendored joyent http-parser | hyper 1.x |
| TLS | OpenSSL | rustls |
| Scripting | LuaJIT (`-s`) | not supported; use `-X`/`--path`/`-b` instead |
| Cert verification | none (`SSL_VERIFY_NONE`) | verified by default; `--insecure` to disable |
| Output format | — | **identical** |

## Implementation notes (faithfulness)

The statistics and formatting logic are faithful ports of `wrk`'s `stats.c`
and `units.c`:

- Latency histogram is **direct-addressed** (1 bucket per µs, up to `timeout_ms × 1000`).
- Unit promotion threshold is **`scale × 0.85`** (e.g. 850 µs → `0.85ms`).
- Standard deviation uses **Bessel's correction** (`n − 1`).
- Percentile rank is `round(p/100 × count + 0.5)` (round half away from zero).
- `status > 399` is counted as a non-2xx/3xx response.
- A latency sample `≥ timeout` is discarded and counted as a timeout error.
- Coordinated-omission correction uses `expected = runtime_us / (complete / connections)`.
- The Req/Sec statistic is sampled every 100 ms per worker.

## License

Apache-2.0.
