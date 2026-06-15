// Output formatting — faithful Rust port of wrk.c's print_* functions.
//
// Replicates the exact column widths, precisions, and the "0–2 trailing
// spaces" padding rule from print_units so printed magnitudes line up
// identically to the original tool.

use crate::stats::Stats;
use crate::units;

/// A formatting function: takes a long double (f64) and returns a unit string.
type UnitFmt = fn(f64) -> String;

/// Mirrors wrk.c print_units (lines 550-561):
///   - format the number with the given formatter
///   - pad = 2 minus (1 if last char is alpha) minus (1 if second-to-last is alpha)
///   - right-justify the formatted string in (width - pad), then print `pad`
///     trailing spaces
fn print_units(n: f64, fmt: UnitFmt, width: usize) {
    let msg = fmt(n);
    let bytes = msg.as_bytes();
    let len = bytes.len();
    let mut pad: usize = 2;
    if len >= 1 && bytes[len - 1].is_ascii_alphabetic() {
        pad = pad.saturating_sub(1);
    }
    if len >= 2 && bytes[len - 2].is_ascii_alphabetic() {
        pad = pad.saturating_sub(1);
    }
    let inner_width = width.saturating_sub(pad);
    // Right-justify the number in inner_width, then append `pad` spaces.
    if msg.len() >= inner_width {
        print!("{}", msg);
    } else {
        print!("{:>width$}", msg, width = inner_width);
    }
    for _ in 0..pad {
        print!(" ");
    }
}

/// "  Thread Stats%6s%11s%8s%12s\n" with Avg/Stdev/Max/+/-Stdev.
pub fn print_stats_header() {
    print!("{:>width$}", "Thread Stats", width = 2 + 12); // "  Thread Stats"
    print!("{:>width$}", "Avg", width = 6);
    print!("{:>width$}", "Stdev", width = 11);
    print!("{:>width$}", "Max", width = 8);
    print!("{:>width$}", "+/- Stdev", width = 12);
    println!();
}

/// Mirrors wrk.c print_stats (563-573).
pub fn print_stats(name: &str, stats: &Stats, fmt: UnitFmt) {
    let max = stats.max();
    let mean = stats.mean();
    let stdev = stats.stdev(mean);

    // "    %-10s" — 4 spaces, left-justified in 10.
    print!("    {:<10}", name);
    print_units(mean, fmt, 8);
    print_units(stdev, fmt, 10);
    print_units(max as f64, fmt, 9);
    let within = stats.within_stdev(mean, stdev, 1.0);
    println!("{:>8.2}%", within);
}

/// Mirrors wrk.c print_stats_latency (575-585): 50/75/90/99 %.
pub fn print_stats_latency(stats: &Stats) {
    let percentiles = [50.0_f64, 75.0, 90.0, 99.0];
    println!("  Latency Distribution");
    for p in percentiles {
        let n = stats.percentile(p);
        print!("{:>7.0}%", p);
        print_units(n as f64, units::format_time_us, 10);
        println!();
    }
}

/// Final report, mirroring wrk.c main (178-191).
pub struct Report<'a> {
    pub complete: u64,
    pub bytes: u64,
    pub runtime_us: u64,
    pub protocol: crate::config::ResolvedVersion,
    pub errors_connect: u64,
    pub errors_read: u64,
    pub errors_write: u64,
    pub errors_timeout: u64,
    pub errors_status: u64,
    pub latency: &'a Stats,
    pub requests: &'a Stats,
    pub print_latency: bool,
}

pub fn print_report(r: &Report) {
    let runtime_s = r.runtime_us as f64 / 1_000_000.0;
    let req_per_s = r.complete as f64 / runtime_s;
    let bytes_per_s = r.bytes as f64 / runtime_s;

    print_stats_header();
    print_stats("Latency", r.latency, units::format_time_us);
    print_stats("Req/Sec", r.requests, units::format_metric);
    if r.print_latency {
        print_stats_latency(r.latency);
    }

    let runtime_msg = units::format_time_us(r.runtime_us as f64);
    let bytes_msg = units::format_binary(r.bytes as f64);
    println!(
        "  {} requests in {}, {}B read [{}]",
        r.complete, runtime_msg, bytes_msg, r.protocol.as_str()
    );

    if r.errors_connect != 0
        || r.errors_read != 0
        || r.errors_write != 0
        || r.errors_timeout != 0
    {
        println!(
            "  Socket errors: connect {}, read {}, write {}, timeout {}",
            r.errors_connect, r.errors_read, r.errors_write, r.errors_timeout
        );
    }

    if r.errors_status != 0 {
        println!("  Non-2xx or 3xx responses: {}", r.errors_status);
    }

    // "Requests/sec: %9.2Lf\n"
    println!("Requests/sec:{:>9.2}", req_per_s);
    // "Transfer/sec: %10sB\n"
    let bps = units::format_binary(bytes_per_s);
    println!("Transfer/sec:{:>10}B", bps);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_width() {
        // The original prints exactly:
        //   "  Thread Stats" + "Avg"(6) + "Stdev"(11) + "Max"(8) + "+/- Stdev"(12)
        // We just exercise print_units to ensure no panic.
        print_units(0.0, units::format_binary, 8);
    }

    #[test]
    fn units_no_trailing_unit_two_spaces() {
        // format_binary(0) = "0.00" — no trailing alpha → pad=2.
        // Just sanity-check the formatter itself here.
        assert_eq!(units::format_binary(0.0), "0.00");
        assert_eq!(units::format_metric(0.0), "0.00");
    }

    #[test]
    fn stats_table_runs() {
        let s = Stats::alloc(10_000);
        s.record(100);
        s.record(200);
        s.record(300);
        print_stats("Latency", &s, units::format_time_us);
        print_stats_latency(&s);
    }
}
