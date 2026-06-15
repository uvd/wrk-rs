// Faithful Rust port of wrk's src/units.c.
// Copyright (C) 2012 Will Glozer (original). Rust port licensed Apache-2.0.
//
// Implements the unit formatting/scanning logic with the exact "0.85 scale"
// promotion threshold used by the original tool, so printed magnitudes match.

const BINARY_UNITS: &[&str] = &["K", "M", "G", "T", "P"];
const METRIC_UNITS: &[&str] = &["k", "M", "G", "T", "P"];
const TIME_UNITS_US: &[&str] = &["ms", "s"];
const TIME_UNITS_S: &[&str] = &["m", "h"];

/// Format `n` using the given `scale`, `base` suffix, ladder of `units`, and
/// `precision`. A value is promoted to the next unit once it reaches 85% of the
/// current scale. `base` is the empty string for binary/metric, or a unit
/// string ("us"/"s") for time.
fn format_units(n: f64, scale: f64, base: &str, units: &[&str], precision: usize) -> String {
    let mut amt = n;
    let mut unit = base;
    let threshold = scale * 0.85;

    let mut i = 0;
    while i + 1 < units.len() && amt >= threshold {
        amt /= scale;
        unit = units[i];
        i += 1;
    }

    format!("{:.*}{}", precision, amt, unit)
}

/// Binary units (1024 scale, K/M/G/...). Used for byte counts.
pub fn format_binary(n: f64) -> String {
    format_units(n, 1024.0, "", BINARY_UNITS, 2)
}

/// Metric units (1000 scale, k/M/G/...). Used for request rates.
pub fn format_metric(n: f64) -> String {
    format_units(n, 1000.0, "", METRIC_UNITS, 2)
}

/// Time formatting from microseconds.
/// - n < 1_000_000: us→ms→s ladder (base "us")
/// - n >= 1_000_000: divide by 1e6 and use s→m→h ladder (base "s")
pub fn format_time_us(n: f64) -> String {
    if n >= 1_000_000.0 {
        format_units(n / 1_000_000.0, 60.0, "s", TIME_UNITS_S, 2)
    } else {
        format_units(n, 1000.0, "us", TIME_UNITS_US, 2)
    }
}

/// Time formatting from seconds (precision 0), s→m→h ladder.
pub fn format_time_s(n: f64) -> String {
    format_units(n, 60.0, "s", TIME_UNITS_S, 0)
}

/// Parse a metric-prefixed integer (e.g. "1k", "1M", "1000").
/// Returns None on parse failure. Case-insensitive unit match.
pub fn scan_metric(s: &str) -> Option<u64> {
    scan_units(s, 1000, METRIC_UNITS)
}

/// Parse a time-prefixed integer in seconds (e.g. "2s", "2m", "2h").
/// Returns None on parse failure. The unit "s" is the implicit base.
pub fn scan_time(s: &str) -> Option<u64> {
    // The base unit for time is "s"; treat it as a valid (no-op) suffix.
    let (base, rest) = split_unit(s);
    let val: u64 = base.parse().ok()?;

    match rest.to_ascii_lowercase().as_str() {
        "" | "s" => Some(val),
        "m" => Some(val.checked_mul(60)?),
        "h" => Some(val.checked_mul(3600)?),
        _ => None,
    }
}

fn scan_units(s: &str, scale: u64, units: &[&str]) -> Option<u64> {
    let (base, rest) = split_unit(s);
    let val: u64 = base.parse().ok()?;

    if rest.is_empty() {
        return Some(val);
    }

    let mut mult: u64 = 1;
    let lower = rest.to_ascii_lowercase();
    for u in units {
        mult = mult.checked_mul(scale)?;
        if lower == u.to_ascii_lowercase() {
            return val.checked_mul(mult);
        }
    }
    None
}

/// Split a numeric string into its leading digits and trailing (<=2 char) unit.
fn split_unit(s: &str) -> (&str, &str) {
    let trimmed = s.trim();
    let digits_end = trimmed
        .char_indices()
        .take_while(|(_, c)| c.is_ascii_digit())
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0);
    let (a, b) = trimmed.split_at(digits_end);
    // Truncate suffix to at most 2 chars (matches %2s in sscanf).
    let b2: &str = b.get(..b.char_indices().take(2).last().map(|(i, c)| i + c.len_utf8()).unwrap_or(b.len())).unwrap_or(b);
    (a, b2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_basic() {
        assert_eq!(format_binary(0.0), "0.00");
        assert_eq!(format_binary(512.0), "512.00");
        assert_eq!(format_binary(1024.0), "1.00K");
        assert_eq!(format_binary(1536.0), "1.50K");
        assert_eq!(format_binary(1048576.0), "1.00M");
    }

    #[test]
    fn metric_basic() {
        assert_eq!(format_metric(1.0), "1.00");
        assert_eq!(format_metric(1000.0), "1.00k");
        assert_eq!(format_metric(1500.0), "1.50k");
        assert_eq!(format_metric(1_000_000.0), "1.00M");
    }

    #[test]
    fn time_us_under_one_second() {
        assert_eq!(format_time_us(100.0), "100.00us");
        // 850us promotes to ms (>= 1000*0.85 = 850)
        assert_eq!(format_time_us(850.0), "0.85ms");
        assert_eq!(format_time_us(1000.0), "1.00ms");
        assert_eq!(format_time_us(999_999.0), "1000.00ms");
    }

    #[test]
    fn time_us_over_one_second() {
        // 1e6 us = 1s
        assert_eq!(format_time_us(1_000_000.0), "1.00s");
        // 51s: scale*0.85 = 60*0.85 = 51, so 51 promotes to minutes → "0.85m".
        // (The topmost unit "h" is unreachable because the C loop checks
        // units[i+1], so the time-s ladder tops out at minutes.)
        assert_eq!(format_time_us(51_000_000.0), "0.85m");
        assert_eq!(format_time_us(60_000_000.0), "1.00m");
    }

    #[test]
    fn time_s_formatting() {
        // Note: the time-s ladder is ["m","h"] but the topmost unit ("h") is
        // unreachable because the C promotion loop only proceeds while
        // units[i+1] exists. So format_time_s tops out at minutes.
        assert_eq!(format_time_s(10.0), "10s");
        assert_eq!(format_time_s(60.0), "1m");
        assert_eq!(format_time_s(120.0), "2m");
        assert_eq!(format_time_s(3600.0), "60m");
    }

    #[test]
    fn scan_metric_units() {
        assert_eq!(scan_metric("1"), Some(1));
        assert_eq!(scan_metric("1k"), Some(1000));
        assert_eq!(scan_metric("1K"), Some(1000));
        assert_eq!(scan_metric("1M"), Some(1_000_000));
        assert_eq!(scan_metric("2G"), Some(2_000_000_000));
        assert_eq!(scan_metric("abc"), None);
        assert_eq!(scan_metric("1x"), None);
    }

    #[test]
    fn scan_time_units() {
        assert_eq!(scan_time("10"), Some(10));
        assert_eq!(scan_time("10s"), Some(10));
        assert_eq!(scan_time("2m"), Some(120));
        assert_eq!(scan_time("1h"), Some(3600));
        assert_eq!(scan_time("1x"), None);
    }

    #[test]
    fn time_us_at_849_stays_us() {
        // 849 < 850 (the threshold), so it should remain in us
        assert_eq!(format_time_us(849.0), "849.00us");
    }
}
