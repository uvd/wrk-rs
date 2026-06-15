// Faithful Rust port of wrk's src/stats.c.
// Copyright (C) 2012 Will Glozer (original). Rust port licensed Apache-2.0.
//
// A direct-addressed histogram: bucket `data[n]` holds the frequency of value
// `n`. Recording is O(1); queries iterate `min..=max`. Mirrors the C struct
// `stats` exactly (count, limit, min, max, data[]) and uses atomic increments
// for thread-safe recording.

use std::sync::atomic::{AtomicU64, Ordering};

/// C's round() rounds half away from zero. Used for percentile rank math,
/// where the argument is always non-negative, so this only needs the
/// positive case.
fn round_half_away(x: f64) -> u64 {
    (x + 0.5) as u64
}

/// Direct-addressed histogram, mirroring wrk's `stats` struct.
pub struct Stats {
    count: AtomicU64,
    limit: u64,
    min: AtomicU64,
    max: AtomicU64,
    data: Box<[AtomicU64]>,
}

impl Stats {
    /// Allocate a histogram that can record values `0..=max`.
    /// `limit = max + 1`. `min` starts at u64::MAX.
    pub fn alloc(max: u64) -> Self {
        let limit = max.checked_add(1).expect("max + 1 overflow");
        let mut data = Vec::with_capacity(limit as usize);
        for _ in 0..limit {
            data.push(AtomicU64::new(0));
        }
        Stats {
            count: AtomicU64::new(0),
            limit,
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
            data: data.into_boxed_slice(),
        }
    }

    /// Record a sample. Returns `false` (and records nothing) if `n >= limit`,
    /// which the caller treats as a timeout. Atomically increments `data[n]`
    /// and `count`, and CAS-updates `min`/`max`.
    pub fn record(&self, n: u64) -> bool {
        if n >= self.limit {
            return false;
        }
        self.data[n as usize].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        // CAS-update min
        let mut cur_min = self.min.load(Ordering::Relaxed);
        while n < cur_min {
            match self.min.compare_exchange_weak(
                cur_min,
                n,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur_min = actual,
            }
        }
        // CAS-update max
        let mut cur_max = self.max.load(Ordering::Relaxed);
        while n > cur_max {
            match self.max.compare_exchange_weak(
                cur_max,
                n,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur_max = actual,
            }
        }
        true
    }

    #[allow(dead_code)]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
    #[allow(dead_code)]
    pub fn min(&self) -> u64 {
        self.min.load(Ordering::Relaxed)
    }
    pub fn max(&self) -> u64 {
        self.max.load(Ordering::Relaxed)
    }

    /// Coordinated-omission correction. For each bucket n >= 2*expected with
    /// samples, redistribute the count down toward `expected` in steps of
    /// `expected`. This synthesizes samples the client would have sent had it
    /// not been slowed by the observed latency.
    pub fn correct(&self, expected: i64) {
        if expected <= 0 {
            return;
        }
        let max = self.max.load(Ordering::Relaxed) as i64;
        let start = (expected * 2) as u64;
        let end = max.max(0) as u64;
        let mut n = start;
        while n <= end {
            let count = self.data[n as usize].load(Ordering::Relaxed);
            if count > 0 {
                let mut m = n as i64 - expected;
                while m > expected {
                    self.data[m as usize].fetch_add(count, Ordering::Relaxed);
                    self.count.fetch_add(count, Ordering::Relaxed);
                    m -= expected;
                }
            }
            n += 1;
        }
    }

    /// Arithmetic mean: sum(data[i]*i) / count.
    pub fn mean(&self) -> f64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let min = self.min.load(Ordering::Relaxed);
        let max = self.max.load(Ordering::Relaxed);
        let mut sum: u128 = 0;
        for i in min..=max {
            sum += self.data[i as usize].load(Ordering::Relaxed) as u128 * i as u128;
        }
        sum as f64 / count as f64
    }

    /// Sample standard deviation (n-1 denominator, Bessel's correction).
    pub fn stdev(&self, mean: f64) -> f64 {
        let count = self.count.load(Ordering::Relaxed);
        if count < 2 {
            return 0.0;
        }
        let min = self.min.load(Ordering::Relaxed);
        let max = self.max.load(Ordering::Relaxed);
        let mut sum: f64 = 0.0;
        for i in min..=max {
            let c = self.data[i as usize].load(Ordering::Relaxed);
            if c > 0 {
                let d = i as f64 - mean;
                sum += d * d * c as f64;
            }
        }
        (sum / (count - 1) as f64).sqrt()
    }

    /// Percentage of samples within `n` standard deviations of the mean.
    pub fn within_stdev(&self, mean: f64, stdev: f64, n: f64) -> f64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let upper = mean + stdev * n;
        let lower = mean - stdev * n;
        let min = self.min.load(Ordering::Relaxed);
        let max = self.max.load(Ordering::Relaxed);
        let mut sum: u64 = 0;
        for i in min..=max {
            let (il, iu) = (i as f64 >= lower, i as f64 <= upper);
            if il && iu {
                sum += self.data[i as usize].load(Ordering::Relaxed);
            }
        }
        sum as f64 / count as f64 * 100.0
    }

    /// Nearest-rank percentile: rank = round(p/100 * count + 0.5).
    /// Scans min..=max accumulating until total >= rank.
    pub fn percentile(&self, p: f64) -> u64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return 0;
        }
        // C's round() is round-half-away-from-zero; Rust's f64::round() is
        // round-half-to-even. Emulate the C semantics here.
        let rank = round_half_away(p / 100.0 * count as f64 + 0.5);
        let min = self.min.load(Ordering::Relaxed);
        let max = self.max.load(Ordering::Relaxed);
        let mut total: u64 = 0;
        for i in min..=max {
            total += self.data[i as usize].load(Ordering::Relaxed);
            if total >= rank {
                return i;
            }
        }
        0
    }

    /// Number of distinct values that have been recorded (non-empty buckets).
    #[allow(dead_code)]
    pub fn popcount(&self) -> u64 {
        let min = self.min.load(Ordering::Relaxed);
        let max = self.max.load(Ordering::Relaxed);
        let mut count = 0u64;
        for i in min..=max {
            if self.data[i as usize].load(Ordering::Relaxed) > 0 {
                count += 1;
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_count() {
        let s = Stats::alloc(1000);
        assert!(s.record(10));
        assert!(s.record(20));
        assert!(s.record(10));
        assert_eq!(s.count(), 3);
        assert_eq!(s.min(), 10);
        assert_eq!(s.max(), 20);
    }

    #[test]
    fn record_out_of_range_returns_false() {
        let s = Stats::alloc(5);
        assert!(s.record(5));
        assert!(!s.record(6));
        assert!(!s.record(100));
        assert_eq!(s.count(), 1);
    }

    #[test]
    fn mean_stdev() {
        // values: 10 (x2), 20 (x1) => mean = 40/3 = 13.333...
        let s = Stats::alloc(1000);
        s.record(10);
        s.record(10);
        s.record(20);
        let m = s.mean();
        assert!((m - (40.0 / 3.0)).abs() < 1e-9);
        // sample stdev: sqrt(((10-13.33)^2*2 + (20-13.33)^2) / 2)
        let sd = s.stdev(m);
        let expected = ((2.0 * (10.0 - m).powi(2) + (20.0 - m).powi(2)) / 2.0).sqrt();
        assert!((sd - expected).abs() < 1e-9);
    }

    #[test]
    fn percentile_basic() {
        let s = Stats::alloc(1000);
        for v in 1..=100u64 {
            s.record(v);
        }
        // 100 samples, p=50 => rank = round(50 + 0.5) = 51 => 51st value
        assert_eq!(s.percentile(50.0), 51);
        // p=99 => rank = round(99.5) = 100 => 100th value
        assert_eq!(s.percentile(99.0), 100);
        // p=100 => rank = round(100.5) = 101, which exceeds total (100), so
        // the C function returns 0 — the same behaviour we replicate.
        assert_eq!(s.percentile(100.0), 0);
    }

    #[test]
    fn correct_redistributes() {
        // Put a single outlier far beyond expected; correction should add
        // synthetic samples at n-expected, n-2*expected, ...
        let s = Stats::alloc(1000);
        s.record(100); // outlier
        let expected = 10;
        s.correct(expected);
        // Should have added samples at 90, 80, 70, ..., down to >expected (i.e. >10)
        // so at 90,80,70,60,50,40,30,20
        assert!(s.data[90].load(Ordering::Relaxed) >= 1);
        assert!(s.data[20].load(Ordering::Relaxed) >= 1);
        // nothing at 10 or below from correction
        assert_eq!(s.data[10].load(Ordering::Relaxed), 0);
    }

    #[test]
    fn within_stdev_all_within_when_uniform() {
        let s = Stats::alloc(100);
        for _ in 0..10 {
            s.record(50);
        }
        let m = s.mean();
        let sd = s.stdev(m);
        // stdev is 0 (all identical) so everything is "within 0 stdev"
        let pct = s.within_stdev(m, sd, 1.0);
        assert!((pct - 100.0).abs() < 1e-9);
    }

    #[test]
    fn popcount() {
        let s = Stats::alloc(100);
        s.record(5);
        s.record(5);
        s.record(10);
        s.record(15);
        assert_eq!(s.popcount(), 3);
    }
}
