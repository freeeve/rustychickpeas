//! Custom Criterion measurement for tracking allocations
//!
//! This module provides utilities for tracking allocations.
//! Allocation data is collected from dhat output files by the collector script.

use criterion::measurement::{Measurement, ValueFormatter};
use criterion::Throughput;
use std::time::{Duration, Instant};

/// Measurement that tracks time (allocations tracked separately via dhat output)
pub struct AllocMeasurement;

impl AllocMeasurement {
    pub fn new() -> Self {
        Self
    }
}

impl Measurement for AllocMeasurement {
    type Intermediate = Instant;
    type Value = Duration;

    fn start(&self) -> Self::Intermediate {
        Instant::now()
    }

    fn end(&self, i: Self::Intermediate) -> Self::Value {
        i.elapsed()
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        *v1 + *v2
    }

    fn zero(&self) -> Self::Value {
        Duration::ZERO
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        value.as_nanos() as f64
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        &TimeFormatter
    }
}

struct TimeFormatter;

impl ValueFormatter for TimeFormatter {
    fn scale_values(&self, _typical_value: f64, _values: &mut [f64]) -> &'static str {
        "ns"
    }

    fn scale_throughputs(
        &self,
        _typical_value: f64,
        _throughput: &Throughput,
        _values: &mut [f64],
    ) -> &'static str {
        "ns"
    }

    fn scale_for_machines(&self, _values: &mut [f64]) -> &'static str {
        "ns"
    }
}

impl Default for AllocMeasurement {
    fn default() -> Self {
        Self::new()
    }
}
