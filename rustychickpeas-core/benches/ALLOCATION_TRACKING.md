# Allocation Tracking in Benchmarks

This document explains how to enable and use allocation tracking in Criterion benchmarks.

## Overview

Criterion.rs doesn't natively track allocations. To measure allocations, we use `dhat-rs`, a heap profiler that can track allocations during benchmark execution.

## Enabling Allocation Tracking

### Option 1: Use dhat feature (Recommended)

1. Run benchmarks with the `dhat` feature:
   ```bash
   cargo bench --features dhat --bench graph_builder
   ```

2. dhat will write allocation data to files in `target/criterion/`

3. The collector script will automatically extract allocation data if available

### Option 2: Manual Tracking (Alternative)

If you don't want to use dhat, you can manually track allocations by wrapping your benchmark code:

```rust
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    // ... implement other methods
}
```

## Current Status

Allocation tracking is currently **optional** and requires:
1. Enabling the `dhat` feature when running benchmarks
2. The collector script will extract allocation data from dhat's output

## Future Improvements

We could:
- Create a custom Criterion measurement that tracks allocations
- Use `criterion-perf-events` if available
- Integrate allocation data directly into Criterion's estimates.json

For now, allocation data will be empty in the CSV if dhat is not used.

