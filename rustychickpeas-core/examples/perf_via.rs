//! Microbench for the fold_via / co_occurring / neighbor_groups kernels, used to
//! check the `impl RelTypeFilter` widening (task 035) for regressions. Calls the
//! public API with a `&str` rel type, so it compiles against both the pre-035
//! (`rel: &str`) and post-035 (`impl RelTypeFilter`) signatures.
//!
//! Run: cargo run --release -p rustychickpeas-core --example perf_via

use std::time::Instant;

use rustychickpeas_core::types::Direction;
use rustychickpeas_core::{CoWeight, GraphBuilder};

fn main() {
    let n: u32 = std::env::var("PERF_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300_000);

    // Build a deterministic graph: each node u has 3 outgoing "R" rels.
    let mut b = GraphBuilder::new(Some(n as usize), Some(n as usize * 3));
    for id in 0..n {
        b.add_node(Some(id), &["N"]).unwrap();
    }
    for u in 0..n {
        for step in [1u32, 7, 97] {
            b.add_relationship(u, (u + step) % n, "R").unwrap();
        }
    }
    let g = b.finalize(None);
    println!(
        "graph: {} nodes, {} rels",
        g.node_count(),
        g.relationship_count()
    );

    // Projection for fold_via: map each node into one of 5000 buckets.
    let proj: Vec<u32> = (0..n).map(|i| i % 5000).collect();

    // --- fold_via ---
    let t = Instant::now();
    let mut fold_checksum = 0u64;
    for _ in 0..5 {
        let m = g.fold_via("R", Direction::Outgoing, &proj);
        fold_checksum = fold_checksum.wrapping_add(m.values().sum::<u64>());
    }
    println!(
        "fold_via      5x : {:>8.2} ms   (checksum {})",
        t.elapsed().as_secs_f64() * 1000.0,
        fold_checksum
    );

    // --- co_occurring (sequential per seed) ---
    let t = Instant::now();
    let mut co_checksum = 0u64;
    for seed in 0..2000u32 {
        let m = g.co_occurring(seed, "R", Direction::Both, CoWeight::Count);
        co_checksum = co_checksum.wrapping_add(m.len() as u64);
    }
    println!(
        "co_occurring 2k : {:>8.2} ms   (checksum {})",
        t.elapsed().as_secs_f64() * 1000.0,
        co_checksum
    );

    // --- neighbor_groups.sizes() ---
    let sources: Vec<u32> = (0..n).collect();
    let t = Instant::now();
    let mut ng_checksum = 0u64;
    for _ in 0..3 {
        let sizes = g
            .neighbor_groups(&sources, "R", Direction::Outgoing)
            .sizes();
        ng_checksum = ng_checksum.wrapping_add(sizes.iter().map(|&(_, s)| s as u64).sum());
    }
    println!(
        "neighbor_grp 3x : {:>8.2} ms   (checksum {})",
        t.elapsed().as_secs_f64() * 1000.0,
        ng_checksum
    );
}
