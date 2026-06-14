//! Instruction-count benchmarks (iai-callgrind).
//!
//! Unlike the wall-clock Criterion suites, these count CPU instructions via
//! Valgrind/Callgrind. Instruction counts are deterministic, so they reliably
//! catch small regressions even on noisy shared CI runners where wall-clock
//! variance would hide them. This is the suite wired for PR regression gating.
//!
//! Running locally requires `valgrind` and the matching `iai-callgrind-runner`:
//!
//! ```bash
//! cargo install iai-callgrind-runner --version 0.16.1
//! cargo bench -p rustychickpeas-core --bench instruction_counts
//! ```

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use roaring::RoaringBitmap;
use rustychickpeas_core::bitmap::NodeSet;
use rustychickpeas_core::types::{Direction, RelationshipType};
use rustychickpeas_core::{GraphBuilder, GraphSnapshot};
use std::hint::black_box;

/// Fixed graph size shared by the build size and the scan loops so traversal
/// benchmarks cover every node exactly once.
const N: usize = 5_000;

type NodeFilter = fn(u32, &GraphSnapshot) -> bool;
type RelFilter = fn(u32, u32, RelationshipType, u32, &GraphSnapshot) -> bool;

/// Build (without finalizing) a ring graph builder with `n` Person nodes and
/// `n` KNOWS relationships.
fn build_ring_builder(n: usize) -> GraphBuilder {
    let mut builder = GraphBuilder::new(Some(n), Some(n));
    for i in 0..n {
        builder.add_node(Some(i as u32), &["Person"]).unwrap();
    }
    for i in 0..n {
        let to = ((i + 1) % n) as u32;
        builder.add_rel(i as u32, to, "KNOWS").unwrap();
    }
    builder
}

/// Build and finalize a ring graph snapshot.
fn build_ring_graph(n: usize) -> GraphSnapshot {
    build_ring_builder(n).finalize(None)
}

// End-to-end construction: add nodes + relationships + finalize.
#[library_benchmark]
#[bench::ring(N)]
fn build_graph(n: usize) -> GraphSnapshot {
    black_box(build_ring_graph(black_box(n)))
}

// Finalize cost in isolation; the populated builder is created in the
// (unmeasured) setup closure.
#[library_benchmark]
#[bench::ring(args = (N), setup = build_ring_builder)]
fn finalize(builder: GraphBuilder) -> GraphSnapshot {
    black_box(builder.finalize(None))
}

// Read path: scan out-neighbors of every node; graph built in setup.
#[library_benchmark]
#[bench::ring(args = (N), setup = build_ring_graph)]
fn out_neighbors_scan(graph: GraphSnapshot) -> usize {
    let mut count = 0usize;
    for i in 0..N as u32 {
        count += black_box(graph.out_neighbors(black_box(i))).len();
    }
    black_box(count)
}

// Traversal: full BFS from a single source; graph built in setup.
#[library_benchmark]
#[bench::ring(args = (N), setup = build_ring_graph)]
fn bfs_traversal(graph: GraphSnapshot) -> u64 {
    let start = NodeSet::new(RoaringBitmap::from_iter([0u32]));
    let (nodes, rels) = graph.bfs::<NodeFilter, RelFilter>(
        black_box(&start),
        Direction::Outgoing,
        None,
        None,
        None,
        None,
    );
    black_box(nodes.len() + rels.len())
}

library_benchmark_group!(
    name = builder;
    benchmarks = build_graph, finalize
);
library_benchmark_group!(
    name = queries;
    benchmarks = out_neighbors_scan, bfs_traversal
);
main!(library_benchmark_groups = builder, queries);
