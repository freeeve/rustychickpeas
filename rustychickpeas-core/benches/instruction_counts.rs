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
use rustychickpeas_core::types::{Direction, PropertyValue, RelationshipType};
use rustychickpeas_core::{GraphBuilder, GraphSnapshot, ValueId};
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
        builder.add_relationship(i as u32, to, "KNOWS").unwrap();
    }
    builder
}

/// Build and finalize a ring graph snapshot.
fn build_ring_graph(n: usize) -> GraphSnapshot {
    build_ring_builder(n).finalize(None)
}

/// Build and finalize a ring graph whose KNOWS relationships each carry a
/// `weight` i64 property, used for the weighted-Dijkstra read path.
fn build_weighted_ring_graph(n: usize) -> GraphSnapshot {
    let mut builder = GraphBuilder::new(Some(n), Some(n));
    for i in 0..n {
        builder.add_node(Some(i as u32), &["Person"]).unwrap();
    }
    for i in 0..n {
        let to = ((i + 1) % n) as u32;
        let idx = builder.add_relationship(i as u32, to, "KNOWS").unwrap();
        builder.set_relationship_props_by_index(
            idx,
            &[("weight", PropertyValue::Integer((i % 7 + 1) as i64))],
        );
    }
    builder.finalize(None)
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
fn neighbors_scan(graph: GraphSnapshot) -> usize {
    let mut count = 0usize;
    for i in 0..N as u32 {
        count += black_box(graph.neighbors(black_box(i), Direction::Outgoing)).len();
    }
    black_box(count)
}

// Read path: scan type-filtered neighbors of every node.
#[library_benchmark]
#[bench::ring(args = (N), setup = build_ring_graph)]
fn neighbors_by_type_scan(graph: GraphSnapshot) -> usize {
    let mut count = 0usize;
    for i in 0..N as u32 {
        count +=
            black_box(graph.neighbors_by_type(black_box(i), Direction::Outgoing, &["KNOWS"])).len();
    }
    black_box(count)
}

// Read path: enumerate relationship refs (neighbor + type + pos) of every node.
#[library_benchmark]
#[bench::ring(args = (N), setup = build_ring_graph)]
fn relationships_scan(graph: GraphSnapshot) -> usize {
    let mut count = 0usize;
    for i in 0..N as u32 {
        count += black_box(graph.relationships(black_box(i), Direction::Outgoing, &[])).len();
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

// Pathfinding: single-source Dijkstra with a derived (constant) weight.
#[library_benchmark]
#[bench::ring(args = (N), setup = build_ring_graph)]
fn dijkstra_unweighted(graph: GraphSnapshot) -> usize {
    let paths = graph.dijkstra(black_box(0), Direction::Outgoing, &[], None, |_, _| 1.0);
    black_box(paths.reached(black_box(N as u32 - 1)) as usize)
}

// Pathfinding: single-source Dijkstra reading a per-relationship `weight`
// property through the weight closure (the relationship_property hot path).
#[library_benchmark]
#[bench::ring(args = (N), setup = build_weighted_ring_graph)]
fn dijkstra_weighted(graph: GraphSnapshot) -> usize {
    let paths = graph.dijkstra(
        black_box(0),
        Direction::Outgoing,
        &[],
        None,
        |_, rel| match graph.relationship_property(rel.pos, "weight") {
            Some(ValueId::I64(w)) => w as f64,
            _ => 1.0,
        },
    );
    black_box(paths.reached(black_box(N as u32 - 1)) as usize)
}

library_benchmark_group!(
    name = builder;
    benchmarks = build_graph, finalize
);
library_benchmark_group!(
    name = queries;
    benchmarks = neighbors_scan, neighbors_by_type_scan, relationships_scan, bfs_traversal
);
library_benchmark_group!(
    name = pathfinding;
    benchmarks = dijkstra_unweighted, dijkstra_weighted
);
main!(library_benchmark_groups = builder, queries, pathfinding);
