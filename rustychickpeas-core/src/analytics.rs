//! Whole-graph analytics over a [`GraphSnapshot`]: PageRank, weakly-connected
//! components, community detection by label propagation (CDLP), the local
//! clustering coefficient (LCC), and single-source shortest paths (SSSP). These are
//! the canonical graph algorithms graph engines expose (Neo4j GDS, NetworkX), and
//! the read-side complement to the snapshot's traversal kernels (`bfs_distances`,
//! `dijkstra`, `neighbor_counts`). Node-indexed `Vec` outputs (index == node id).
//!
//! `directed` selects the forward direction: `Outgoing` for a directed graph,
//! `Both` for an undirected one (whose rels are stored once). The parallel kernels
//! (PageRank/CDLP/LCC) split node ranges across `GA_THREADS` workers (default: all
//! cores) via an atomic cursor, each writing disjoint output slots.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::graph_snapshot::{GraphSnapshot, PropExt};
use crate::types::{Direction, NodeId};

/// Forward rel direction: outgoing (directed) or both (undirected).
fn fwd(directed: bool) -> Direction {
    if directed {
        Direction::Outgoing
    } else {
        Direction::Both
    }
}

/// In-rel direction: incoming (directed) or both (undirected).
fn ind(directed: bool) -> Direction {
    if directed {
        Direction::Incoming
    } else {
        Direction::Both
    }
}

/// Worker count from `GA_THREADS`, else the machine's parallelism (min 1).
fn workers() -> usize {
    std::env::var("GA_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |p| p.get()))
}

const BATCH: u32 = 2048;

impl GraphSnapshot {
    /// Single-source shortest paths over forward rels, additive edge weights from
    /// the `weight_key` rel property (`None` = unit weights); unreachable nodes get
    /// `f64::INFINITY`. Wraps [`dijkstra`](Self::dijkstra).
    pub fn sssp(&self, source: NodeId, directed: bool, weight_key: Option<&str>) -> Vec<f64> {
        let sp = self.dijkstra(source, fwd(directed), &[] as &[&str], None, |_from, rel| {
            match weight_key {
                Some(k) => self.rel_prop(rel.pos, k).f64_or(1.0),
                None => 1.0,
            }
        });
        (0..self.node_count())
            .map(|v| sp.distance(v).unwrap_or(f64::INFINITY))
            .collect()
    }

    /// Weakly connected components: each node's label is the smallest node id in its
    /// component, found by flooding undirected (`Both`) rels in ascending id sweep
    /// order (so the first-reached node of a component is its minimum).
    pub fn wcc(&self) -> Vec<u32> {
        let n = self.node_count();
        let mut comp = vec![u32::MAX; n as usize];
        let mut stack: Vec<u32> = Vec::new();
        for s in 0..n {
            if comp[s as usize] != u32::MAX {
                continue;
            }
            comp[s as usize] = s;
            stack.push(s);
            while let Some(v) = stack.pop() {
                for u in self.neighbors(v, Direction::Both) {
                    if comp[u as usize] == u32::MAX {
                        comp[u as usize] = s;
                        stack.push(u);
                    }
                }
            }
        }
        comp
    }

    /// PageRank after `iterations` synchronous pull updates with damping `damping`:
    /// `PR0(v) = 1/|V|`, then `PRi(v) = (1-d)/|V| + d*(Σ_{u∈Nin(v)} PRi-1(u)/|Nout(u)|
    /// + Σ_{sink} PRi-1/|V|)`. Sinks (out-degree 0) redistribute their rank uniformly.
    pub fn pagerank(&self, directed: bool, damping: f64, iterations: u32) -> Vec<f64> {
        let n = self.node_count() as usize;
        if n == 0 {
            return Vec::new();
        }
        let nf = n as f64;
        let (out, in_dir) = (fwd(directed), ind(directed));
        let outdeg: Vec<u32> = (0..n as u32)
            .map(|v| self.neighbors(v, out).count() as u32)
            .collect();
        let mut pr = vec![1.0 / nf; n];
        let mut next = vec![0.0_f64; n];
        let nworkers = workers();
        for _ in 0..iterations {
            let dangling: f64 = (0..n).filter(|&v| outdeg[v] == 0).map(|v| pr[v]).sum();
            let base = (1.0 - damping) / nf + damping * dangling / nf;
            let cursor = AtomicU32::new(0);
            let nxt_ptr = next.as_mut_ptr() as usize;
            let (pr_ref, outdeg_ref): (&[f64], &[u32]) = (&pr, &outdeg);
            let cursor = &cursor;
            std::thread::scope(|scope| {
                for _ in 0..nworkers {
                    scope.spawn(move || loop {
                        let start = cursor.fetch_add(BATCH, Ordering::Relaxed);
                        if start as usize >= n {
                            break;
                        }
                        let end = (start as usize + BATCH as usize).min(n) as u32;
                        for v in start..end {
                            let mut pull = 0.0_f64;
                            for u in self.neighbors(v, in_dir) {
                                let d = outdeg_ref[u as usize];
                                if d > 0 {
                                    pull += pr_ref[u as usize] / d as f64;
                                }
                            }
                            // SAFETY: the atomic cursor hands out disjoint node
                            // batches, so the `next` slots never alias; `next`
                            // outlives the scope.
                            unsafe {
                                *(nxt_ptr as *mut f64).add(v as usize) = base + damping * pull;
                            }
                        }
                    });
                }
            });
            std::mem::swap(&mut pr, &mut next);
        }
        pr
    }

    /// Community detection by synchronous label propagation: `L0(v) = v`, then each
    /// node adopts the most frequent label among its neighbours (in+out tallied
    /// separately for directed graphs, so a mutual rel counts twice; each neighbour
    /// once for undirected), smallest label breaking ties; a node with no neighbours
    /// keeps its label. Runs `iterations` rounds.
    pub fn cdlp(&self, directed: bool, iterations: u32) -> Vec<u32> {
        let init: Vec<u32> = (0..self.node_count()).collect();
        self.cdlp_seeded(directed, iterations, &init)
    }

    /// [`cdlp`](Self::cdlp) seeded with explicit initial labels (`init[node]` is
    /// `L0(node)`). Seed with original vertex ids to match a vertex-id-keyed
    /// reference: the smallest-label tie-break then operates in that id space.
    pub fn cdlp_seeded(&self, directed: bool, iterations: u32, init: &[u32]) -> Vec<u32> {
        let n = self.node_count();
        let mut cur: Vec<u32> = init.to_vec();
        let mut nxt: Vec<u32> = vec![0; n as usize];
        let nworkers = workers();
        let mut bufs: Vec<Vec<u32>> = (0..nworkers).map(|_| Vec::new()).collect();
        for _ in 0..iterations {
            let cursor = AtomicU32::new(0);
            let nxt_ptr = nxt.as_mut_ptr() as usize;
            let cur_ref: &[u32] = &cur;
            let cursor = &cursor;
            std::thread::scope(|scope| {
                for buf in bufs.iter_mut() {
                    scope.spawn(move || loop {
                        let start = cursor.fetch_add(BATCH, Ordering::Relaxed);
                        if start >= n {
                            break;
                        }
                        let end = (start + BATCH).min(n);
                        for v in start..end {
                            let label = self.cdlp_label(directed, cur_ref, v, buf);
                            // SAFETY: disjoint node batches -> disjoint `nxt` slots;
                            // `nxt` outlives the scope.
                            unsafe {
                                *(nxt_ptr as *mut u32).add(v as usize) = label;
                            }
                        }
                    });
                }
            });
            std::mem::swap(&mut cur, &mut nxt);
        }
        cur
    }

    /// One synchronous CDLP update for node `v`: gather neighbour labels into the
    /// reused `buf`, then the most frequent (smallest on a tie via a sort + run
    /// scan), defaulting to `cur[v]` when `v` has no neighbours.
    fn cdlp_label(&self, directed: bool, cur: &[u32], v: u32, buf: &mut Vec<u32>) -> u32 {
        buf.clear();
        if directed {
            buf.extend(self.neighbors(v, Direction::Outgoing).map(|u| cur[u as usize]));
            buf.extend(self.neighbors(v, Direction::Incoming).map(|u| cur[u as usize]));
        } else {
            buf.extend(self.neighbors(v, Direction::Both).map(|u| cur[u as usize]));
        }
        buf.sort_unstable();
        let mut best_label = cur[v as usize];
        let mut best_count = 0usize;
        let mut i = 0;
        while i < buf.len() {
            let lab = buf[i];
            let mut j = i + 1;
            while j < buf.len() && buf[j] == lab {
                j += 1;
            }
            if j - i > best_count {
                best_count = j - i;
                best_label = lab;
            }
            i = j;
        }
        best_label
    }

    /// Local clustering coefficient: for each node `v` with undirected neighbour set
    /// `N(v)` (each neighbour once, self excluded), `0` if `|N(v)| <= 1` else the
    /// number of forward rels between members of `N(v)` over `|N(v)|*(|N(v)|-1)`.
    pub fn lcc(&self, directed: bool) -> Vec<f64> {
        let n = self.node_count();
        let out = fwd(directed);

        // Sorted forward-adjacency (CSR), built once, so the triangle count probes
        // the smaller side of each intersection.
        let mut off = vec![0u32; n as usize + 1];
        for v in 0..n {
            off[v as usize + 1] = off[v as usize] + self.neighbors(v, out).count() as u32;
        }
        let mut adj = vec![0u32; off[n as usize] as usize];
        for v in 0..n {
            let s = off[v as usize] as usize;
            let mut p = s;
            for w in self.neighbors(v, out) {
                adj[p] = w;
                p += 1;
            }
            adj[s..p].sort_unstable();
        }

        let mut result = vec![0.0_f64; n as usize];
        let nworkers = workers();
        let cursor = AtomicU32::new(0);
        let rptr = result.as_mut_ptr() as usize;
        let (off, adj, cursor) = (&off, &adj, &cursor);
        std::thread::scope(|scope| {
            for _ in 0..nworkers {
                scope.spawn(move || {
                    let mut mark = vec![0u64; (n as usize + 63) / 64];
                    let mut nbrs: Vec<u32> = Vec::new();
                    loop {
                        let start = cursor.fetch_add(BATCH, Ordering::Relaxed);
                        if start >= n {
                            break;
                        }
                        let end = (start + BATCH).min(n);
                        // SAFETY: disjoint [start,end) batches -> disjoint slices;
                        // `result` outlives the scope.
                        let slice = unsafe {
                            std::slice::from_raw_parts_mut(
                                (rptr as *mut f64).add(start as usize),
                                (end - start) as usize,
                            )
                        };
                        self.lcc_count_range(off, adj, start, slice, &mut mark, &mut nbrs);
                    }
                });
            }
        });
        result
    }

    /// LCC for nodes `start .. start + result.len()`. `mark` (the N(v) membership
    /// bitset) and `nbrs` are caller-owned scratch reused across batches.
    fn lcc_count_range(
        &self,
        off: &[u32],
        adj: &[u32],
        start: u32,
        result: &mut [f64],
        mark: &mut [u64],
        nbrs: &mut Vec<u32>,
    ) {
        for (i, slot) in result.iter_mut().enumerate() {
            let v = start + i as u32;
            nbrs.clear();
            for u in self.neighbors(v, Direction::Both) {
                let marked = mark[(u >> 6) as usize] >> (u & 63) & 1 != 0;
                if u != v && !marked {
                    mark[(u >> 6) as usize] |= 1u64 << (u & 63);
                    nbrs.push(u);
                }
            }
            let k = nbrs.len();
            if k >= 2 {
                if nbrs
                    .iter()
                    .any(|&u| (off[u as usize + 1] - off[u as usize]) as usize > k)
                {
                    nbrs.sort_unstable();
                }
                let mut rels = 0u64;
                for &u in nbrs.iter() {
                    let uo = &adj[off[u as usize] as usize..off[u as usize + 1] as usize];
                    if uo.len() <= k {
                        for &w in uo {
                            if w != u && mark[(w >> 6) as usize] >> (w & 63) & 1 != 0 {
                                rels += 1;
                            }
                        }
                    } else {
                        let mut cursor = 0;
                        for &w in nbrs.iter() {
                            if w == u {
                                continue;
                            }
                            let (found, pos) = gallop(uo, cursor, w);
                            cursor = pos;
                            if found {
                                rels += 1;
                            }
                        }
                    }
                }
                *slot = rels as f64 / (k as f64 * (k as f64 - 1.0));
            }
            for &u in nbrs.iter() {
                mark[(u >> 6) as usize] &= !(1u64 << (u & 63));
            }
        }
    }
}

/// Galloping (exponential) search for `target` in `uo[from..]`: whether present
/// and the index where it is/would be (always `>= from`); the cursor only advances.
fn gallop(uo: &[u32], from: usize, target: u32) -> (bool, usize) {
    let n = uo.len();
    if from >= n {
        return (false, n);
    }
    let mut bound = 1;
    while from + bound < n && uo[from + bound] < target {
        bound *= 2;
    }
    let lo = from + bound / 2;
    let hi = (from + bound + 1).min(n);
    match uo[lo..hi].binary_search(&target) {
        Ok(i) => (true, lo + i),
        Err(i) => (false, lo + i),
    }
}

#[cfg(test)]
mod tests {
    use crate::GraphBuilder;

    fn build(n: u32, rels: &[(u32, u32)]) -> crate::GraphSnapshot {
        let mut b = GraphBuilder::new(Some(n as usize), Some(rels.len()));
        for i in 0..n {
            b.add_node(Some(i), &["V"]).unwrap();
        }
        for &(u, v) in rels {
            b.add_relationship(u, v, "e").unwrap();
        }
        b.finalize(None)
    }

    fn build_weighted(n: u32, rels: &[(u32, u32, f64)]) -> crate::GraphSnapshot {
        let mut b = GraphBuilder::new(Some(n as usize), Some(rels.len()));
        for i in 0..n {
            b.add_node(Some(i), &["V"]).unwrap();
        }
        for &(u, v, w) in rels {
            b.add_relationship(u, v, "e").unwrap();
            b.set_relationship_prop_f64(u, v, "e", "weight", w);
        }
        b.finalize(None)
    }

    #[test]
    fn sssp_weighted_shortest_and_unreachable() {
        let g = build_weighted(4, &[(0, 1, 2.0), (1, 2, 3.0), (0, 2, 10.0)]);
        let d = g.sssp(0, true, Some("weight"));
        assert_eq!(d[0], 0.0);
        assert_eq!(d[1], 2.0);
        assert_eq!(d[2], 5.0);
        assert_eq!(d[3], f64::INFINITY);
    }

    #[test]
    fn wcc_two_components_label_min_id() {
        let g = build(5, &[(0, 1), (1, 2), (3, 4)]);
        assert_eq!(g.wcc(), vec![0, 0, 0, 3, 3]);
    }

    #[test]
    fn pagerank_uniform_on_directed_cycle() {
        let g = build(3, &[(0, 1), (1, 2), (2, 0)]);
        let pr = g.pagerank(true, 0.85, 30);
        for p in &pr {
            assert!((p - 1.0 / 3.0).abs() < 1e-9, "{p}");
        }
        assert!((pr.iter().sum::<f64>() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn pagerank_redistributes_sink_rank() {
        let g = build(2, &[(0, 1)]);
        let pr = g.pagerank(true, 0.85, 1);
        assert!((pr[0] - 0.2875).abs() < 1e-9, "{}", pr[0]);
        assert!((pr[1] - 0.7125).abs() < 1e-9, "{}", pr[1]);
    }

    #[test]
    fn cdlp_triangle_and_seeded() {
        let g = build(3, &[(0, 1), (1, 2), (2, 0)]);
        assert_eq!(g.cdlp(false, 2), vec![0, 0, 0]);
        assert_eq!(g.cdlp_seeded(false, 2, &[10, 20, 30]), vec![10, 10, 10]);
    }

    #[test]
    fn lcc_triangle_with_pendant_and_gallop() {
        let g = build(4, &[(0, 1), (1, 2), (2, 0), (0, 3)]);
        let c = g.lcc(false);
        assert!((c[0] - 1.0 / 3.0).abs() < 1e-9, "{}", c[0]);
        assert!((c[1] - 1.0).abs() < 1e-9 && (c[2] - 1.0).abs() < 1e-9);
        assert_eq!(c[3], 0.0);

        // High-degree neighbour exercises the gallop branch: LCC(0)=1/(2*1)=0.5.
        let g2 = build(5, &[(0, 1), (0, 2), (1, 2), (1, 3), (1, 4)]);
        assert!((g2.lcc(true)[0] - 0.5).abs() < 1e-9);
    }
}
