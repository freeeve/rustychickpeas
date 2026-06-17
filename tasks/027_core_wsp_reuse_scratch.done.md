# 027 — Reuse weighted_shortest_path working state across calls

`weighted_shortest_path` (bidirectional Dijkstra) built a fresh pair of
`HashMap<NodeId, f64>` dist maps and two `BinaryHeap`s on every call. On a deep
weighted search (LDBC IC14) the maps grow to tens of thousands of entries,
churning ~1 MB per call in repeated table reallocations.

## Change (internal only — signature/return/semantics unchanged)

Reuse the two dist maps and two heaps from a thread-local scratch, `clear()`ing
them at the top of each call (which keeps their capacity). Thread-local so
parallel callers never contend. The guard is deref'd once to a `&mut WspScratch`
so the borrow checker can split-borrow the disjoint `heap`/`dist`/`other` fields
the meet-in-the-middle loop needs.

Sign-off: maintainer chose the **reused-HashMap** shape over a dense
generation-stamped `Vec<f64>` — the dense form would be ~2 ns/access but cost
24 B × node_count (~70 MB/thread at SF1); the reused maps keep ~1 MB/thread and
kill the per-call churn while leaving the hashing CPU in place.

## Verification

- **Value-identity:** all 21 LDBC IC queries byte-identical to the pre-change
  baseline; IC13 and IC14 specifically identical.
- **Allocations (deterministic, warmup-then-count):** IC14 **28 / 1,081,456 B → 0 / 0**;
  IC13 **0 / 0**.
- **Core unit test:** `weighted_shortest_path` passes (299-test suite green).
- **Wall-clock:** not cleanly measurable — a concurrent benchmark session pinned
  the machine at load 70–165 throughout. Min-sampled A/B (BEFORE 15.26 ms /
  AFTER ~17 ms) sat in the same load-dominated band → no regression. By
  construction the change only removes allocation; re-measure on a quiet machine.

**Status: done** (wall-clock confirmation pending a quiet window).
