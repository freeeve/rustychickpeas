# chain_root lazy forest-root index (LDBC opportunity B) — DONE

Added core forest-root primitive. Initial sign-off was a per-node lazy
`Mutex` memo; that **regressed** the hot callers badly (Q15 25ms -> 559ms
from `Mutex` contention inside a `par_fold` over ~2M comments; Q12
108ms -> 160ms from per-call lock + path-`Vec` alloc). Re-signed-off and
shipped as an **eager, lock-free** index instead:

```rust
pub fn chain_roots(&self, direction: Direction, rel_type: RelationshipType)
    -> Arc<[NodeId]>     // root[node] = terminal of node's chain; built once
pub fn chain_root(&self, node, direction, rel_type) -> NodeId   // convenience
```

`chain_roots` builds the whole per-node terminal array once per
`(direction, rel_type)` in O(node_count) (path compression, node_count
depth cap for malformed cycles), caches it in
`Mutex<HashMap<(Direction, RelationshipType), Arc<[NodeId]>>>`, and hands
back an `Arc<[NodeId]>` the hot loop indexes lock-free. `Direction` gained
`Hash`. Core tests: `test_chain_root_walks_reply_forest_to_root` (also
exercises `chain_roots`), `test_chain_root_terminates_on_cycle`.

Wired (each grabs the array once, then indexes per row):
- Q12 `root_lang` (faithful_a) — index for the root-post language.
- Q15 (faithful_c) — replaced the per-worker `FastMap` root cache; the
  `par_fold` indexes the shared array lock-free.
- Q17 `forum_of` (faithful_c) — removed the hand-rolled walk + cache.

All three use `(Outgoing, replyOf)`, so the array is built once and reused
across them and the harness's 5x repeats. ~40 duplicated lines removed.
Value-identical (all 23 SF1 result files byte-identical to baseline);
Q12/Q15 back to/under their original timings (see commit / timing log).
