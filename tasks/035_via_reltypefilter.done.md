# 035 — fold_via/co_occurring/neighbor_groups take impl RelTypeFilter (core)

## Finding
These took `rel: &str` while every other traversal takes `impl RelTypeFilter`
(accepts `&str` OR a pre-resolved `RelationshipType`). They re-resolved the type
on every node via `neighbors_by_type` (which calls `into_match` per call).

## Done
- Widened all three core signatures to `rel_type: impl RelTypeFilter` (folds in
  034's core param rename → `rel_type` everywhere).
- Resolve the filter **once** to a `RelMatch` (`rel_type.into_match(self)`), then
  drive the hot loops with the pre-resolved `neighbors_with(node, dir,
  matcher.clone())` (the same path `roots_via`/`bfs_distances` use) instead of
  `neighbors_by_type` re-resolving per node.
- `neighbor_groups` now stores a resolved `RelMatch` in the `NeighborGroups`
  struct (was `rel: &'a str`), resolved at construction.
- Unknown type → matches nothing → empty result (same as the old early-return).
- Python wrappers pass `rel_type.as_str()` (a generic bound doesn't deref-coerce
  `&String`); Python surface unchanged.
- Docstrings updated `rel` → `rel_type`.

## Perf (before/after, release, 300k nodes / 900k rels; noisy machine)
| kernel            | before      | after      |
|-------------------|-------------|------------|
| fold_via x5       | 104-126 ms  | 57-66 ms   |
| co_occurring x2k  | 2.6-2.9 ms  | 1.7-2.5 ms |
| neighbor_groups x3| 69-84 ms    | 56-63 ms   |

Identical checksums (4500000 / 36000 / 900000) → behavior unchanged. **No
regression; equal-or-faster** (resolve-once + pre-resolved `neighbors_with`).
Harness: `cargo run --release -p rustychickpeas-core --example perf_via`.

## Verification
- `cargo clippy -p rustychickpeas-core --all-targets -- -D warnings`: clean.
  Core lib: 323 passed.
- `maturin develop`; via/co_occurring/neighbor_groups python tests pass.
