# 026 — Make `neighbor_counts` fast + Python-exposed (hidden scratch)

Status: **done** — (1) fast internals + (2) Python exposure shipped; variants
skipped after review. The one carve-out — unifying SPB's `target_counts` onto
`neighbor_counts` — is blocked on the concurrent SPB session that owns those
files; tracked separately, not part of this task's completable scope.

Variants **skipped after review** (weak fit): `neighbor_counts_where` is Rust-only
(closures don't cross PyO3), redundant for source/target-only filters, and doesn't
even fit IC5 (filter depends on the member, not the post→forum edge); a core
top-k must pick one tiebreak, but real queries tiebreak by name/property and would
take the full map anyway. Add either only when a concrete consumer needs it.
SPB `target_counts` unification still open (coordinate with that session).

## Done this pass
- `neighbor_counts` now counts into a hidden thread-local generation-stamped dense
  buffer (no per-target hashing, no HashMap growth, no O(n) clear, no contention);
  rel-type resolution hoisted out of the source loop. Signature unchanged;
  unit-tested incl. scratch-reuse correctness.
- Exposed to Python: `g.neighbor_counts(sources, direction, rel_type) -> dict`
  (core's hashbrown map collected to a std map so PyO3 yields a dict). Smoke-tested.
- Incidental fix: the Python `Column` match now covers the rank/select variants
  (`RankI64/F64/Bool/Str`) — the Python crate isn't built in the LDBC path, so the
  non-exhaustive match from the rank/select work had gone unnoticed.

## Reality check

The primitive already exists — `GraphSnapshot::neighbor_counts`:

```rust
pub fn neighbor_counts(
    &self,
    sources: impl IntoIterator<Item = NodeId>,
    direction: Direction,
    rel_type: &str,
) -> HashMap<NodeId, usize>   // for each source, count its rel_type neighbours by target
```

So we **don't add a new method** (no `count_by_target`/`target_counts`). The job
is to (1) make `neighbor_counts` fast, (2) expose it to Python, (3) unify the
parallel SPB `target_counts` onto it, and (4) add any variants under the same
naming convention.

## 1. Fast internals (no API change)

`neighbor_counts` currently grows a `HashMap` (per-target hashing + doubling
reallocs). Swap the body to a **hidden, thread-local generation-stamped dense
scratch** — the same trick `bfs_distances` uses internally:

```rust
struct CountScratch { val: Vec<u32>, gen: Vec<u32>, cur: u32, touched: Vec<u32> }
// borrow: cur += 1 (no O(n) clear; zero `gen` once on u32 wrap)
// per edge: if gen[t]!=cur { gen[t]=cur; val[t]=0; touched.push(t) } val[t]+=1
// drain: HashMap::with_capacity(touched.len()); for &t in &touched { out.insert(t, val[t]) }
thread_local! { static COUNT_SCRATCH: RefCell<CountScratch> = ... }   // private, never pub
```

- Thread-local ⇒ no shared state, **no data races, no lock contention** (not a
  `Mutex` on the snapshot — that regressed `chain_root`). Reused across calls.
- Signature unchanged; the scratch is invisible to every caller (Rust or Python).
- Net: no per-call hashing in the loop, pre-sized result, ~0 growth allocs.

## 2. Python exposure (the actual "Python benefits" deliverable)

Add a PyO3 wrapper so the whole loop runs in Rust on one FFI call:

```python
counts: dict[int, int] = g.neighbor_counts(sources, Direction.Incoming, "hasMember")
```
- `sources`: a Python iterable of node ids (and/or a `NodeSet` wrapper).
- Returns `dict[int, int]`. (A raw scratch could never help Python — per-op FFI
  cost dominates; a whole-loop method is the only thing that does.)

## 3. Unify with the parallel SPB `target_counts`

The SPB session is adding `g.target_counts(sources, dir, rel_type)` — the **same**
aggregation under a different name. Land **one** method (`neighbor_counts`) and
point SPB's callers at it, rather than shipping two competing primitives.

## 4. Variants — same naming convention (`neighbor_*`)

Only if needed, and named to match `neighbor_counts` / `neighbors_by_type`:

- `neighbor_counts_where(sources, dir, rel_type, keep: impl Fn(src, tgt)->bool)` —
  Rust-only per-edge filter (covers IC5's per-member qualifying-forum set). Not
  Python-exposed (closures don't cross FFI).
- a top-k form to avoid materializing the whole map for "top-N" queries — naming
  TBD under the convention (e.g. `neighbor_counts_top` returning a sorted `Vec`).

## Open decisions for sign-off

1. **Scope now:** just (1) fast internals + (2) Python exposure of `neighbor_counts`?
   Or also the `_where` / top-k variants in the same pass?
2. **Count unit:** keep `usize` multiplicity (every matching edge) as today; add a
   distinct-source variant only if a query needs it.
3. **`u32` vs `usize`** in the result — keep the existing `usize` for source-compat.
4. **SPB coordination:** confirm `target_counts` folds into `neighbor_counts`
   (same semantics) before either lands, to avoid a duplicate core method.

## Out of scope (stays query-side)

IC4's "tally tags in a date window minus the before-window set" — too bespoke
(date filter + set subtraction) for a general core method.
