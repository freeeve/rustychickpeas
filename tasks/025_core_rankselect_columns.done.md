# 025 — Rank/select column for moderately-sparse properties (core perf)

Sparse property columns (`SparseI64` = sorted `Vec<(pos,val)>`) are read by binary
search — `O(log n)` with ~21 cache-missing accesses for SF1's `hd`/`kd`/`cy`/`ld`
edge columns. Add a rank/select `Column` variant for the moderately-sparse band
that is both **smaller AND O(1)**:

```
RankI64 { present: BitVec, block_rank: Vec<u32>, values: Vec<i64> }
get(pos) = if present[pos] { values[ block_rank[pos/B] + popcount(present[B*(pos/B)..pos]) ] }
```

Memory: rank/select beats the pair-array whenever fill > ~1.5% (`m/8 + 8·present`
< `16·present` ⇔ `present·64 > m`), so it's a pure win in that band, not a
tradeoff.

Heuristic (`build_rel_columns`, i64 first):
- fill ≥ 80%                              → `DenseI64`  (today)
- else m > 1M rels AND `len·64 ≥ m`       → `RankI64`   (new)
- else                                    → `SparseI64` (today)

Touches: `Column` enum + `get` + `iter_entries` (graph_snapshot.rs),
`build_rel_columns` (graph_builder.rs), `serialize::column_to_data`
(`RankI64` → `SparseI64`, on-disk format unchanged; RCPG read stays Sparse).
i64 first (covers `hd`/`kd`/`cy`/`ld`/`wf` → IC5/Q11/Q20/IC7); f64/bool/str and
node columns are follow-ups.

Roaring note: Roaring presence compresses but its `rank()` recomputes per call;
`bitvec` + a block-rank index gives clean O(1). Roaring-presence is a follow-up
for very-sparse (<1.5%) columns.

Acceptance: core unit tests pass; SF1 BI **value-identical** vs the pre-change
baseline (Q11 reads `kd`, Q20 reads `cy`); BI timings no regression; IC5/Q11/Q20
measurably faster. Sign-off received on the `RankI64` Column shape.

## Results (verified 2026-06-16)

- Core unit tests: 298 pass (+2 rank/select parity tests, incl. cross-block).
- Lib clippy clean; my edits fmt-clean (pre-existing fmt drift elsewhere in the
  core is unrelated, from earlier commits).
- **Value-identity (back-to-back A/B, core stashed vs restored):**
  - SF1 BI: 23/23 emit files **byte-identical**.
  - SF1 IC: 21/21 emit files **byte-identical** (IC5=`hd`, IC7=`ld`, IC14=interaction).
- **Timings (same load window):**
  - IC5 (reads `hd`): ~495 ms → ~270 ms ≈ **1.85× faster** — flips IC5 from a
    Kùzu win (434 ms) to a chickpeas win.
  - IC7 (`ld`): ~1.52 → ~1.27 ms. IC14: unchanged (weight read via map, not column).
  - BI: no regression (noise-level; edge-prop readers Q11 4.0→3.3, Q19 6.4→5.8 a bit faster).

### Extension (f64/bool/str + node columns)

- Added `RankF64`/`RankBool`/`RankStr` variants; routed both edge and node column
  construction through shared `column_from_pairs_{i64,f64,bool,str}` helpers that
  pick dense / rank-select / sparse by fill and span (per-type memory crossover:
  i64/f64 ~1/64, str ~1/32, bool ~1/63). 299 core tests pass (+f64/bool/str parity).
- Re-verified vs the **original** pre-change baselines: SF1 BI 23/23 and IC 21/21
  **byte-identical**. IC5 holds at ~273 ms (i64 win preserved through the refactor).
- BI: no regression; several node-prop-heavy queries faster (Q12 55.6→37.3,
  Q6 130→94, Q4 249→212, BI4 66→34 ms) — node columns now O(1) instead of
  binary search.

### Follow-up: Roaring-backed presence for very-sparse columns — DECLINED

Measured before building (`examples/roaring_presence_eval.rs`, N=2.9M, full i64
column bytes + rank latency over 1M lookups). In the <1.56% range these columns
target — where the column is **SparseI64 (binary search)** today — Roaring
presence is **never a speedup**: its `rank` is ~46 ns flat (container scan),
losing to sparse's 9–19 ns binary search *and* to bitvec rank/select's ~9 ns.

| fill | roaring B | sparse B | roaring ns | sparse ns |
|------|-----------|----------|------------|-----------|
| 0.1% | 29 KB     | 46 KB    | 46.1       | **8.6**   |
| 1.0% | 290 KB    | 464 KB   | 47.9       | **16.3**  |
| 1.5% | 435 KB    | 696 KB   | 49.3       | **19.3**  |

Its only edge is ~1.6× less memory than sparse — a modest absolute saving
(~174 KB/column at 1% fill) for a ~3× slower read. The current frontier is
already optimal: **SparseI64 below ~1.56% fill** (compact + fast), **bitvec
rank/select above** (O(1) rank). Roaring presence sits inside that frontier
(dominated on speed). Declined; the eval example stays as the evidence.
