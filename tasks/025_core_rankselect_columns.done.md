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

Done for i64 rel columns. Follow-ups (separate tasks): f64/bool/str rel columns,
node columns, and a Roaring-backed presence for very-sparse (<1.5%) columns.
