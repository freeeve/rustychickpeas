# Typed dense-column slice accessors (LDBC opportunity A) — DONE

Core `Column::as_i64_slice/as_f64_slice/as_bool_slice/as_str_ids`
(commit 69439d4) were already wired into Q1 and Q12 (commit 846b026).
Remaining call sites now wired in `rustychickpeas-ldbc`:

- Q9 `q9_thread_initiators` (faithful_a.rs) — hoist the `day` column once;
  the reply-tree DFS indexes the dense slice per node instead of `pi64`
  per node. plid sort hoisted too.
- Q2 `q2_tag_evolution` (faithful_a.rs) — the two-window `day` scan now
  indexes the dense slice (was `col_i64` per message).
- plid sort-keys — hoist the plid column once and compare via `col_i64`
  instead of `pi64` (double hash) per comparison: Q5/Q6/Q8/Q9 (faithful_a),
  Q13/Q14/Q18/Q19/Q20 (faithful_b), Q10/Q16 (faithful_c). Q14 also uses the
  hoisted column for its hot candidate-selection tiebreak. (Q4 already
  precomputes its forum-ranking keys; left as-is.)
- Edge columns — Q11 `kd` (knows creationDate, faithful_a) and Q20 `cy`
  (studyAt classYear, in `build_studyat`, faithful_b) read via a hoisted
  `rel_columns` ref indexed by CSR position, instead of
  `relationship_property` (which re-resolves the key) per edge.

Verification: full SF1 BI suite re-emitted via `LDBC_EMIT_JSON`; all 23
result JSON files byte-identical to the pre-change baseline (0 differ).
