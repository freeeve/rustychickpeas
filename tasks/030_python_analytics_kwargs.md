# 030 — Python analytics: keyword-only args + defaults

Make the whole-graph analytics readable at the call site. Python-only (PyO3
signatures); no core change, non-breaking where defaults match today's args.

## Finding
Positional-boolean signatures are inscrutable:
`g.pagerank(True, 0.85, 30)`, `g.sssp(0, True, "weight")`, `g.cdlp(False, 2)`,
`g.lcc(False)` (py/graph_snapshot.rs:1291/1311/1329/1339).

## Proposed change
`#[pyo3(signature = (..., *, directed=True, damping=0.85, iterations=30))]` etc.,
naming the bool `directed` (already the Rust param). Targets: pagerank, sssp
(`weight_key` kw), cdlp (`iterations`/`seed` kw), lcc, wcc. Result:
`g.pagerank()`, `g.cdlp(directed=False, iterations=2)`,
`g.sssp(0, weight_key="weight")`.

## Sign-off: not required (Python binding ergonomics only).

## Verification
- Update test_analytics.py call sites to kwargs; pytest green.
- Positional calls still work where args map positionally.
