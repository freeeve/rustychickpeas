# 030 — Python analytics: keyword-only args + defaults

Make the whole-graph analytics readable at the call site. Python-only (PyO3
signatures); no core change.

## Finding
Positional-boolean signatures were inscrutable:
`g.pagerank(True, 0.85, 30)`, `g.sssp(0, True, "weight")`, `g.cdlp(False, 2)`,
`g.lcc(False)`.

## Done
Added `#[pyo3(signature = ...)]` defaults so the common case is argument-free and
kwargs read clearly (params stay positional-or-keyword, so existing positional
calls keep working — non-breaking):
- `pagerank(directed=True, damping=0.85, iterations=30)` → `g.pagerank()`
- `cdlp(directed=True, iterations=10, seed=None)` → `g.cdlp()`,
  `g.cdlp(directed=False, iterations=2)`
- `lcc(directed=True)` → `g.lcc()`
- `sssp(source, directed=True, weight_key=None)` → `g.sssp(0)`,
  `g.sssp(0, weight_key="...")`
- `wcc()` already argument-free.
Docstrings note the defaults. The bool is already named `directed` (matches Rust).

## Not done (deliberate)
Did NOT force keyword-only (`*`) — that would break shipped 0.9.0 positional
calls (an API break needing sign-off). Enforcing keyword-only is a separate
decision if desired.

## Verification
`maturin develop`; test_analytics.py — 8 passed, incl. new
`test_analytics_defaults_and_kwargs` exercising the zero-arg + kwarg forms and
confirming positional calls still pass.
