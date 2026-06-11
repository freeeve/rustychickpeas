# Remove panics from production code paths

- `bitmap.rs:493/517/541` — `panic!("Expected Roaring for large result")`
- `interner.rs:91` — panic on Arc extraction failure in `into_vec()`
- `interner.rs:52-54` — `resolve()` panics on invalid intern IDs
- 4 type-mismatch `panic!`s in graph_builder_parquet.rs
- Lock poisoning: `.unwrap()` on RwLock/Mutex in rusty_chickpeas.rs,
  interner.rs, graph_snapshot.rs (core), and 18 `.lock().unwrap()` in
  rustychickpeas-python/src/graph_snapshot.rs BFS error cells (can abort
  the Python interpreter after a callback panic)

Fix: convert to Result where reachable from user input; use
`unwrap_or_else(|e| e.into_inner())` for poisoned locks.
