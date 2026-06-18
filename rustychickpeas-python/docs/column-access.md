# Python columnar access — design proposal

Status: **proposed (awaiting sign-off on the core additions)**

## Motivation

Bulk column reads are the hot path for analytic Python workloads (the LDBC BI
port is the driving example: Q1 went from a ~1.5 s Python loop to ~30 ms once the
columns were exported in bulk and aggregated with pyarrow).

The current Python surface for this is ad-hoc and drifting from core:

- `g.i64_column(key) -> list[int]` — materializes millions of `PyLong`s.
- `g.i64_column_bytes(key) -> bytes` — added for the Q1 optimization; flat
  `i64_`-prefixed name, only covers `i64`.

Two problems:

1. **Not Pythonic.** Mirroring Rust's `g.col(key).i64()` would push a
   static-typing artifact (the caller asserting the element type) into a dynamic
   language. Python data tools (pandas/polars/pyarrow/numpy) hand back a
   *self-describing* typed array; the dtype is intrinsic and you *convert*
   (`.to_numpy()`, `.to_pylist()`), you don't narrow.
2. **Incomplete.** Only `i64` is reachable in bulk. `f64`, `bool`, and `string`
   columns have no fast path, even though core stores all of them densely.

## Proposed Python API

A single self-describing `Column` object, obtained once per key:

```python
col = g.column("day")          # -> Column | None  (None if key absent)
col.dtype                      # 'int64' | 'float64' | 'bool' | 'string'
len(col)                       # node_count (one slot per node id)

# zero-copy interop via the buffer protocol — no library lock-in:
import numpy as np;  np.asarray(col)            # int64 ndarray, zero-copy
import pyarrow as pa; pa.py_buffer(col)         # wrap the same memory
memoryview(col)                                 # stdlib, zero-copy

col.to_pylist()                # convenience (list[int|float|bool|str])
```

String columns are dictionary-encoded in core (interned ids). v1 exposes them as
their integer **codes** plus a resolver, which is exactly what equality / set
filters need and stays zero-copy:

```python
lang = g.column("lang")        # dtype 'string'
lang.dtype                     # 'string'
codes = np.asarray(lang)       # uint32 interned codes, zero-copy
ar, hu = g.string_id("ar"), g.string_id("hu")   # target strings -> codes
mask = np.isin(codes, [ar, hu])                 # vectorized, no per-row resolve
lang.to_pylist()               # resolves codes -> actual strings (when needed)
```

This is how BI Q12's `lang ∈ {ar, hu}` filter becomes a vectorized op instead of
a per-node `prop_str`.

### Semantics

- **One slot per node id.** A `Column` is dense over `[0, node_count)`; index by
  node id directly. Absent values read as the type default (`0` / `0.0` /
  `false` / `""`), matching today's `i64_column` and the Rust query readers
  (`i64_or_zero`). v1 does **not** carry an Arrow null bitmap — dense columns
  don't track per-node presence, so a null mask can't be produced cheaply.
- **Dense fast path only in v1.** Sparse/rank columns (rare; large + mostly
  absent) return `None` from `g.column(key)` in v1's zero-copy path; callers fall
  back to per-node `get_property`. (A later version can materialize them.)
- **Lifetime-safe.** `Column` holds an `Arc<GraphSnapshot>`; the buffer it
  exposes points into the snapshot's immutable storage (zero-copy, no dangling).

## Core additions (need sign-off)

All four dense representations and their slice accessors already exist on
`Column` (`as_i64_slice`, `as_f64_slice`, `as_bool_slice`, `as_str_ids`); they
are simply not reachable through the public `Col` reader, which only narrows to
`.i64()` / `.bool()`. The additions complete that reader symmetrically:

1. **`Col::f64(self) -> F64Col<'a>`** + `pub struct F64Col` with
   `get(pos) -> Option<f64>` and `as_slice() -> Option<&'a [f64]>`.
   Exact mirror of the existing `I64Col`; backed by `Column::as_f64_slice`.

2. **`Col::str(self) -> StrCol<'a>`** + `pub struct StrCol` with
   `get(pos) -> Option<&'a str>` (resolve via the snapshot string table) and
   `as_ids(self) -> Option<&'a [u32]>` (the dense interned codes; backed by
   `Column::as_str_ids`).

3. **`Col::dtype(self) -> ColumnDtype`** where
   `pub enum ColumnDtype { I64, F64, Bool, Str }` — lets the binding pick the
   right typed reader/buffer without probing each `as_*_slice`, and reports the
   dtype even for non-dense columns.

These are query-side reader conveniences over storage that already exists; no
storage-format or builder change. Naming follows the established
`Col::i64()` / `Col::bool()` / `I64Col` / `BoolCol` pattern.

(Deferred to v2, only if we add the Arrow path below:
`GraphSnapshot::string_table(&self) -> &[String]` to back a real Arrow
`DictionaryArray`. Not needed for v1 code-based filtering, which uses the
existing `resolve_string` / `get_string_id`.)

## Binding additions (no sign-off needed)

- `GraphSnapshot.column(key) -> Column | None`.
- `Column` pyclass holding `Arc<CoreGraphSnapshot>`:
  - `dtype: str`, `__len__`, `to_pylist()`.
  - **Buffer protocol** (`__getbuffer__`) exposing the typed dense buffer:
    `int64` → format `q`, `float64` → `d`, `string` → `I` (uint32 codes).
    `bool` is bit-packed in core, so it is expanded to a `uint8` 0/1 buffer in
    one O(n) pass (documented as a copy; bool columns are small).
- Replace `i64_column` / `i64_column_bytes` (both local-only, unreleased) with
  `column(key)`; keep thin deprecated aliases for one release if desired, else
  remove outright since there are no published users.

## Dependencies & phasing

- **v1 — buffer protocol only. No new Rust dependency.** Covers numeric columns,
  bool, and string-as-codes filtering — i.e. the real LDBC needs (Q1 numeric, Q12
  `lang` filter). numpy/pyarrow stay optional; the extension just exposes memory.
- **v2 — Arrow PyCapsule Interface (`__arrow_c_array__`).** First-class zero-copy
  for `pa.array(col)` / `pl.Series(col)` / pandas / duckdb, including a proper
  string `DictionaryArray` (codes + shared table) and null bitmaps. Cost: add
  `arrow` (with `ffi`) as a direct dep of the python crate — already in the
  workspace lock (core uses arrow 57), so version-aligned, but a new direct dep,
  hence gated behind explicit demand. Adds the v2 core item `string_table()`.

## Suggested plan

1. Sign-off on the three v1 core additions (`Col::f64`, `Col::str`, `Col::dtype`).
2. Implement them in core (+ unit tests mirroring the `I64Col` tests).
3. Implement the `Column` pyclass + `GraphSnapshot.column`; migrate `i64_column*`.
4. Port the Python LDBC columns helper to `g.column(key)`; add Q12 using the
   string-code filter as the first consumer + pytest.
5. Revisit v2 (Arrow capsule) once v1 is in use and we know we want cross-library
   zero-copy / dictionary strings.
```
