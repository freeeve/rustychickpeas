# Fuzz RCPG parse against arbitrary + mutated bytes

The format tests covered round-trip (fixed + proptest-random) and
truncation (every prefix -> Err, no panic), but not arbitrary corruption —
bytes that are the right length but wrong content (bad offsets, oversized
length fields, invalid tags, bit flips). The reader documents "never panics
on malformed input", which was only partly verified.

Added two proptest properties to rustychickpeas-format/tests/format_tests.rs:
- `rcpg_arbitrary_bytes_never_panic` — random Vec<u8> -> parse, no panic.
- `rcpg_mutation_never_panic` — a valid RCPG with corrupted bytes -> parse,
  no panic.

Both pass; no panic found (the reader's all-`Result` design holds). A full
cargo-fuzz harness (coverage-guided) remains a possible follow-up for
deeper, out-of-band fuzzing.
