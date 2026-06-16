# Expose RRSR record-store write/read in Python

The "consider also" follow-on from task 014, and the next concrete step
toward 015 (the shared-ID build pipeline): a Python loader needs to emit
the RRSR node-record store that pairs with a topology-only RCPG graph.

Added module functions (src/rrsr.rs, registered in lib.rs):
- `write_rrsr(idx_path, bin_path, records)` — records is any iterable of
  `bytes`; record `i` is addressed by id `i` (pass in node-ID order).
- `read_rrsr(idx_path, bin_path) -> list[bytes]` — read records back (for
  round-tripping/verification; resident traversal uses the reader crate's
  ranged fetches).

Gotcha: `Vec<u8>` maps to a Python `list[int]`, so `read_rrsr` returns
`PyBytes` to get real `bytes`. Round-trip + empty-store + missing-file
tests (196 pass).

Remaining toward 015: a loader script that assigns descending-rank IDs and
emits the roaringrange index family + graph.rcpg (topology-only) +
records.{idx,bin} from one corpus (OpenAlex slice demo). The open design
point (rel-property filters vs node-keyed records) is still unresolved.
