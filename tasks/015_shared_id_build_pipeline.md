# Shared-ID build pipeline (roaringrange + RCPG from one corpus)

The search-then-traverse browser flow needs both artifact families built
over ONE rank-ordered u32 ID space: doc IDs (roaringrange index/records)
== node IDs (RCPG graph). Build a pipeline that:
- assigns IDs in descending static rank (citations etc.)
- emits the roaringrange index family AND graph.rcpg (topology-only) +
  records.{idx,bin} from the same corpus
- demo candidate: an OpenAlex slice (one topic's citation neighborhood or
  top-N-cited works) — full 1.8B-edge graph exceeds the resident wasm
  budget (~tens of millions of rels)

Open design point from 2026-06-11 discussion: relationship-property
filters have no record-store equivalent (records are node-keyed); options
are opt-in rel columns (often small) or a rel-records store keyed by
outgoing-CSR position.
