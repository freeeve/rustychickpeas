# RCPG — RustyChickpeas graph file (v1)

A `GraphSnapshot` serialized into independently locatable sections, designed
for the split-residency reader model: a traversal reader downloads this file
wholesale (or skips the property sections) and keeps it in memory, while
per-node records live in a separate range-fetched RRSR record store (see
[roaringrange RECORDS.md](https://github.com/freeeve/roaringrange/blob/main/RECORDS.md)).

All integers are **little-endian**. Node IDs are u32 over one rank-ordered ID
space shared with any companion RRSR/roaringrange files: assign IDs in
descending static rank so low IDs are the most important nodes.

## Layout

```
header (16 B):
  magic     char[4]  "RCPG"
  version   u16      1
  flags     u16      0 (reserved)
  sections  u32      number of directory entries
  reserved  u32      0

directory (sections x 24 B):
  id        u32      section identifier (below)
  reserved  u32      0
  offset    u64      absolute byte offset of the section body
  length    u64      section body length in bytes

section bodies follow at the directory offsets.
```

Readers MUST ignore directory entries with unknown section IDs (forward
compatibility) and MUST reject files whose `version` exceeds what they
support. A reader interested only in topology can fetch the directory, then
read just the sections it wants by offset.

## Sections

| id | name | contents |
|----|------|----------|
| 1 | atoms | string table |
| 2 | meta | optional snapshot version string |
| 3 | nodes | node count + label index |
| 4 | relationships | CSR adjacency + type index |
| 5 | node columns | node property columns |
| 6 | relationship columns | relationship property columns |

Common encodings:

- `string`: `len u32` + UTF-8 bytes.
- `u32vec`: `len u32` + len × u32.
- `bitmap`: `atom u32` + `len u32` + portable RoaringBitmap serialization
  (interoperable with the other roaring implementations).
- `bitmap index`: `count u32` + count × bitmap, sorted by atom.

### 1: atoms

`count u32` + count × `string`. Atom 0 is always the empty string. Labels,
relationship types, property keys, and string property values elsewhere in
the file are indices into this table.

### 2: meta

`present u8` (0/1); if 1, `version string`.

### 3: nodes

`n_nodes u32` (actual node count) + label `bitmap index` (label atom →
node IDs).

Note: `n_nodes` is the count of distinct nodes, NOT the CSR ID-space size.
With sparse node IDs the ID space (`len(out_offsets) - 1`) exceeds
`n_nodes`; absent IDs simply have empty adjacency ranges.

### 4: relationships

```
n_rels       u64
out_offsets  u32vec   (csr_id_space + 1 entries)
out_nbrs     u32vec   (n_rels entries, destination node IDs)
out_types    u32vec   (n_rels entries, type atoms, parallel to out_nbrs)
in_offsets   u32vec
in_nbrs      u32vec   (source node IDs)
in_types     u32vec
type index   bitmap index  (type atom -> outgoing-CSR positions)
```

Neighbors of node `v`: `out_nbrs[out_offsets[v] .. out_offsets[v+1]]`.

### 5 / 6: property columns

`count u32`, then per column: `key u32` (atom) + `tag u8` + payload:

| tag | column | payload |
|-----|--------|---------|
| 0 | dense i64 | `len u32` + len × i64 |
| 1 | dense f64 | `len u32` + len × u64 (IEEE-754 bits) |
| 2 | dense bool | `bits u32` + ceil(bits/8) packed bytes, LSB-first |
| 3 | dense str | `u32vec` of value atoms |
| 4 | sparse i64 | `len u32` + len × (id u32 + i64), sorted by id |
| 5 | sparse f64 | `len u32` + len × (id u32 + u64 bits), sorted by id |
| 6 | sparse bool | `len u32` + len × (id u32 + u8), sorted by id |
| 7 | sparse str | `len u32` + len × (id u32 + atom u32), sorted by id |

Dense columns are indexed by node ID (section 5) or outgoing-CSR position
(section 6). Relationship columns index by the relationship's position in
the outgoing CSR arrays.

## RRSR record stores

Per-node records are not part of RCPG; write them as an RRSR store
(`rrsr` module in this crate, byte-compatible with roaringrange) keyed by
the same node IDs. This crate implements RRSR version 1 (raw payloads) and
range planning for batched reads; version 2 (zstd shared-dictionary
framing) is provided by roaringrange's readers.
