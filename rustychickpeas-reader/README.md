# rustychickpeas-reader

Split-residency graph reader for RCPG snapshots (see
`rustychickpeas-format/FORMAT.md`):

- **Resident**: the graph file — CSR adjacency, label/type indexes, string
  table — loads fully into memory. Traversals (neighbors, typed neighbors,
  BFS, label scans) are local with zero network round trips per hop.
- **Remote**: per-node records stay in an RRSR record store on object
  storage/CDN and are read via batched, coalesced byte-range fetches.
  Record decoding uses [roaringrange]'s `RecordStore` (RRSR v1 + v2/zstd).

Memory budget: resident adjacency costs ~8 bytes per relationship plus
offset/type arrays — practical in a browser tab up to tens of millions of
relationships. Bigger graphs need a range-fetched adjacency variant (not
implemented here).

[roaringrange]: https://github.com/freeeve/roaringrange

## Native

```rust
let reader = GraphReader::from_rcpg_bytes(&bytes)?;
reader.neighbors(0, Direction::Outgoing);
reader.bfs(0, 3, Direction::Outgoing);

// records via roaringrange's RecordStore over any RangeFetch transport
use rustychickpeas_reader::records::{MemoryFetch, RecordStore};
let store = RecordStore::open(idx_fetch, bin_fetch).await?;
let record = store.get(node_id).await?;
```

## Browser

```bash
wasm-pack build --target web --features wasm
cargo run --example generate_demo        # writes demo/graph.rcpg + records.{idx,bin}
python3 -m http.server -d . 8080         # http.server supports Range requests
# open http://localhost:8080/demo/
```

The wasm bindings are sans-IO: JavaScript fetches `graph.rcpg` once and
hands the bytes to `WasmGraph`; for records, `WasmRecordIndex.planRanges`
turns record IDs into few coalesced `[start, end)` byte ranges, JS fetches
them with HTTP `Range:` headers, and `extract` slices records back out.
See `demo/main.js` for the complete flow.
