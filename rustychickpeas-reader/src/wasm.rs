//! Browser bindings (sans-IO).
//!
//! JavaScript owns the network: it fetches the whole `.rcpg` once at boot
//! and hands the bytes to [`WasmGraph`]; record reads go through
//! [`WasmRecordIndex`], which plans coalesced byte ranges for JS to fetch
//! with HTTP `Range:` headers and then extracts individual records from the
//! returned bytes. Keeping I/O in JS avoids async glue here and works with
//! any transport (fetch, cache API, service worker).
//!
//! Byte offsets cross the boundary as f64: exact up to 2^53-1, far beyond
//! any practical record store.
//!
//! Build with `wasm-pack build --target web --features wasm`.

use wasm_bindgen::prelude::*;

use crate::{Direction, GraphReader, PropValue, ResidentSearch};
use rustychickpeas_format::rrsr::RecordIndex;

/// Resident graph: construct from the full RCPG file bytes.
#[wasm_bindgen]
pub struct WasmGraph {
    inner: GraphReader,
}

#[wasm_bindgen]
impl WasmGraph {
    /// Parse RCPG bytes. By default only topology stays resident
    /// (adjacency, label/type indexes, strings) — the safe choice for
    /// large graphs. Pass `loadProperties = true` to also materialize
    /// property columns in wasm memory.
    #[wasm_bindgen(constructor)]
    pub fn new(bytes: &[u8], load_properties: Option<bool>) -> Result<WasmGraph, JsError> {
        let inner = if load_properties.unwrap_or(false) {
            GraphReader::from_rcpg_bytes(bytes)
        } else {
            GraphReader::topology_only(bytes)
        }
        .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(WasmGraph { inner })
    }

    #[wasm_bindgen(js_name = nodeCount)]
    pub fn node_count(&self) -> u32 {
        self.inner.node_count()
    }

    #[wasm_bindgen(js_name = relationshipCount)]
    pub fn relationship_count(&self) -> f64 {
        self.inner.relationship_count() as f64
    }

    #[wasm_bindgen(js_name = csrIdSpace)]
    pub fn csr_id_space(&self) -> u32 {
        self.inner.csr_id_space()
    }

    /// Neighbors of `nodeId` in `direction` (0 = outgoing, 1 = incoming,
    /// 2 = both).
    pub fn neighbors(&self, node_id: u32, direction: u8) -> Vec<u32> {
        self.inner.neighbors(node_id, dir_from_u8(direction))
    }

    /// Neighbors of `nodeId` in `direction` reached via a relationship type.
    #[wasm_bindgen(js_name = neighborsByType)]
    pub fn neighbors_by_type(&self, node_id: u32, direction: u8, rel_type: &str) -> Vec<u32> {
        self.inner
            .neighbors_by_type(node_id, dir_from_u8(direction), rel_type)
    }

    /// BFS from `start` up to `maxDepth` hops. `direction`: 0 = outgoing,
    /// 1 = incoming, 2 = both. Returns visited IDs in BFS order.
    pub fn bfs(&self, start: u32, max_depth: u32, direction: u8) -> Vec<u32> {
        self.inner.bfs(start, max_depth, dir_from_u8(direction))
    }

    #[wasm_bindgen(js_name = nodeLabels)]
    pub fn node_labels(&self, node_id: u32) -> Vec<String> {
        self.inner
            .node_labels(node_id)
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    #[wasm_bindgen(js_name = nodesWithLabel)]
    pub fn nodes_with_label(&self, label: &str) -> Vec<u32> {
        self.inner
            .nodes_with_label(label)
            .map(|bm| bm.iter().collect())
            .unwrap_or_default()
    }

    pub fn labels(&self) -> Vec<String> {
        self.inner
            .labels()
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    #[wasm_bindgen(js_name = relationshipTypes)]
    pub fn relationship_types(&self) -> Vec<String> {
        self.inner
            .relationship_types()
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    pub fn atom(&self, id: u32) -> Option<String> {
        self.inner.atom(id).map(str::to_string)
    }

    /// Value of node property `key` for `nodeId`, or `undefined` when absent
    /// (unknown key, properties not loaded, or no value for this node). Comes
    /// back as the natural JS type — number (i64 via f64, exact to 2^53-1),
    /// boolean, or string — ready for property-dependent traversal filters.
    #[wasm_bindgen(js_name = nodeProp)]
    pub fn node_prop(&self, node_id: u32, key: &str) -> JsValue {
        prop_to_js(self.inner.node_prop(node_id, key))
    }

    /// Outgoing rels of `nodeId` as a flat `[neighbor0, pos0, neighbor1, pos1,
    /// …]` array; `pos` is the CSR position to pass to `relProp`.
    #[wasm_bindgen(js_name = outRels)]
    pub fn out_rels(&self, node_id: u32) -> Vec<u32> {
        self.inner
            .out_rels(node_id)
            .into_iter()
            .flat_map(|(n, p)| [n, p])
            .collect()
    }

    /// Value of relationship property `key` at outgoing-CSR position `csrPos`
    /// (from `outRels`), or `undefined`. Natural JS type, like `nodeProp` —
    /// ready for rel-property traversal filters.
    #[wasm_bindgen(js_name = relProp)]
    pub fn rel_prop(&self, csr_pos: u32, key: &str) -> JsValue {
        prop_to_js(self.inner.rel_prop(csr_pos, key))
    }

    /// First neighbor of `nodeId` via `relType` in `direction`, or `undefined`
    /// — short-circuits the scan when one neighbor is all you need.
    #[wasm_bindgen(js_name = firstNeighbor)]
    pub fn first_neighbor(&self, node_id: u32, direction: u8, rel_type: &str) -> Option<u32> {
        self.inner
            .first_neighbor(node_id, dir_from_u8(direction), rel_type)
    }

    /// Follow a fixed chain of steps from `start`: `directions[i]` (0/1/2) paired
    /// with `relTypes[i]`, taking the first neighbor at each. `undefined` if a
    /// step has no neighbor or the arrays differ in length.
    pub fn follow(&self, start: u32, directions: &[u8], rel_types: Vec<String>) -> Option<u32> {
        if directions.len() != rel_types.len() {
            return None;
        }
        let steps: Vec<(crate::Direction, &str)> = directions
            .iter()
            .zip(rel_types.iter())
            .map(|(&d, r)| (dir_from_u8(d), r.as_str()))
            .collect();
        self.inner.follow(start, &steps)
    }

    /// Whether `nodeId` has any neighbor via `relType` in `direction`.
    #[wasm_bindgen(js_name = hasRel)]
    pub fn has_rel(&self, node_id: u32, direction: u8, rel_type: &str) -> bool {
        self.inner.has_rel(node_id, dir_from_u8(direction), rel_type)
    }

    /// Whether `nodeId` has a neighbor (via `relType`, `direction`) whose string
    /// node property `key` equals `value`. String-typed — the facet-predicate
    /// case; needs `loadProperties = true`.
    #[wasm_bindgen(js_name = hasNeighborWithStrProperty)]
    pub fn has_neighbor_with_str_property(
        &self,
        node_id: u32,
        direction: u8,
        rel_type: &str,
        key: &str,
        value: &str,
    ) -> bool {
        self.inner.has_neighbor_with_property(
            node_id,
            dir_from_u8(direction),
            rel_type,
            key,
            PropValue::Str(value),
        )
    }

    /// Deduplicated union of the neighbors of `nodeId` reached via any of
    /// `relTypes` in `direction`, ascending by id.
    #[wasm_bindgen(js_name = neighborsByTypes)]
    pub fn neighbors_by_types(&self, node_id: u32, direction: u8, rel_types: Vec<String>) -> Vec<u32> {
        let refs: Vec<&str> = rel_types.iter().map(String::as_str).collect();
        self.inner
            .neighbors_by_types(node_id, dir_from_u8(direction), &refs)
    }

    /// O(1) degree of `nodeId` in `direction` (0 = out, 1 = in, 2 = both).
    pub fn degree(&self, node_id: u32, direction: u8) -> u32 {
        self.inner.degree(node_id, dir_from_u8(direction))
    }

    /// All nodes within `1..=maxHops` of `seed` via `relType` in `direction`
    /// (the typed k-hop neighborhood), as a sorted id array; excludes `seed`.
    pub fn neighborhood(&self, seed: u32, direction: u8, rel_type: &str, max_hops: u32) -> Vec<u32> {
        self.inner
            .neighborhood(seed, dir_from_u8(direction), rel_type, max_hops)
            .iter()
            .collect()
    }
}

/// Map a resolved property value to its natural JS value, or `undefined`.
fn prop_to_js(value: Option<PropValue<'_>>) -> JsValue {
    match value {
        Some(PropValue::Int(i)) => JsValue::from_f64(i as f64),
        Some(PropValue::Float(f)) => JsValue::from_f64(f),
        Some(PropValue::Bool(b)) => JsValue::from_bool(b),
        Some(PropValue::Str(s)) => JsValue::from_str(s),
        None => JsValue::UNDEFINED,
    }
}

/// Resident trigram search: construct from the full `.rrs` index bytes, then
/// `search` for ranked doc IDs. The IDs share the graph's node-ID space, so a
/// hit feeds straight into `WasmGraph` traversal and `WasmRecordIndex` fetches.
#[wasm_bindgen]
pub struct WasmSearch {
    inner: ResidentSearch,
}

#[wasm_bindgen]
impl WasmSearch {
    #[wasm_bindgen(constructor)]
    pub fn new(rrs_bytes: &[u8]) -> Result<WasmSearch, JsError> {
        ResidentSearch::open(rrs_bytes.to_vec())
            .map(|inner| WasmSearch { inner })
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Doc IDs matching `query`, best-ranked first, up to `limit`.
    pub fn search(&self, query: &str, limit: usize) -> Vec<u32> {
        self.inner.search(query, limit)
    }
}

/// Map a JS direction code (0 = outgoing, 1 = incoming, 2 = both) to [`Direction`].
fn dir_from_u8(direction: u8) -> Direction {
    match direction {
        0 => Direction::Outgoing,
        1 => Direction::Incoming,
        _ => Direction::Both,
    }
}

/// Record-store index: construct from the full `.idx` file bytes, then use
/// `planRanges` to turn record IDs into few coalesced `.bin` byte ranges
/// for HTTP Range fetches, and `extract` to slice records back out.
#[wasm_bindgen]
pub struct WasmRecordIndex {
    inner: RecordIndex,
}

#[wasm_bindgen]
impl WasmRecordIndex {
    #[wasm_bindgen(constructor)]
    pub fn new(idx_bytes: &[u8]) -> Result<WasmRecordIndex, JsError> {
        let inner = RecordIndex::parse(idx_bytes).map_err(|e| JsError::new(&e.to_string()))?;
        Ok(WasmRecordIndex { inner })
    }

    pub fn len(&self) -> u32 {
        self.inner.len() as u32
    }

    #[wasm_bindgen(js_name = isEmpty)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// `[start, end]` byte range of a record in `.bin`, or empty if out of
    /// range.
    #[wasm_bindgen(js_name = recordRange)]
    pub fn record_range(&self, id: u32) -> Vec<f64> {
        match self.inner.record_range(id) {
            Some((s, e)) => vec![s as f64, e as f64],
            None => Vec::new(),
        }
    }

    /// Plan coalesced byte ranges for a batch of record IDs; ranges whose
    /// gap is at most `maxGap` bytes merge into one read. Returns a flat
    /// `[start0, end0, start1, end1, ...]` array.
    #[wasm_bindgen(js_name = planRanges)]
    pub fn plan_ranges(&self, ids: &[u32], max_gap: f64) -> Vec<f64> {
        self.inner
            .plan_ranges(ids, max_gap as u64)
            .into_iter()
            .flat_map(|(s, e)| [s as f64, e as f64])
            .collect()
    }

    /// Extract one record's bytes from a fetched range that started at
    /// `rangeStart` in `.bin`. Returns undefined if the record isn't fully
    /// inside the supplied bytes.
    pub fn extract(&self, id: u32, range_start: f64, range_bytes: &[u8]) -> Option<Vec<u8>> {
        let (s, e) = self.inner.record_range(id)?;
        let base = range_start as u64;
        if s < base {
            return None;
        }
        let lo = (s - base) as usize;
        let hi = (e - base) as usize;
        if hi > range_bytes.len() {
            return None;
        }
        Some(range_bytes[lo..hi].to_vec())
    }
}
