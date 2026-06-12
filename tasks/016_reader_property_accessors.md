# Property accessors on GraphReader / WasmGraph

When properties ARE loaded (small graphs, loadProperties=true), the
reader parses node/rel columns into GraphSection but exposes no accessor
methods. Add GraphReader::node_prop(id, key) -> Option<value> (resolving
str atoms) and the wasm equivalent, enabling property-dependent traversal
filters in the browser for small graphs. ~50 lines; mirrors core's
Column::get dense/sparse lookup.
