# Propagate relationship property errors in parquet loads

`set_rel_property_from_arrow` (graph_builder_parquet.rs:145) returns `()`,
so failures at its call sites (~967, ~1347) are silently swallowed. Node
properties propagate errors with `?`; relationship properties don't, so a
load can "succeed" with relationships missing properties.

Fix: return `Result` and propagate at call sites.
