# Add a relationship-type filter to Python neighbors()

Python `relationships(node_id, direction, rel_types=None)` accepts a
relationship-type filter, but `neighbors(node_id, direction)` does not — so a
caller wanting type-filtered neighbors has to go through `relationships` and
extract endpoints by hand. Mirror the `relationships` signature so it matches
core's `neighbors_by_type`:

- `neighbors(node_id, direction, rel_types=None) -> list[Node]`

While here, the binding resolves type names via the private
`snapshot.atoms.get_id`; switch to the public `rel_type` /
`relationship_type_from_str` accessor now that it exists, so the bindings
don't reach into core internals.

Add a test covering type-filtered neighbors (a node with rels of two types,
filtering to one).
