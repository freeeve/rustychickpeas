# Expose RCPG serialization in the Python API

Core has GraphSnapshot::write_rcpg/read_rcpg (+_with options for
topology-only) but the Python bindings don't expose them. Add
snapshot.write_rcpg(path, topology_only=False) and
GraphSnapshot.read_rcpg(path) so Python pipelines (e.g. OpenAlex loaders)
can emit graph files and RRSR record stores without Rust. Consider also
exposing rrsr writing (records from an iterator of bytes).
