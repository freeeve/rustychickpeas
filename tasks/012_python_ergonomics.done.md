# Python binding ergonomics

- __iter__ on GraphSnapshot (yield node IDs)
- big-int overflow gets a clear error message in utils.rs conversion
- stable __hash__ for Node/Relationship (not DefaultHasher)
- evaluate allow_threads for BFS methods; document GIL behavior either way
