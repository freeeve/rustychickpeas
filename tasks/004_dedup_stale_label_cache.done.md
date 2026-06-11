# Fix stale label cache in parquet deduplication

In graph_builder_parquet.rs (~562-600), node labels are cached in pass one
of dedup and re-fetched in pass two; if modified between passes,
`dedup_updates` carries stale label data, potentially losing or duplicating
labels. Make the merge read labels once at update time.
