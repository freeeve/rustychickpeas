# Fix S3 parquet loads buffering entire file in memory

`create_parquet_reader` collects all record batches from the S3 stream
(`stream.try_collect::<Vec<_>>()` at graph_builder_parquet.rs:363) before
processing, so a 10GB file needs 10GB+ of RAM.

Fix: hold the tokio runtime and the `ParquetRecordBatchStream` in
`ParquetReaderEnum::Async` and pull one batch per `next()` call
(`runtime.block_on(stream.next())`), so batches stream incrementally via
HTTP range requests.
