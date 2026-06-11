# Validate S3 streaming path with LocalStack, in CI

The v0.7.0 streaming rewrite of the S3 parquet reader had no test coverage
because all 6 S3 integration tests are #[ignore]d.

- Fixed ensure_bucket_exists in s3_integration_test.rs: a missing bucket
  surfaces as Error::Generic (404 NoSuchBucket), not Error::NotFound, so the
  old auto-create fallback never ran; now creates the bucket explicitly via
  the S3 CreateBucket HTTP call.
- All 6 tests pass locally against localstack/localstack:4.9 (2026.x images
  now require a license token).
- Added an s3-integration CI job running the ignored tests against a
  LocalStack 4.9 service container.
