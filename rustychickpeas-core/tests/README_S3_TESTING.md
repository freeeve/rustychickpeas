# S3 Integration Testing

This directory contains integration tests for S3 Parquet file loading using LocalStack.

## Prerequisites

1. **Docker** - Required to run LocalStack
2. **LocalStack** - AWS service emulator

## Setting Up LocalStack

### Start LocalStack

```bash
docker run --rm -d \
  --name localstack \
  -p 4566:4566 \
  -e SERVICES=s3 \
  localstack/localstack
```

### Verify LocalStack is Running

```bash
curl http://localhost:4566/_localstack/health
```

You should see a JSON response with `"s3": "running"`.

### Stop LocalStack

```bash
docker stop localstack
```

## Running S3 Tests

The S3 integration tests are marked with `#[ignore]` by default, so they won't run during normal test execution.

### Run S3 Tests

```bash
# Make sure LocalStack is running first!
cargo test --test s3_integration_test -- --ignored
```

### Run All Tests (Including S3)

```bash
cargo test --test s3_integration_test -- --include-ignored
```

## How It Works

1. **LocalStack Setup**: LocalStack emulates AWS S3 on `http://localhost:4566`
2. **Test File Creation**: The test creates a Parquet file locally
3. **Upload to LocalStack**: The test uploads the Parquet file to LocalStack's S3
4. **Load from S3**: The test uses the `s3://` path format to load the file via our S3 integration

## Environment Variables

The tests automatically set these environment variables for LocalStack:
- `AWS_ENDPOINT_URL=http://localhost:4566`
- `AWS_ACCESS_KEY_ID=test`
- `AWS_SECRET_ACCESS_KEY=test`
- `AWS_REGION=us-east-1`

## Alternative: MinIO

You can also use MinIO instead of LocalStack:

```bash
docker run --rm -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Then set:
- `AWS_ENDPOINT_URL=http://localhost:9000`
- `AWS_ACCESS_KEY_ID=minioadmin`
- `AWS_SECRET_ACCESS_KEY=minioadmin`

## CI/CD Integration

For CI/CD pipelines, you can add LocalStack as a service:

```yaml
services:
  - name: localstack
    image: localstack/localstack
    ports:
      - 4566:4566
    environment:
      SERVICES: s3
```

Then run tests with `--include-ignored` to include S3 tests.

