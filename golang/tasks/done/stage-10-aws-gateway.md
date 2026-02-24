# Stage 10: AWS S3 Gateway Backend

**Completed:** 2026-02-23

## Summary

Implemented a full AWS S3 gateway storage backend that proxies all data operations
to an upstream AWS S3 bucket via the AWS SDK for Go v2. BleepStore acts as an
S3-compatible gateway in front of native AWS S3, with BleepStore handling
authentication, metadata, and the S3 API surface, while the AWS backend handles
raw data storage and retrieval.

## Key Decisions

1. **S3API interface for testability**: Defined a mockable interface covering all
   S3 operations. The real `*s3.Client` implicitly satisfies it. Tests use a full
   in-memory mock.

2. **Local MD5 computation**: Always compute MD5 locally rather than relying on AWS
   ETags (which differ with SSE). Matches Python reference implementation.

3. **Server-side multipart assembly**: Uses native AWS CreateMultipartUpload +
   UploadPartCopy for assembly without downloading data. Falls back to
   download + re-upload when EntityTooSmall.

4. **CreateBucket/DeleteBucket are no-ops**: All BleepStore buckets share a single
   upstream S3 bucket with key prefixes.

5. **Key mapping**: `{prefix}{bleepstore_bucket}/{key}` for objects,
   `{prefix}.parts/{upload_id}/{part_number}` for temporary parts.

6. **Batch deletion**: Uses ListObjectsV2 + DeleteObjects for efficient part cleanup.

## Files Changed

| File | Change |
|------|--------|
| `internal/storage/aws.go` | Full implementation (replaced stub) |
| `internal/storage/aws_test.go` | New: 22 unit tests with mock S3 client |
| `internal/config/config.go` | Added `AWSPrefix` field |
| `cmd/bleepstore/main.go` | Backend factory: switch on `cfg.Storage.Backend` |
| `go.mod` | Added aws-sdk-go-v2 dependencies |

## Dependencies Added

- `github.com/aws/aws-sdk-go-v2`
- `github.com/aws/aws-sdk-go-v2/config`
- `github.com/aws/aws-sdk-go-v2/service/s3`
- `github.com/aws/smithy-go`

## Test Coverage

22 new unit tests in `aws_test.go`:
- PutObject/GetObject round-trip
- Empty body handling
- GetObject not found error mapping
- Delete object (normal + idempotent)
- CopyObject (normal + not found)
- ObjectExists (true/false)
- CreateBucket/DeleteBucket no-op
- Key mapping (with prefix + without prefix)
- PutPart + DeleteParts lifecycle
- AssembleParts single part (CopyObject path)
- AssembleParts multiple parts (multipart upload path)
- AssembleParts EntityTooSmall fallback
- ETag consistency (object + part)
- PutObject overwrite
- Key/part key mapping table-driven
- Interface compliance
- DeleteParts for non-existent upload

## Configuration

```yaml
storage:
  backend: "aws"
  aws_bucket: "my-backing-bucket"
  aws_region: "us-east-1"
  aws_prefix: "bleepstore/"  # optional
```

## Issues

- User must run `go mod tidy` after this stage to resolve transitive AWS SDK dependencies.
- E2E tests with AWS backend require actual AWS credentials and an accessible S3 bucket.
- Local backend E2E tests continue to pass at 84/86 (no regressions).
