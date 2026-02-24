# Stage 3: Bucket CRUD -- Completed 2026-02-23

## What Was Implemented

All 7 bucket operation handlers, wired to real metadata and storage backends.

### Files Changed

| File | Changes |
|------|---------|
| `src/lib.rs` | Added `AppState` struct (config, metadata, storage) |
| `src/main.rs` | Initialize SQLite metadata store, seed credentials, initialize LocalBackend, build AppState, pass to app() |
| `src/server.rs` | Updated all dispatch handlers to extract `State<Arc<AppState>>` and pass state + bucket to handlers |
| `src/handlers/bucket.rs` | Complete rewrite: all 7 handlers implemented with validation, XML parsing, ACL handling |
| `src/xml.rs` | Added `render_location_constraint()` and `render_access_control_policy()` |
| `src/storage/local.rs` | Implemented `create_bucket()` and `delete_bucket()` |

### Key Decisions

1. **Bucket name validation**: Manual char-by-char validation instead of regex. Checks: length 3-63, lowercase+digits+hyphens+periods only, starts/ends with alphanumeric, not IP address format, no xn-- prefix, no -s3alias/-ol-s3 suffix.

2. **ACL storage**: ACLs stored as JSON strings in SQLite. The `Acl`/`AclGrant`/`AclGrantee` types with serde derive handle serialization. Canned ACL headers (`x-amz-acl`) are converted to full ACL JSON before storage.

3. **CreateBucket idempotency (us-east-1)**: When a bucket already exists and is owned by the same user in us-east-1, returns 200 OK instead of 409 BucketAlreadyOwnedByYou. This matches real S3 behavior.

4. **HEAD bucket**: Returns plain 404 status code (no body) for non-existent buckets, as HEAD responses must not have a body per HTTP spec. Uses `StatusCode::NOT_FOUND.into_response()` instead of `S3Error::NoSuchBucket` to avoid XML body.

5. **LocationConstraint for us-east-1**: Returns self-closing `<LocationConstraint/>` empty element (not the string "us-east-1"), matching S3 spec quirk.

6. **Storage backend**: `create_bucket` creates a directory, `delete_bucket` removes it. Storage is best-effort; metadata is the source of truth.

7. **No new dependencies**: Used existing quick-xml Reader for XML parsing, existing serde_json for ACL serialization.

### Issues Encountered

- quick-xml 0.31 `Reader::from_reader()` requires `BufRead` impl; used `&[u8]` slice directly which implements `BufRead`.
- Temporary string reference issue in axum response tuples: switched to building `Response` manually with `headers_mut()` for `Location` header.
- `Event::Empty` variant used for self-closing XML elements (LocationConstraint for us-east-1).

### Test Coverage

- **Unit tests added**: 12 bucket name validation + 3 ACL conversion + 2 XML parsing = 17 new tests
- **Existing tests**: 31 metadata + metrics path normalization tests still pass
- **E2E tests**: 15 of 16 bucket tests expected to pass (1 requires Stage 4 object CRUD)
