# BleepStore Python -- Do Next

## Current State: Stage 15 COMPLETE + Gap Analysis — 86/86 E2E Tests Passing

- `uv run pytest tests/ -v` — 619/619 pass
- `./run_e2e.sh` — **86/86 pass**
- Gap analysis complete: see `S3_GAP_REMAINING.md`

## Next: Stage 16 — S3 API Completeness

Close minor S3 API gaps identified in the gap analysis. These are polish items—the core API is feature-complete.

### Priority 1: GetObject Response Overrides (Medium)
Presigned URLs often use `response-*` query params to override Content-Type, Content-Disposition, etc.

**Files to modify:**
- `src/bleepstore/handlers/object.py` — Parse and apply response-* params in `get_object()`
- `src/bleepstore/xml_utils.py` — Update presigned URL handling if needed

**Query params to support:**
- `response-content-type`
- `response-content-language`
- `response-expires`
- `response-cache-control`
- `response-content-disposition`
- `response-content-encoding`

### Priority 2: Conditional CopyObject (Medium)
Add `x-amz-copy-source-if-*` conditional headers for CopyObject.

**Files to modify:**
- `src/bleepstore/handlers/object.py` — Add conditional evaluation in `copy_object()`

**Headers to support:**
- `x-amz-copy-source-if-match`
- `x-amz-copy-source-if-none-match`
- `x-amz-copy-source-if-modified-since`
- `x-amz-copy-source-if-unmodified-since`

### Priority 3: Conditional UploadPartCopy (Medium)
Same conditional headers for UploadPartCopy.

**Files to modify:**
- `src/bleepstore/handlers/multipart.py` — Add conditional evaluation in `upload_part_copy()`

### Priority 4: EncodingType in List Operations (Low)
Support `encoding-type=url` parameter in list operations.

**Files to modify:**
- `src/bleepstore/handlers/object.py` — Handle `encoding-type` in `list_objects_v1/v2()`
- `src/bleepstore/xml_utils.py` — URL-encode keys when requested

## Run Tests

```bash
cd /Users/zardoz/projects/bleepstore/python
uv run pytest tests/ -v
./run_e2e.sh
```

## Known Issues
- None — all 86 E2E tests pass
