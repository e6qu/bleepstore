# BleepStore Go -- Do Next

## Current State: Stage 11b COMPLETE (Azure Blob Storage Gateway Backend) -- 85/86 E2E Tests Passing

All E2E tests pass except `test_missing_content_length` (Go runtime limitation).

- `go test -count=1 ./...` -- all unit tests pass (251 total: 226 existing + 25 new Azure gateway tests)
- `./run_e2e.sh` -- **85/86 pass**
- Azure gateway backend fully implemented with mocked unit tests
- GCP gateway backend fully implemented with mocked unit tests
- AWS gateway backend fully implemented with mocked unit tests
- All three cloud gateway backends (AWS, GCP, Azure) complete

**IMPORTANT**: After adding Azure SDK deps, run:
```bash
cd /Users/zardoz/projects/bleepstore/golang
go get github.com/Azure/azure-sdk-for-go/sdk/storage/azblob github.com/Azure/azure-sdk-for-go/sdk/azidentity github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming
go mod tidy
go test -count=1 ./...
./run_e2e.sh
```

## Next: Stage 12a -- Raft State Machine & Storage

### Goal
Implement the core Raft state machine (FSM), log entry types, command serialization, and persistent storage using `hashicorp/raft`.

### Implementation Scope (per PLAN.md)
1. **Raft FSM** -- implement the finite state machine for handling state transitions
2. **Log entry types** -- define command types for metadata operations
3. **Command serialization** -- serialize/deserialize Raft commands
4. **Persistent storage** -- use `hashicorp/raft-boltdb/v2` for log and stable storage

### Dependencies
```
github.com/hashicorp/raft
github.com/hashicorp/raft-boltdb/v2
```

### Run Tests
```bash
cd /Users/zardoz/projects/bleepstore/golang
go test -count=1 ./...
./run_e2e.sh
```

## Known Issues
- `test_missing_content_length`: Go's `net/http` returns 501 for `Transfer-Encoding: identity` at protocol level before handler code runs; test expects 400/411/403
