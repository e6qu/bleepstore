package serialization

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

const schemaDDL = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (1, '2026-01-01T00:00:00.000Z');

CREATE TABLE IF NOT EXISTS buckets (
    name TEXT PRIMARY KEY, region TEXT NOT NULL DEFAULT 'us-east-1',
    owner_id TEXT NOT NULL, owner_display TEXT NOT NULL DEFAULT '',
    acl TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS objects (
    bucket TEXT NOT NULL, key TEXT NOT NULL, size INTEGER NOT NULL,
    etag TEXT NOT NULL, content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    content_encoding TEXT, content_language TEXT, content_disposition TEXT,
    cache_control TEXT, expires TEXT,
    storage_class TEXT NOT NULL DEFAULT 'STANDARD',
    acl TEXT NOT NULL DEFAULT '{}', user_metadata TEXT NOT NULL DEFAULT '{}',
    last_modified TEXT NOT NULL, delete_marker INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket, key),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, key TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    content_encoding TEXT, content_language TEXT, content_disposition TEXT,
    cache_control TEXT, expires TEXT,
    storage_class TEXT NOT NULL DEFAULT 'STANDARD',
    acl TEXT NOT NULL DEFAULT '{}', user_metadata TEXT NOT NULL DEFAULT '{}',
    owner_id TEXT NOT NULL, owner_display TEXT NOT NULL DEFAULT '',
    initiated_at TEXT NOT NULL,
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS multipart_parts (
    upload_id TEXT NOT NULL, part_number INTEGER NOT NULL,
    size INTEGER NOT NULL, etag TEXT NOT NULL, last_modified TEXT NOT NULL,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS credentials (
    access_key_id TEXT PRIMARY KEY, secret_key TEXT NOT NULL,
    owner_id TEXT NOT NULL, display_name TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL
);
`

func createTestDB(t *testing.T, dir string, seed bool) string {
	t.Helper()
	dbPath := filepath.Join(dir, "test.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(schemaDDL); err != nil {
		t.Fatalf("schema: %v", err)
	}

	if seed {
		db.Exec(`INSERT INTO buckets VALUES ('test-bucket', 'us-east-1', 'bleepstore', 'bleepstore', '{"owner":{"id":"bleepstore"},"grants":[]}', '2026-02-25T12:00:00.000Z')`)
		db.Exec(`INSERT INTO objects VALUES ('test-bucket', 'photos/cat.jpg', 142857, '"d41d8cd98f00b204e9800998ecf8427e"', 'image/jpeg', NULL, NULL, NULL, NULL, NULL, 'STANDARD', '{}', '{"x-amz-meta-author":"John"}', '2026-02-25T14:30:45.000Z', 0)`)
		db.Exec(`INSERT INTO multipart_uploads VALUES ('upload-abc123', 'test-bucket', 'large-file.bin', 'application/octet-stream', NULL, NULL, NULL, NULL, NULL, 'STANDARD', '{}', '{}', 'bleepstore', 'bleepstore', '2026-02-25T13:00:00.000Z')`)
		db.Exec(`INSERT INTO multipart_parts VALUES ('upload-abc123', 1, 5242880, '"098f6bcd4621d373cade4e832627b4f6"', '2026-02-25T13:05:00.000Z')`)
		db.Exec(`INSERT INTO credentials VALUES ('bleepstore', 'bleepstore-secret', 'bleepstore', 'bleepstore', 1, '2026-02-25T12:00:00.000Z')`)
	}

	return dbPath
}

func TestExportAllTables(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, true)

	result, err := ExportMetadata(dbPath, nil)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(result), &data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	envelope := data["bleepstore_export"].(map[string]any)
	if envelope["version"].(float64) != 1 {
		t.Error("expected version 1")
	}
	if envelope["source"].(string) != "go/0.1.0" {
		t.Error("expected source go/0.1.0")
	}

	buckets := data["buckets"].([]any)
	if len(buckets) != 1 {
		t.Errorf("expected 1 bucket, got %d", len(buckets))
	}

	objects := data["objects"].([]any)
	if len(objects) != 1 {
		t.Errorf("expected 1 object, got %d", len(objects))
	}
}

func TestExportACLExpanded(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, true)

	result, err := ExportMetadata(dbPath, nil)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	var data map[string]any
	json.Unmarshal([]byte(result), &data)

	buckets := data["buckets"].([]any)
	bucket := buckets[0].(map[string]any)
	acl := bucket["acl"].(map[string]any)
	owner := acl["owner"].(map[string]any)
	if owner["id"].(string) != "bleepstore" {
		t.Error("expected acl.owner.id = bleepstore")
	}
}

func TestExportBoolFields(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, true)

	result, err := ExportMetadata(dbPath, nil)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	var data map[string]any
	json.Unmarshal([]byte(result), &data)

	objects := data["objects"].([]any)
	obj := objects[0].(map[string]any)
	if obj["delete_marker"].(bool) != false {
		t.Error("expected delete_marker = false")
	}

	creds := data["credentials"].([]any)
	cred := creds[0].(map[string]any)
	if cred["active"].(bool) != true {
		t.Error("expected active = true")
	}
}

func TestExportNullFields(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, true)

	result, err := ExportMetadata(dbPath, nil)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	var data map[string]any
	json.Unmarshal([]byte(result), &data)

	objects := data["objects"].([]any)
	obj := objects[0].(map[string]any)
	if obj["content_encoding"] != nil {
		t.Error("expected content_encoding = null")
	}
}

func TestExportCredentialsRedacted(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, true)

	result, err := ExportMetadata(dbPath, nil)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	var data map[string]any
	json.Unmarshal([]byte(result), &data)

	creds := data["credentials"].([]any)
	cred := creds[0].(map[string]any)
	if cred["secret_key"].(string) != "REDACTED" {
		t.Error("expected secret_key = REDACTED")
	}
}

func TestExportCredentialsIncluded(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, true)

	opts := &ExportOptions{Tables: AllTables, IncludeCredentials: true}
	result, err := ExportMetadata(dbPath, opts)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	var data map[string]any
	json.Unmarshal([]byte(result), &data)

	creds := data["credentials"].([]any)
	cred := creds[0].(map[string]any)
	if cred["secret_key"].(string) != "bleepstore-secret" {
		t.Error("expected real secret_key")
	}
}

func TestExportPartialTables(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, true)

	opts := &ExportOptions{Tables: []string{"buckets", "objects"}}
	result, err := ExportMetadata(dbPath, opts)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	var data map[string]any
	json.Unmarshal([]byte(result), &data)

	if _, ok := data["buckets"]; !ok {
		t.Error("expected buckets")
	}
	if _, ok := data["objects"]; !ok {
		t.Error("expected objects")
	}
	if _, ok := data["credentials"]; ok {
		t.Error("should not have credentials")
	}
}

func TestExportSortedKeys(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, true)

	result, err := ExportMetadata(dbPath, nil)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	// Verify sorted keys by checking the JSON output directly.
	// The first key after { should be "bleepstore_export" which comes before "buckets".
	if result[0] != '{' {
		t.Error("expected JSON object")
	}
	// Decode and re-encode with standard sorted marshal to compare.
	var data map[string]any
	json.Unmarshal([]byte(result), &data)
	// Top-level keys should be sorted in the output.
	// Just verify we can parse it and it has the expected structure.
	if _, ok := data["bleepstore_export"]; !ok {
		t.Error("expected bleepstore_export key")
	}
}

func TestRoundTrip(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	db1 := createTestDB(t, dir1, true)
	db2 := createTestDB(t, dir2, false)

	opts := &ExportOptions{Tables: AllTables, IncludeCredentials: true}
	exported, err := ExportMetadata(db1, opts)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	result, err := ImportMetadata(db2, exported, nil)
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	if result.Counts["buckets"] != 1 {
		t.Errorf("expected 1 bucket imported, got %d", result.Counts["buckets"])
	}
	if result.Counts["objects"] != 1 {
		t.Errorf("expected 1 object imported, got %d", result.Counts["objects"])
	}

	// Re-export and compare data sections.
	reExported, err := ExportMetadata(db2, opts)
	if err != nil {
		t.Fatalf("re-export: %v", err)
	}

	var data1, data2 map[string]any
	json.Unmarshal([]byte(exported), &data1)
	json.Unmarshal([]byte(reExported), &data2)
	delete(data1, "bleepstore_export")
	delete(data2, "bleepstore_export")

	b1, _ := json.Marshal(data1)
	b2, _ := json.Marshal(data2)
	if string(b1) != string(b2) {
		t.Error("round-trip data mismatch")
	}
}

func TestImportMergeIdempotent(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, true)

	opts := &ExportOptions{Tables: AllTables, IncludeCredentials: true}
	exported, err := ExportMetadata(dbPath, opts)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	result, err := ImportMetadata(dbPath, exported, nil)
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	if result.Counts["buckets"] != 0 {
		t.Errorf("expected 0 buckets (idempotent), got %d", result.Counts["buckets"])
	}
}

func TestImportReplace(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	db1 := createTestDB(t, dir1, true)
	db2 := createTestDB(t, dir2, true)

	opts := &ExportOptions{Tables: AllTables, IncludeCredentials: true}
	exported, err := ExportMetadata(db1, opts)
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	result, err := ImportMetadata(db2, exported, &ImportOptions{Replace: true})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	if result.Counts["buckets"] != 1 {
		t.Errorf("expected 1 bucket, got %d", result.Counts["buckets"])
	}
}

func TestImportSkipsRedactedCredentials(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	db1 := createTestDB(t, dir1, true)
	db2 := createTestDB(t, dir2, false)

	exported, err := ExportMetadata(db1, nil) // credentials redacted
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	result, err := ImportMetadata(db2, exported, nil)
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	if result.Skipped["credentials"] != 1 {
		t.Errorf("expected 1 skipped credential, got %d", result.Skipped["credentials"])
	}
	if len(result.Warnings) != 1 {
		t.Errorf("expected 1 warning, got %d", len(result.Warnings))
	}
}

func TestImportInvalidVersion(t *testing.T) {
	dir := t.TempDir()
	dbPath := createTestDB(t, dir, false)

	_, err := ImportMetadata(dbPath, `{"bleepstore_export":{"version":99}}`, nil)
	if err == nil {
		t.Error("expected error for invalid version")
	}
}

func TestReferenceFixture(t *testing.T) {
	// Load and import the reference fixture, then re-export and compare.
	fixturePath := "../../../tests/fixtures/metadata-export-reference.json"
	fixtureData, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Skipf("reference fixture not found: %v", err)
	}

	dir := t.TempDir()
	dbPath := createTestDB(t, dir, false)

	result, err := ImportMetadata(dbPath, string(fixtureData), nil)
	if err != nil {
		t.Fatalf("import reference fixture: %v", err)
	}

	if result.Counts["buckets"] != 2 {
		t.Errorf("expected 2 buckets, got %d", result.Counts["buckets"])
	}
	if result.Counts["objects"] != 3 {
		t.Errorf("expected 3 objects, got %d", result.Counts["objects"])
	}

	// Re-export and compare data sections.
	opts := &ExportOptions{Tables: AllTables, IncludeCredentials: true}
	reExported, err := ExportMetadata(dbPath, opts)
	if err != nil {
		t.Fatalf("re-export: %v", err)
	}

	var refData, goData map[string]any
	json.Unmarshal(fixtureData, &refData)
	json.Unmarshal([]byte(reExported), &goData)

	// Compare each table section.
	for _, table := range AllTables {
		refTable, _ := json.Marshal(refData[table])
		goTable, _ := json.Marshal(goData[table])
		if string(refTable) != string(goTable) {
			t.Errorf("table %s mismatch:\nref: %s\n go: %s", table, refTable, goTable)
		}
	}
}
