// Package serialization handles metadata export/import between SQLite and JSON.
package serialization

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	Version       = "0.1.0"
	ExportVersion = 1
)

// AllTables lists all valid table names in dependency order.
var AllTables = []string{"buckets", "objects", "multipart_uploads", "multipart_parts", "credentials"}

// jsonFields are SQLite columns that store JSON strings to be expanded.
var jsonFields = map[string]bool{"acl": true, "user_metadata": true}

// boolFields are SQLite columns that store integer booleans.
var boolFields = map[string]bool{"delete_marker": true, "active": true}

// tableColumns defines column order for each table.
var tableColumns = map[string][]string{
	"buckets":           {"name", "region", "owner_id", "owner_display", "acl", "created_at"},
	"objects":           {"bucket", "key", "size", "etag", "content_type", "content_encoding", "content_language", "content_disposition", "cache_control", "expires", "storage_class", "acl", "user_metadata", "last_modified", "delete_marker"},
	"multipart_uploads": {"upload_id", "bucket", "key", "content_type", "content_encoding", "content_language", "content_disposition", "cache_control", "expires", "storage_class", "acl", "user_metadata", "owner_id", "owner_display", "initiated_at"},
	"multipart_parts":   {"upload_id", "part_number", "size", "etag", "last_modified"},
	"credentials":       {"access_key_id", "secret_key", "owner_id", "display_name", "active", "created_at"},
}

var tableOrderBy = map[string]string{
	"buckets":           "name",
	"objects":           "bucket, key",
	"multipart_uploads": "upload_id",
	"multipart_parts":   "upload_id, part_number",
	"credentials":       "access_key_id",
}

var deleteOrder = []string{"multipart_parts", "multipart_uploads", "objects", "buckets", "credentials"}
var insertOrder = []string{"buckets", "objects", "multipart_uploads", "multipart_parts", "credentials"}

// ExportOptions configures what to export.
type ExportOptions struct {
	Tables             []string
	IncludeCredentials bool
}

// ImportOptions configures how to import.
type ImportOptions struct {
	Replace bool
}

// ImportResult holds the result of an import operation.
type ImportResult struct {
	Counts   map[string]int
	Skipped  map[string]int
	Warnings []string
}

// ExportMetadata exports metadata from SQLite to a JSON string.
func ExportMetadata(dbPath string, opts *ExportOptions) (string, error) {
	if opts == nil {
		opts = &ExportOptions{Tables: AllTables}
	}

	db, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		return "", fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	schemaVersion := getSchemaVersion(db)
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")

	result := map[string]any{
		"bleepstore_export": map[string]any{
			"version":        ExportVersion,
			"exported_at":    now,
			"schema_version": schemaVersion,
			"source":         "go/" + Version,
		},
	}

	for _, table := range opts.Tables {
		columns, ok := tableColumns[table]
		if !ok {
			continue
		}
		orderBy := tableOrderBy[table]
		query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s", table, orderBy)
		rows, err := db.Query(query)
		if err != nil {
			return "", fmt.Errorf("querying %s: %w", table, err)
		}

		tableRows := make([]map[string]any, 0)
		for rows.Next() {
			values := make([]any, len(columns))
			ptrs := make([]any, len(columns))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return "", fmt.Errorf("scanning %s row: %w", table, err)
			}

			row := make(map[string]any, len(columns))
			for i, col := range columns {
				row[col] = convertValue(col, values[i])
			}

			if table == "credentials" && !opts.IncludeCredentials {
				row["secret_key"] = "REDACTED"
			}

			tableRows = append(tableRows, row)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("iterating %s: %w", table, err)
		}

		result[table] = tableRows
	}

	return marshalSorted(result)
}

// ImportMetadata imports metadata from a JSON string into SQLite.
func ImportMetadata(dbPath string, jsonStr string, opts *ImportOptions) (*ImportResult, error) {
	if opts == nil {
		opts = &ImportOptions{}
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("parsing JSON: %w", err)
	}

	envelope, _ := data["bleepstore_export"].(map[string]any)
	version, _ := envelope["version"].(float64)
	if version < 1 || version > ExportVersion {
		return nil, fmt.Errorf("unsupported export version: %v", version)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	db.Exec("PRAGMA foreign_keys = ON")

	result := &ImportResult{
		Counts:  make(map[string]int),
		Skipped: make(map[string]int),
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}

	if opts.Replace {
		for _, table := range deleteOrder {
			if _, ok := data[table]; ok {
				if _, err := tx.Exec(fmt.Sprintf("DELETE FROM %s", table)); err != nil {
					tx.Rollback()
					return nil, fmt.Errorf("deleting %s: %w", table, err)
				}
			}
		}
	}

	for _, table := range insertOrder {
		rowsData, ok := data[table]
		if !ok {
			continue
		}
		rowList, ok := rowsData.([]any)
		if !ok {
			continue
		}
		columns, ok := tableColumns[table]
		if !ok {
			continue
		}

		inserted := 0
		skipped := 0

		for _, rawRow := range rowList {
			rowMap, ok := rawRow.(map[string]any)
			if !ok {
				skipped++
				continue
			}

			if table == "credentials" {
				if sk, _ := rowMap["secret_key"].(string); sk == "REDACTED" {
					skipped++
					result.Warnings = append(result.Warnings,
						fmt.Sprintf("Skipped credential '%v': REDACTED secret_key", rowMap["access_key_id"]))
					continue
				}
			}

			collapsed := collapseRow(rowMap)
			placeholders := make([]string, len(columns))
			values := make([]any, len(columns))
			for i, col := range columns {
				placeholders[i] = "?"
				values[i] = collapsed[col]
			}

			colNames := strings.Join(columns, ", ")
			ph := strings.Join(placeholders, ", ")
			var query string
			if opts.Replace {
				query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, colNames, ph)
			} else {
				query = fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES (%s)", table, colNames, ph)
			}

			res, err := tx.Exec(query, values...)
			if err != nil {
				skipped++
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("Skipped %s row: %v", table, err))
				continue
			}
			affected, _ := res.RowsAffected()
			if affected > 0 {
				inserted++
			} else {
				skipped++
			}
		}

		result.Counts[table] = inserted
		result.Skipped[table] = skipped
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing transaction: %w", err)
	}

	return result, nil
}

func getSchemaVersion(db *sql.DB) int {
	var version int
	err := db.QueryRow("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1").Scan(&version)
	if err != nil {
		return 1
	}
	return version
}

func convertValue(col string, val any) any {
	if val == nil {
		return nil
	}
	if jsonFields[col] {
		s, ok := val.(string)
		if !ok {
			// sql driver may return []byte
			if b, ok := val.([]byte); ok {
				s = string(b)
			} else {
				return map[string]any{}
			}
		}
		var obj any
		if err := json.Unmarshal([]byte(s), &obj); err != nil {
			return map[string]any{}
		}
		return obj
	}
	if boolFields[col] {
		switch v := val.(type) {
		case int64:
			return v != 0
		case float64:
			return v != 0
		case bool:
			return v
		default:
			return false
		}
	}
	// Convert int64 to int for cleaner JSON output.
	if v, ok := val.(int64); ok {
		return v
	}
	// sql driver may return []byte for TEXT columns.
	if b, ok := val.([]byte); ok {
		return string(b)
	}
	return val
}

func collapseRow(row map[string]any) map[string]any {
	result := make(map[string]any, len(row))
	for k, v := range row {
		if jsonFields[k] {
			if v == nil {
				result[k] = nil
			} else {
				b, err := json.Marshal(v)
				if err != nil {
					result[k] = "{}"
				} else {
					result[k] = string(b)
				}
			}
		} else if boolFields[k] {
			if v == nil {
				result[k] = nil
			} else {
				switch b := v.(type) {
				case bool:
					if b {
						result[k] = int64(1)
					} else {
						result[k] = int64(0)
					}
				default:
					result[k] = v
				}
			}
		} else {
			result[k] = v
		}
	}
	return result
}

// marshalSorted produces JSON with sorted keys, 2-space indent.
func marshalSorted(data map[string]any) (string, error) {
	b, err := json.MarshalIndent(sortedMap(data), "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// sortedMap is a map that marshals with sorted keys.
type sortedMap map[string]any

func (m sortedMap) MarshalJSON() ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	buf := []byte{'{'}
	for i, k := range keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		keyBytes, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf = append(buf, keyBytes...)
		buf = append(buf, ':')

		valBytes, err := marshalValue(m[k])
		if err != nil {
			return nil, err
		}
		buf = append(buf, valBytes...)
	}
	buf = append(buf, '}')
	return buf, nil
}

func marshalValue(v any) ([]byte, error) {
	switch val := v.(type) {
	case map[string]any:
		return sortedMap(val).MarshalJSON()
	case []any:
		buf := []byte{'['}
		for i, elem := range val {
			if i > 0 {
				buf = append(buf, ',')
			}
			b, err := marshalValue(elem)
			if err != nil {
				return nil, err
			}
			buf = append(buf, b...)
		}
		buf = append(buf, ']')
		return buf, nil
	default:
		return json.Marshal(v)
	}
}
