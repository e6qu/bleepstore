package server

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestOpenAPISpecMatchesCanonical(t *testing.T) {
	// Parse embedded spec
	var embedded map[string]interface{}
	if err := json.Unmarshal(canonicalSpec, &embedded); err != nil {
		t.Fatalf("failed to parse embedded spec: %v", err)
	}

	// Load canonical from file
	canonical, err := os.ReadFile("../../../schemas/s3-api.openapi.json")
	if err != nil {
		t.Fatalf("failed to read canonical spec: %v", err)
	}
	var canonicalMap map[string]interface{}
	if err := json.Unmarshal(canonical, &canonicalMap); err != nil {
		t.Fatalf("failed to parse canonical spec: %v", err)
	}

	// Strip servers for comparison
	delete(embedded, "servers")
	delete(canonicalMap, "servers")

	if !reflect.DeepEqual(embedded, canonicalMap) {
		t.Error("Embedded OpenAPI spec does not match canonical schema (excluding servers)")
	}
}
