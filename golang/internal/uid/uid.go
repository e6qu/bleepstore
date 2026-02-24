// Package uid provides unique identifier generation for BleepStore.
package uid

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// New generates a 32-character hex string suitable for use as a unique
// identifier (temp file names, upload IDs, etc.) using crypto/rand.
func New() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Fallback: timestamp-based ID. Should never happen with crypto/rand.
		return fmt.Sprintf("%032x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
