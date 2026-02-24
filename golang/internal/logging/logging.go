// Package logging configures structured logging for BleepStore using log/slog.
package logging

import (
	"io"
	"log/slog"
	"strings"
)

// Setup configures the default slog logger with the specified level and format.
// Supported levels: "debug", "info", "warn", "error" (default: "info").
// Supported formats: "text", "json" (default: "text").
func Setup(level, format string, w io.Writer) {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}

	var handler slog.Handler
	switch strings.ToLower(format) {
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	default:
		handler = slog.NewTextHandler(w, opts)
	}

	slog.SetDefault(slog.New(handler))
}
