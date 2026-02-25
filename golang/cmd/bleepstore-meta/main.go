// Package main is the entry point for bleepstore-meta, the metadata export/import tool.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/bleepstore/bleepstore/internal/serialization"
	"gopkg.in/yaml.v3"
)

func resolveDBPath(configPath string) (string, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", err
	}
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return "", err
	}
	metadata, _ := raw["metadata"].(map[string]any)
	if metadata == nil {
		return "./data/metadata.db", nil
	}
	sqliteSection, _ := metadata["sqlite"].(map[string]any)
	if sqliteSection == nil {
		return "./data/metadata.db", nil
	}
	path, _ := sqliteSection["path"].(string)
	if path == "" {
		return "./data/metadata.db", nil
	}
	return path, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: bleepstore-meta <export|import> [flags]")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "export":
		rc := runExport(os.Args[2:])
		os.Exit(rc)
	case "import":
		rc := runImport(os.Args[2:])
		os.Exit(rc)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\nUsage: bleepstore-meta <export|import> [flags]\n", command)
		os.Exit(1)
	}
}

func runExport(args []string) int {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	configPath := fs.String("config", "bleepstore.yaml", "Config file path")
	dbPath := fs.String("db", "", "SQLite database path (overrides config)")
	format := fs.String("format", "json", "Output format")
	output := fs.String("output", "-", "Output file path (- for stdout)")
	tables := fs.String("tables", "", "Comma-separated table names")
	includeCreds := fs.Bool("include-credentials", false, "Include real secret keys")
	fs.Parse(args)

	if *format != "json" {
		fmt.Fprintf(os.Stderr, "Error: unsupported format: %s\n", *format)
		return 1
	}

	db := *dbPath
	if db == "" {
		var err error
		db, err = resolveDBPath(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading config: %v\n", err)
			return 1
		}
	}

	tableList := serialization.AllTables
	if *tables != "" {
		tableList = strings.Split(*tables, ",")
		for i := range tableList {
			tableList[i] = strings.TrimSpace(tableList[i])
		}
		valid := make(map[string]bool)
		for _, t := range serialization.AllTables {
			valid[t] = true
		}
		for _, t := range tableList {
			if !valid[t] {
				fmt.Fprintf(os.Stderr, "Error: invalid table name: %s\n", t)
				return 1
			}
		}
	}

	opts := &serialization.ExportOptions{
		Tables:             tableList,
		IncludeCredentials: *includeCreds,
	}

	result, err := serialization.ExportMetadata(db, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error exporting: %v\n", err)
		return 1
	}

	if *output == "-" {
		fmt.Println(result)
	} else {
		if err := os.WriteFile(*output, []byte(result+"\n"), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing output: %v\n", err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "Exported to %s\n", *output)
	}

	return 0
}

func runImport(args []string) int {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	configPath := fs.String("config", "bleepstore.yaml", "Config file path")
	dbPath := fs.String("db", "", "SQLite database path (overrides config)")
	input := fs.String("input", "-", "Input file path (- for stdin)")
	replace := fs.Bool("replace", false, "Replace mode (DELETE then INSERT)")
	fs.Parse(args)

	db := *dbPath
	if db == "" {
		var err error
		db, err = resolveDBPath(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading config: %v\n", err)
			return 1
		}
	}

	var jsonData []byte
	var err error
	if *input == "-" {
		jsonData, err = os.ReadFile("/dev/stdin")
	} else {
		jsonData, err = os.ReadFile(*input)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		return 1
	}

	opts := &serialization.ImportOptions{Replace: *replace}

	result, err := serialization.ImportMetadata(db, string(jsonData), opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error importing: %v\n", err)
		return 1
	}

	for _, table := range serialization.AllTables {
		count, ok := result.Counts[table]
		if !ok {
			continue
		}
		skip := result.Skipped[table]
		msg := fmt.Sprintf("  %s: %d imported", table, count)
		if skip > 0 {
			msg += fmt.Sprintf(", %d skipped", skip)
		}
		fmt.Fprintln(os.Stderr, msg)
	}

	for _, w := range result.Warnings {
		fmt.Fprintf(os.Stderr, "  WARNING: %s\n", w)
	}

	return 0
}
