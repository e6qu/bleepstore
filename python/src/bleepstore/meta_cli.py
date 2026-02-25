"""CLI entry point for bleepstore-meta: metadata export/import tool."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from bleepstore.serialization import (
    ALL_TABLES,
    ExportOptions,
    ImportOptions,
    export_metadata,
    import_metadata,
)


def _resolve_db_path(config_path: Path) -> str:
    """Read the SQLite path from a bleepstore YAML config file."""
    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}

    metadata = raw.get("metadata", {})
    sqlite_section = metadata.get("sqlite", {})
    db_path = sqlite_section.get("path", "./data/metadata.db")
    return db_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bleepstore-meta",
        description="BleepStore metadata export/import tool",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Export subcommand
    export_parser = subparsers.add_parser("export", help="Export metadata to JSON")
    export_parser.add_argument(
        "--config", type=Path, default=Path("bleepstore.yaml"),
        help="Config file path (default: bleepstore.yaml)",
    )
    export_parser.add_argument(
        "--db", type=str, default=None,
        help="SQLite database path (overrides config)",
    )
    export_parser.add_argument(
        "--format", choices=["json"], default="json",
        help="Output format (default: json)",
    )
    export_parser.add_argument(
        "--output", type=str, default="-",
        help="Output file path (default: stdout)",
    )
    export_parser.add_argument(
        "--tables", type=str, default=None,
        help=f"Comma-separated table names (default: all). Valid: {','.join(ALL_TABLES)}",
    )
    export_parser.add_argument(
        "--include-credentials", action="store_true", default=False,
        help="Include real secret keys (default: redacted)",
    )

    # Import subcommand
    import_parser = subparsers.add_parser("import", help="Import metadata from JSON")
    import_parser.add_argument(
        "--config", type=Path, default=Path("bleepstore.yaml"),
        help="Config file path (default: bleepstore.yaml)",
    )
    import_parser.add_argument(
        "--db", type=str, default=None,
        help="SQLite database path (overrides config)",
    )
    import_parser.add_argument(
        "--input", type=str, default="-",
        help="Input file path (default: stdin)",
    )
    import_group = import_parser.add_mutually_exclusive_group()
    import_group.add_argument(
        "--merge", action="store_true", default=True,
        help="INSERT OR IGNORE (default)",
    )
    import_group.add_argument(
        "--replace", action="store_true", default=False,
        help="DELETE existing rows first, then INSERT",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    # Resolve database path.
    if args.db:
        db_path = args.db
    else:
        try:
            db_path = _resolve_db_path(args.config)
        except FileNotFoundError:
            print(f"Error: config file not found: {args.config}", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Error reading config: {e}", file=sys.stderr)
            return 1

    if args.command == "export":
        tables = ALL_TABLES
        if args.tables:
            tables = [t.strip() for t in args.tables.split(",")]
            invalid = [t for t in tables if t not in ALL_TABLES]
            if invalid:
                print(f"Error: invalid table names: {', '.join(invalid)}", file=sys.stderr)
                return 1

        options = ExportOptions(
            tables=tables,
            include_credentials=args.include_credentials,
        )

        try:
            output = export_metadata(db_path, options)
        except Exception as e:
            print(f"Error exporting: {e}", file=sys.stderr)
            return 1

        if args.output == "-":
            print(output)
        else:
            Path(args.output).write_text(output, encoding="utf-8")
            print(f"Exported to {args.output}", file=sys.stderr)

    elif args.command == "import":
        if args.input == "-":
            json_str = sys.stdin.read()
        else:
            try:
                json_str = Path(args.input).read_text(encoding="utf-8")
            except FileNotFoundError:
                print(f"Error: input file not found: {args.input}", file=sys.stderr)
                return 1

        options = ImportOptions(replace=args.replace)

        try:
            result = import_metadata(db_path, json_str, options)
        except Exception as e:
            print(f"Error importing: {e}", file=sys.stderr)
            return 1

        for table, count in result.counts.items():
            skip = result.skipped.get(table, 0)
            msg = f"  {table}: {count} imported"
            if skip > 0:
                msg += f", {skip} skipped"
            print(msg, file=sys.stderr)

        for warning in result.warnings:
            print(f"  WARNING: {warning}", file=sys.stderr)

    return 0


def entry_point() -> None:
    """Console script entry point."""
    sys.exit(main())


if __name__ == "__main__":
    entry_point()
