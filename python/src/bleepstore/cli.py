"""CLI entry point for BleepStore."""

import argparse
import logging
import sys
from pathlib import Path

import uvicorn

from bleepstore.config import load_config
from bleepstore.server import create_app


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Argument list to parse. Defaults to sys.argv[1:].

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        prog="bleepstore",
        description="BleepStore - S3-compatible object storage server",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("bleepstore.yaml"),
        help="Path to YAML configuration file (default: bleepstore.yaml)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Host address to bind to (overrides config)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to listen on (overrides config)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Main entry point for the BleepStore CLI.

    Loads configuration, applies CLI overrides, and starts the server
    using uvicorn. SIGTERM handling is provided by uvicorn's built-in
    graceful shutdown.

    Args:
        argv: Argument list to parse. Defaults to sys.argv[1:].
    """
    args = parse_args(argv)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    logger = logging.getLogger("bleepstore")

    # Load configuration
    try:
        config = load_config(args.config)
    except FileNotFoundError:
        logger.error("Config file not found: %s", args.config)
        sys.exit(1)
    except Exception as exc:
        logger.error("Failed to load config: %s", exc)
        sys.exit(1)

    # Apply CLI overrides
    if args.host is not None:
        config.server.host = args.host
    if args.port is not None:
        config.server.port = args.port

    logger.info(
        "Starting BleepStore on %s:%d (region=%s)",
        config.server.host,
        config.server.port,
        config.server.region,
    )

    # Create the FastAPI app
    app = create_app(config)

    # Run with uvicorn (crash-only: every startup is recovery)
    # uvicorn handles SIGTERM/SIGINT graceful shutdown natively
    uvicorn.run(
        app,
        host=config.server.host,
        port=config.server.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
