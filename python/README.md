# BleepStore — Python Implementation

## Prerequisites
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager

## Setup
```bash
cd python/
uv venv .venv
source .venv/bin/activate  # or: .venv/bin/activate.fish
uv pip install -e ".[dev]"
```

## Running
```bash
bleepstore --config ../bleepstore.example.yaml
```

## Development
```bash
# Run unit tests
uv run pytest tests/

# Type checking
uv run mypy src/bleepstore/

# Linting
uv run ruff check src/
uv run ruff format src/
```
