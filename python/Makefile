.PHONY: build test-unit test-e2e test run clean lint fmt fmt-check

build:
	uv sync --all-extras

test-unit:
	uv run pytest tests/ -v

test-e2e:
	./run_e2e.sh

test: test-unit test-e2e

run:
	uv run bleepstore --config ../bleepstore.example.yaml --port 9010

clean:
	rm -rf .venv __pycache__ src/**/__pycache__ .pytest_cache logs/*.log

lint:
	uv run ruff check src/ tests/

fmt:
	uv run ruff format src/ tests/

fmt-check:
	uv run ruff format --check src/ tests/
