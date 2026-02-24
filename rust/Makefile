.PHONY: build test-unit test-e2e test run clean lint fmt fmt-check

build:
	cargo build

test-unit:
	cargo test

test-e2e:
	./run_e2e.sh

test: test-unit test-e2e

run:
	cargo run --release -- --config ../bleepstore.example.yaml --bind 0.0.0.0:9012

clean:
	cargo clean

lint:
	cargo clippy -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check
