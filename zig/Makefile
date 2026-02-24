.PHONY: build test-unit test-e2e test run clean lint fmt fmt-check

build:
	zig build

test-unit:
	zig build test

test-e2e:
	./run_e2e.sh

test: test-unit test-e2e

run:
	zig build run -- --config ../bleepstore.example.yaml --port 9013

clean:
	rm -rf zig-out zig-cache .zig-cache

lint:
	zlint src/

fmt:
	zig fmt src/

fmt-check:
	zig fmt --check src/
