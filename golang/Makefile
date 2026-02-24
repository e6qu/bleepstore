.PHONY: build test-unit test-e2e test run clean lint fmt fmt-check

build:
	go build -o bleepstore ./cmd/bleepstore

test-unit:
	go test -count=1 ./...

test-e2e:
	./run_e2e.sh

test: test-unit test-e2e

run:
	go run ./cmd/bleepstore --config ../bleepstore.example.yaml --port 9011

clean:
	rm -f bleepstore
	go clean ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "Files need formatting:"; gofmt -l .; exit 1)
