.PHONY: run demo loadtest test lint build

build:
	go build ./...

run:
	go run ./cmd/motor serve

demo:
	go run ./cmd/motor demo

loadtest:
	go run ./cmd/motor loadtest

test:
	go test -race ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, skipping (optional)"; \
	fi
