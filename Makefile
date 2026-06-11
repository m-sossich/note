.PHONY: test test-unit test-integration test-e2e tidy vet fmt check

test:
	go test ./... -count=1

# pkg/codec, pkg/dht, pkg/wire — no network I/O
test-unit:
	go test ./pkg/codec/... ./pkg/dht/... ./pkg/wire/... -v -count=1

# pkg/discovery, pkg/node — spin up real UDP/TCP sockets
test-integration:
	go test ./pkg/discovery/... ./pkg/node/... -v -count=1 -timeout 30s

# test/ — multi-node end-to-end scenarios
test-e2e:
	go test ./test/... -v -count=1 -timeout 60s

tidy:
	go mod tidy

vet:
	go vet ./...

fmt:
	@test -z "$$(gofmt -l .)" || { gofmt -l . >&2; exit 1; }

check: tidy vet fmt
