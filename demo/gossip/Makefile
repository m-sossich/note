.PHONY: test test-e2e tidy vet check build clean docker-build docker-run

PORT ?= 6000
ARGS ?= --mode bootstrap --addr 0.0.0.0:$(PORT)

test:
	go test ./... -count=1

# TestGossip_E2E — 1 bootstrap + 5 gossip nodes, full convergence
test-e2e:
	go test . -run TestGossip_E2E -v -count=1 -timeout 60s

tidy:
	go mod tidy

vet:
	go vet ./...

check: tidy vet

build:
	go build -o gossip .

clean:
	rm -f gossip *.key *.id

docker-build:
	docker build -f Dockerfile -t gossip ../..

docker-run: docker-build
	docker run --rm -p $(PORT):$(PORT)/udp -p $(PORT):$(PORT)/tcp gossip $(ARGS)
