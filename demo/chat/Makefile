.PHONY: test test-e2e tidy vet check build clean docker-build docker-run

PORT ?= 8000
ARGS ?= --mode bootstrap --addr 0.0.0.0:$(PORT)

test:
	go test ./... -count=1

# TestChat_E2E — 1 bootstrap + 5 chat nodes, full discovery + message delivery
test-e2e:
	go test . -run TestChat_E2E -v -count=1 -timeout 60s

tidy:
	go mod tidy

vet:
	go vet ./...

check: tidy vet

build:
	go build -o chat .

clean:
	rm -f chat *.key

docker-build:
	docker build -f Dockerfile -t chat ../..

docker-run: docker-build
	docker run --rm -p $(PORT):$(PORT)/udp -p $(PORT):$(PORT)/tcp chat $(ARGS)
