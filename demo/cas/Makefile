.PHONY: test test-e2e tidy vet check build clean docker-build docker-run

PORT ?= 9000
ARGS ?= --mode bootstrap --addr 0.0.0.0:$(PORT)

test:
	go test ./... -count=1

# TestCAS_E2E — 1 bootstrap + 2 providers + 1 getter, multi-provider parallel fetch
test-e2e:
	go test . -run TestCAS_E2E -v -count=1 -timeout 60s

tidy:
	go mod tidy

vet:
	go vet ./...

check: tidy vet

build:
	go build -o cas .

clean:
	rm -f cas *.key

docker-build:
	docker build -f Dockerfile -t cas ../..

docker-run: docker-build
	docker run --rm -p $(PORT):$(PORT)/udp -p $(PORT):$(PORT)/tcp cas $(ARGS)
