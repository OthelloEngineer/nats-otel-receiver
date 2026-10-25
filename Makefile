.PHONY: all build build-linux build-ocb test test-race vet fmt tidy test-containers lgtm-up lgtm-down clean

all: build test

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -o bin/otelcol-nats ./cmd/otelcol-nats
	CGO_ENABLED=0 go build -o bin/nats-otel-receiver ./cmd/nats-otel-receiver
	CGO_ENABLED=0 go build -o bin/test-runner ./test/integration
	CGO_ENABLED=0 go build -o bin/ping ./examples/ping-pong/cmd/ping
	CGO_ENABLED=0 go build -o bin/pong ./examples/ping-pong/cmd/pong

build-linux:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux go build -o bin/otelcol-nats ./cmd/otelcol-nats
	CGO_ENABLED=0 GOOS=linux go build -o bin/nats-otel-receiver ./cmd/nats-otel-receiver
	CGO_ENABLED=0 GOOS=linux go build -o bin/test-runner ./test/integration
	CGO_ENABLED=0 GOOS=linux go build -o bin/ping ./examples/ping-pong/cmd/ping
	CGO_ENABLED=0 GOOS=linux go build -o bin/pong ./examples/ping-pong/cmd/pong

build-ocb:
	docker run --rm -v $$(pwd):/workspace -w /workspace otel/opentelemetry-collector-builder:0.122.0 --config /workspace/builder-config.yaml

test:
	go test -v ./...

test-race:
	go test -v -race ./...

vet:
	go vet ./...

fmt:
	go fmt ./...

tidy:
	go mod tidy

test-containers: build-linux
	docker compose -f docker-compose.test.yml down -v --remove-orphans
	docker compose -f docker-compose.test.yml build
	docker compose -f docker-compose.test.yml up --abort-on-container-exit --exit-code-from test-runner
	docker compose -f docker-compose.test.yml down -v --remove-orphans

lgtm-up: build-linux
	docker compose -f deploy/lgtm/docker-compose.yml up -d --build

lgtm-down:
	docker compose -f deploy/lgtm/docker-compose.yml down -v --remove-orphans

clean:
	rm -rf bin/ dist/ coverage.txt profile.out
	docker compose -f docker-compose.test.yml down -v --remove-orphans
	docker compose -f deploy/lgtm/docker-compose.yml down -v --remove-orphans
