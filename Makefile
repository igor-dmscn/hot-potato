# Hot Potato. `make` on its own lists the targets.
#
# Every recipe here is something that was actually run while building this: the
# test lines, the measurement lines and the demo are the ones in docs/.

SHELL := /bin/bash
.DEFAULT_GOAL := help

BIN     := bin
SERVER  := $(BIN)/server
SPUD    := $(BIN)/spud
ADDR    ?= 127.0.0.1:8080
BASE    ?= http://$(ADDR)

# Integration suites skip themselves unless these point at something real. They
# are separate from the server's own variables on purpose: a suite that flushes a
# database should have to be aimed deliberately.
export HP_TEST_DATABASE_URL ?= postgres://hotpotato:hotpotato@localhost:5432/hotpotato
export HP_TEST_REDIS_URL    ?= redis://localhost:6379/1
export HP_TEST_NATS_URL     ?= nats://localhost:4222
export HP_TEST_KAFKA_BROKERS ?= localhost:29092

##@ Building

.PHONY: build
build: $(SERVER) $(SPUD) ## Build both binaries into ./bin

$(SERVER): $(shell find . -name '*.go' -not -name '*_test.go') go.mod
	@mkdir -p $(BIN)
	go build -trimpath -o $@ ./cmd/server

$(SPUD): $(shell find . -name '*.go' -not -name '*_test.go') go.mod
	@mkdir -p $(BIN)
	go build -trimpath -o $@ ./cmd/spud

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

.PHONY: fmt
fmt: ## gofmt every file in place
	gofmt -w .

.PHONY: vet
vet: ## go vet
	go vet ./...

.PHONY: check
check: fmt vet test ## Format, vet, and run the tests that need nothing running

.PHONY: clean
clean: ## Remove built binaries and coverage output
	rm -rf $(BIN) coverage.out

##@ Dependencies

.PHONY: up
up: ## Start Postgres only — enough to run the server
	docker compose up -d postgres
	@echo "waiting for postgres..."
	@until docker compose exec -T postgres pg_isready -U hotpotato -d hotpotato >/dev/null 2>&1; do sleep 1; done
	@echo "ready"

.PHONY: up-brokers
up-brokers: ## Start Postgres, Redis, NATS and Kafka — everything the full suite needs
	docker compose up -d postgres redis nats kafka
	@echo "waiting for health..."
	@for i in $$(seq 60); do \
		ready=$$(docker compose ps --format '{{.Health}}' postgres redis nats kafka | grep -c healthy); \
		[ "$$ready" = "4" ] && break; sleep 2; \
	done
	@docker compose ps --format '{{.Service}}\t{{.State}}\t{{.Health}}'

.PHONY: up-cluster
up-cluster: ## Start the whole two-instance topology behind Caddy
	docker compose up -d --build
	@echo
	@echo "  http://localhost:8080   Caddy, round-robins between them"
	@echo "  http://localhost:8081   inst-a"
	@echo "  http://localhost:8082   inst-b"

.PHONY: down
down: ## Stop everything and remove the containers
	docker compose down -v

.PHONY: ps
ps: ## What is running
	docker compose ps --format '{{.Service}}\t{{.State}}\t{{.Health}}\t{{.Ports}}'

.PHONY: logs
logs: ## Follow the two instances' logs
	docker compose logs -f inst-a inst-b

##@ Running

.PHONY: run
run: $(SERVER) ## Run one instance in the foreground (needs `make up`)
	HP_ADDR=$(ADDR) $(SERVER)

.PHONY: run-debug
run-debug: $(SERVER) ## Same, with debug logging and fast timings
	HP_ADDR=$(ADDR) HP_LOG_LEVEL=debug HP_SSE_HEARTBEAT=2s HP_PROGRESS_INTERVAL=100ms $(SERVER)

.PHONY: watch
watch: $(SPUD) ## spud watch — print control-plane events (EMAIL=, PASSWORD=)
	$(SPUD) -base $(BASE) -email $(EMAIL) -password $(PASSWORD) watch

.PHONY: demo
demo: $(SERVER) $(SPUD) ## A whole transfer, end to end, with two spuds and no browser
	@./scripts/demo.sh

##@ Testing

.PHONY: test
test: ## Everything that needs no broker (integration suites skip themselves)
	go test ./...

.PHONY: test-short
test-short: ## The same, skipping the 1 GB relay
	go test -short ./...

.PHONY: test-race
test-race: ## Under the race detector
	go test -short -race ./...

.PHONY: test-all
test-all: ## Everything, including Postgres, the distributed suite and all four buses
	go test -count=1 ./...

.PHONY: cover
cover: ## Coverage, as a percentage per package
	go test -short -coverprofile=coverage.out ./... >/dev/null
	go tool cover -func=coverage.out | tail -20

##@ Measurements — see docs/measurements.md

.PHONY: measure-relay
measure-relay: ## 1 GB through a flat heap
	go test -run TestOneGigabyteWithAFlatHeap -v -timeout 300s ./internal/httpapi/

.PHONY: measure-bus
measure-bus: ## Publish-to-deliver latency for all four buses (needs `make up-brokers`)
	go test -run TestBusLatency -v -timeout 300s ./internal/bus/

.PHONY: measure-load
measure-load: $(SERVER) $(SPUD) ## 10,000 idle Streams, then time the graceful drain
	@./scripts/load.sh

.PHONY: measure-distributed
measure-distributed: ## Cross-instance transfer, the 307, and its streaming-body trap
	go test -run 'TestCrossInstance|TestWrongInstance|TestAStreamingUpload|TestPresence' -v ./internal/httpapi/

.PHONY: measure
measure: measure-relay measure-bus measure-distributed measure-load ## All of them

##@ Help

.PHONY: help
help: ## This list
	@awk 'BEGIN {FS = ":.*##"} \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next } \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo
