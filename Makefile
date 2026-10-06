# Sluice developer tasks. `make help` lists them.
GO      ?= go
SEEDS   ?= 200
BIN     := bin

.PHONY: help build test test-short sim sim-nightly planted fuzz lint proto dev compat bench docker web clean

help: ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

build: ## build every binary into bin/
	$(GO) build -trimpath -o $(BIN)/ ./cmd/...

test: ## unit, property, simulation and end-to-end tests
	$(GO) test -count=1 ./...

test-short: ## fast subset
	$(GO) test -short -count=1 ./...

sim: build ## $(SEEDS) simulation seeds, each replayed twice for determinism
	$(BIN)/sluice-sim -seeds $(SEEDS) -determinism

sim-nightly: build ## 20,000 seeds, time-boxed to 45 minutes
	$(BIN)/sluice-sim -seeds 20000 -budget 45m -json sim-nightly.json

planted: ## prove the checker catches planted bugs
	$(GO) test -tags sluice_bugs -run Planted -count=1 -v ./internal/sim

fuzz: ## run each fuzz target for FUZZTIME (default 1m)
	$(GO) test -run '^$$' -fuzz FuzzSplit -fuzztime $${FUZZTIME:-1m} ./internal/kbatch
	$(GO) test -run '^$$' -fuzz FuzzParse -fuzztime $${FUZZTIME:-1m} ./internal/segment
	$(GO) test -run '^$$' -fuzz FuzzApply -fuzztime $${FUZZTIME:-1m} ./internal/sequencer

lint: ## golangci-lint
	golangci-lint run ./...

proto: ## regenerate protobuf code with the pinned generators
	$(GO) install github.com/bufbuild/buf/cmd/buf@v1.73.0
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
	buf lint && buf generate

dev: build ## run a local cluster on ./sluice-data
	$(BIN)/sluice-dev -store file://./sluice-data

compat: ## client compatibility matrix (needs Docker)
	test/compat/run.sh

bench: build ## latency vs PUTs-per-GB curve on an injected-latency store
	mkdir -p bench-results
	$(BIN)/sluice-bench -flush-windows 50ms,100ms,250ms,500ms,1s -json bench-results/curve.json

docker: ## multi-arch image
	docker buildx build --platform linux/amd64,linux/arm64 -t sluice:local .

web: ## build the playground
	cd web && npm ci && npm run build

clean:
	rm -rf $(BIN) bench-results sluice-data
