GO ?= go
GOFMT ?= gofmt
BUF_VERSION ?= v1.47.2
PROTOC_GEN_GO_VERSION ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1
ACCESS_ROOT ?= ../wiseiot-access

.PHONY: generate sync-contracts test test-race lint

# generate rebuilds internal/wire from proto, the copy of the Processor Runtime contract of Access.
generate:
	@set -eu; tools_dir="$$(mktemp -d /tmp/observationprocessor-proto-tools.XXXXXX)"; \
	trap 'rm -r -- "$$tools_dir"' EXIT; \
	GOBIN="$$tools_dir" $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION); \
	GOBIN="$$tools_dir" $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION); \
	PATH="$$tools_dir:$$PATH" $(GO) run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) generate

# sync-contracts copies the Processor Runtime contract from an Access checkout at ACCESS_ROOT.
sync-contracts:
	cp $(ACCESS_ROOT)/contracts/wiseiot/access/processor/v1/processor_runtime.proto proto/wiseiot/access/processor/v1/

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

lint:
	@test -z "$$($(GOFMT) -l .)" || { $(GOFMT) -l .; exit 1; }
	$(GO) vet ./...
	$(GO) mod tidy -diff
