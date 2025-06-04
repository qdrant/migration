PLATFORMS=darwin/amd64 darwin/arm64 linux/amd64 linux/arm64 windows/amd64 windows/arm64
VERSION=0.0.0
BUILD=dev

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

DIST_DIR ?= $(shell pwd)/dist
$(DIST_DIR):
	mkdir -p $(DIST_DIR)

GCI ?= $(LOCALBIN)/gci

$(GCI): $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install github.com/daixiang0/gci@latest

.PHONY: go_fmt
go_fmt:
	gofmt -w -s .

.PHONY: fmt_imports
fmt_imports: $(GCI)
	$(GCI) write ./ --skip-generated -s standard -s default -s 'prefix(github.com/qdrant)' -s 'prefix(github.com/qdrant/migration/)'

.PHONY: fmt
format: go_fmt fmt_imports

fmt: format

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: fmt vet lint test_integration

.PHONY: test_integration
test_integration:
	bats --print-output-on-failure integration_tests

.PHONY: test_unit
test_unit:
	go test -v -coverprofile cover.out ./...

.PHONY: lint
lint:
	golangci-lint run

