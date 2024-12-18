## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

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
	bats integration_tests

.PHONY: lint
lint:
	golangci-lint run