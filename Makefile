# kubeswarm-cli Makefile
# Run `make help` for all available targets.

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

BINARY    ?= swarm
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS   := -ldflags "-X main.version=$(VERSION)"

LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	@mkdir -p "$(LOCALBIN)"

GOLANGCI_LINT         = $(LOCALBIN)/golangci-lint
GOLANGCI_LINT_VERSION ?= v2.8.0

# ---------------------------------------------------------------------------
##@ Development
# ---------------------------------------------------------------------------

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: setup
setup: ## One-time dev setup: install git hooks.
	@ln -sf ../../scripts/commit-msg .git/hooks/commit-msg
	@ln -sf ../../scripts/pre-push .git/hooks/pre-push
	@echo "Installed git hooks (commit-msg, pre-push)."

.PHONY: build
build: ## Build the swarm CLI binary.
	@go build $(LDFLAGS) -o bin/$(BINARY) ./cmd/swarm

.PHONY: install
install: ## Install the swarm CLI to GOPATH/bin.
	@go install $(LDFLAGS) ./cmd/swarm

.PHONY: clean
clean: ## Remove build artifacts and Go test cache.
	@rm -f bin/$(BINARY)
	@go clean -testcache

# ---------------------------------------------------------------------------
##@ Quality
# ---------------------------------------------------------------------------

.PHONY: fmt
fmt: ## Format all Go code.
	@gofmt -w .

.PHONY: verify
verify: ## Verify module dependencies (requires go.work for workspace-only deps).
	@go mod verify

.PHONY: test
test: ## Run all tests.
	@go test ./...

.PHONY: lint
lint: fmt golangci-lint ## Format and run linter.
	@"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: fmt golangci-lint ## Format and run linter with auto-fix.
	@"$(GOLANGCI_LINT)" run --fix

.PHONY: tidy
tidy: ## Run go mod tidy.
	@go mod tidy

# ---------------------------------------------------------------------------
##@ CI
# ---------------------------------------------------------------------------

.PHONY: ci
ci: clean fmt verify lint test ## Run the full CI pipeline locally.
	@echo "CI passed."

# ---------------------------------------------------------------------------
##@ Dependencies (auto-installed, pinned versions)
# ---------------------------------------------------------------------------

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT)
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef
