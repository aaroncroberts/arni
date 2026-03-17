# Makefile for Arni
# Provides convenient targets for common development tasks

.DEFAULT_GOAL := help

# Phony targets (not files)
.PHONY: help all build build-release clean check test test-unit test-integration \
        fmt fmt-check clippy clippy-fix coverage dev watch install pre-commit \
        check-all ci-check

##@ General

help: ## Display this help message
	@echo "Arni Development Commands"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Build

build: ## Build in debug mode
	@./scripts/build.sh

build-release: ## Build in release mode (optimized)
	@./scripts/build.sh --release

clean: ## Remove build artifacts
	@./scripts/clean.sh

clean-all: ## Deep clean (includes Cargo.lock and caches)
	@./scripts/clean.sh --all

check: ## Fast compilation check without building binaries
	@./scripts/check.sh

##@ Testing

test: ## Run all tests
	@./scripts/test.sh

test-unit: ## Run unit tests only
	@./scripts/test.sh --unit

test-integration: ## Run integration tests only
	@./scripts/test.sh --integration

test-verbose: ## Run tests with verbose output
	@./scripts/test.sh --verbose --nocapture

coverage: ## Generate code coverage report (HTML)
	@./scripts/coverage.sh

coverage-html: ## Generate and open coverage report in browser
	@./scripts/coverage.sh --open

coverage-xml: ## Generate coverage report in XML format (for CI)
	@./scripts/coverage.sh --output xml

coverage-threshold: ## Generate coverage with 80% threshold
	@./scripts/coverage.sh --threshold 80

##@ Code Quality

fmt: ## Format all code with rustfmt
	@./scripts/fmt.sh

fmt-check: ## Check code formatting without modifying files
	@./scripts/fmt.sh --check

clippy: ## Run clippy linter (deny warnings)
	@./scripts/clippy.sh

clippy-fix: ## Run clippy and automatically fix warnings
	@./scripts/clippy.sh --fix

clippy-all: ## Run clippy on all targets (lib, bins, tests, examples)
	@./scripts/clippy.sh --all

##@ Development

dev: ## Watch and rebuild on file changes
	@./scripts/dev.sh

watch: dev ## Alias for dev (watch mode)

dev-test: ## Watch and run tests on file changes
	@./scripts/dev.sh --test

dev-check: ## Watch and run cargo check on file changes
	@./scripts/dev.sh --check

db-start: ## Start all dev database containers (postgres, mysql, mssql, mongodb, oracle)
	@./scripts/dev-containers.sh start

db-stop: ## Stop all dev database containers
	@./scripts/dev-containers.sh stop

db-status: ## Show status of all dev database containers
	@./scripts/dev-containers.sh status

db-logs: ## Tail logs for a service — usage: make db-logs SERVICE=postgres
	@./scripts/dev-containers.sh logs $(SERVICE)

db-rm: ## Remove all dev database containers (data volumes preserved)
	@./scripts/dev-containers.sh rm

##@ Workflows

all: fmt clippy test build ## Format, lint, test, and build

pre-commit: fmt clippy-fix check test ## Run all pre-commit checks (format, fix, check, test)

check-all: fmt-check clippy check test ## Quick validation without modifying files

ci-check: fmt-check clippy-all build-release test coverage-xml ## Full CI validation pipeline

install: ## Install development dependencies
	@echo "Installing development dependencies..."
	@command -v cargo-watch >/dev/null 2>&1 || cargo install cargo-watch
	@command -v cargo-tarpaulin >/dev/null 2>&1 || cargo install cargo-tarpaulin
	@echo "✓ Development dependencies installed"

##@ Release

release-build: ## Build optimized release binary
	@./scripts/build.sh --release --verbose

release-test: ## Run tests against release build
	@./scripts/test.sh --release

##@ Maintenance

rebuild: clean build ## Clean and rebuild from scratch

rebuild-release: clean build-release ## Clean and rebuild release from scratch

info: ## Display project information
	@echo "Project: Arni"
	@echo "Rust version: $$(rustc --version)"
	@echo "Cargo version: $$(cargo --version)"
	@echo ""
	@echo "Available scripts:"
	@ls -1 scripts/*.sh | xargs -n 1 basename
