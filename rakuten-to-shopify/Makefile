# Rakuten to Shopify Converter - Development Commands

.PHONY: help install install-dev test lint format clean run build docs

# Default target
help: ## Show this help message
	@echo "Rakuten to Shopify Converter - Development Commands"
	@echo "=================================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Installation
install: ## Install the package and dependencies
	uv sync

install-dev: ## Install with development dependencies
	uv sync --extra dev --extra test

# Development
test: ## Run tests
	uv run pytest

test-cov: ## Run tests with coverage
	uv run pytest --cov=src/rakuten_to_shopify --cov-report=html --cov-report=term

lint: ## Run linting (ruff)
	uv run ruff check .
	uv run mypy src/

format: ## Format code with black and ruff
	uv run black .
	uv run ruff check --fix .

# Conversion
run: ## Run the converter (auto-detect or specify INPUT_FILE)
	@if [ -n "$(INPUT_FILE)" ]; then \
		uv run python convert.py $(INPUT_FILE); \
	else \
		uv run python convert.py; \
	fi

run-auto: ## Run with auto-detection from data/input/
	uv run python convert.py --output-dir output/

run-quiet: ## Run in quiet mode (clean output, CSV in → CSV out)
	uv run python convert.py --quiet --output-dir output/

run-example: ## Run with example data (specify file)
	uv run python convert.py data/input/test_rakuten_item.csv --output-dir output/

# Build and clean
build: ## Build the package
	uv build

clean: ## Clean build artifacts and cache
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Documentation
docs: ## Build documentation
	uv run mkdocs build

docs-serve: ## Serve documentation locally
	uv run mkdocs serve

# Quick setup for new environments
setup: install-dev ## Complete setup for development
	uv run pre-commit install
	@echo "Setup complete! Run 'make run-example' to test."

# CI/CD helpers
ci-test: ## Run CI tests
	uv run pytest --cov=src/rakuten_to_shopify --cov-report=xml

ci-lint: ## Run CI linting
	uv run ruff check .
	uv run black --check .
	uv run mypy src/