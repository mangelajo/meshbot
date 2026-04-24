.PHONY: help install dev test lint lint-fix format clean run mcp-server sync

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	uv pip install -e .

dev: ## Install development dependencies
	uv pip install -e ".[dev]"

test: ## Run tests with coverage
	uv run pytest

lint: ## Run linting checks
	uv run ruff check
	uv run mypy meshbot

lint-fix: ## Run linting fixes
	uv run ruff check --fix .

format: ## Format code with ruff
	uv run ruff format .

clean: ## Clean up generated files
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete

run: ## Run meshbot (pass ARGS, e.g. make run ARGS="-p /dev/ttyUSB0 run")
	uv run meshbot $(ARGS)

mcp-server: ## Run MCP server standalone (pass ARGS, e.g. make mcp-server ARGS="-p /dev/ttyUSB0")
	uv run meshbot $(ARGS) mcp-server

sync: ## Sync dependencies
	uv sync
