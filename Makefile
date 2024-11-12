VENV = .venv
BIN = $(VENV)/bin
PYTHON = $(BIN)/python
PIP = $(BIN)/pip
TEST = pytest

# Self documenting commands
.DEFAULT_GOAL := help
.PHONY: help
help: ## show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%s\033[0m|%s\n", $$1, $$2}' \
	| column -t -s '|'

$(VENV)/bin/activate: requirements.txt
	python3 -m venv .venv
	$(PIP) install -U pip
	$(PIP) install -r requirements.txt

clean: ## Remove temporary files
	@rm -rf .ipynb_checkpoints
	@rm -rf **/.ipynb_checkpoints
	@rm -rf __pycache__
	@rm -rf **/__pycache__
	@rm -rf **/**/__pycache__
	@rm -rf .pytest_cache
	@rm -rf **/.pytest_cache
	@rm -rf .ruff_cache
	@rm -rf .coverage
	@rm -rf build
	@rm -rf dist
	@rm -rf *.egg-info
	@rm -rf pg_upsert.log
	@rm -rf site/

bump: ## Show the next version
	@bump-my-version show-bump

bump-patch: $(VENV)/bin/activate ## Bump patch version
	@printf "Applying patch bump\n"
	@$(BIN)/bump-my-version bump patch
	@$(MAKE) bump

bump-minor: $(VENV)/bin/activate ## Bump minor version
	@printf "Applying minor bump\n"
	@$(BIN)/bump-my-version bump minor
	@$(MAKE) bump

bump-major: $(VENV)/bin/activate ## Bump major version
	@printf "Applying major bump\n"
	@$(BIN)/bump-my-version bump major
	@$(MAKE) bump

update: $(VENV)/bin/activate ## Update pip and pre-commit
	$(PIP) install -U pip
	$(PYTHON) -m pre_commit autoupdate

lint: $(VENV)/bin/activate ## Run pre-commit hooks
	$(PYTHON) -m pre_commit install --install-hooks
	$(PYTHON) -m pre_commit run --all-files

test: $(VENV)/bin/activate ## Run unit tests
	$(PYTHON) -m $(TEST)

build-dist: $(VENV)/bin/activate ## Generate distrubition packages
	$(PYTHON) -m build

build-docs: $(VENV)/bin/activate ## Generate documentation
	@printf "Building documentation\n"
	@rm -rf site/
	@mkdocs build -c -q

preview-docs: $(VENV)/bin/activate ## Serve documentation
	@mkdocs serve -w .

publish: $(VENV)/bin/activate ## Publish to PyPI
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) build-dist
	$(PYTHON) -m twine upload --repository pypi dist/*
	$(MAKE) clean

build: $(VENV)/bin/activate ## Build the project
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) build-dist
	$(MAKE) build-docs
