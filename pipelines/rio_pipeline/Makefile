.PHONY: help

help: ## Print the help
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%s\033[0m|%s\n", $$1, $$2}' \
        | column -t -s '|'
	@echo

install-build: ## Dependencies for the build
	python3 -m pip install -e ".[build]"

install-test: ## Dependencies for the test
	python3 -m pip install -e ".[test]"

install-lint: ## Dependencies for the linter
	python3 -m pip install -e ".[lint]"

build: install-build ## Create the wheel files
	# To make build every time when needed
	SOURCE_DATE_EPOCH=315532800 python3 -m build

test: ## Run the unit-tests
	(cd src/rio_pipeline/tests; pytest)

artifacts: build ## Create files to be used in PySpark cluster
	mkdir -p artifacts
	cp src/rio_pipeline/entrypoints/*.py artifacts
	cp dist/*.whl artifacts/

clean: ## Remove all files for PySpark clusters
	rm -rf artifacts
	rm -rf dist
	rm -rf src/*.egg-info
	find . -name "*.pyc" -type f -delete

mypy: ## Run mypy (formatter)
	mypy "src"
