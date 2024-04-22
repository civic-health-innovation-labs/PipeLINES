.PHONY: help

define all-pipelines  # Arguments: <command>
    cd pipelines && find . -maxdepth 1 -mindepth 1 -type d | xargs -i make -C {} $(1)
endef

help: ## Print the help
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%s\033[0m|%s\n", $$1, $$2}' \
        | column -t -s '|'
	@echo

artifacts: ## Create files to be used in PySpark cluster for all pipelines
	$(call all-pipelines,artifacts)

install-build: ## Dependencies for the build for all pipelines
	$(call all-pipelines,install-build)

install-test: ## Dependencies for the tests for all pipelines
	$(call all-pipelines,install-test)

install-lint: ## Dependencies for the linter for all pipelines
	$(call all-pipelines,install-lint)

test: ## Run the unit-tests for all pipelines
	$(call all-pipelines,test)

mypy: ## Run the mypy for all pipelines
	$(call all-pipelines,mypy)
