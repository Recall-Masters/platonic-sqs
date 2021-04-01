SHELL:=/usr/bin/env bash

.PHONY: lint
lint:
	mypy --namespace-packages platonic
	git diff origin/master | poetry run flakehell lint platonic tests

.PHONY: unit
unit:
	pytest tests

.PHONY: package
package:
	poetry check
	pip check
	safety check --bare --full-report


.PHONY: format
format:
	poetry run isort -rc platonic tests

.PHONY: test
test: lint unit package
