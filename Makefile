SHELL:=/usr/bin/env bash

.PHONY: lint
lint:
	mypy platonic/sqs tests/**/*.py
	flake8 .

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
