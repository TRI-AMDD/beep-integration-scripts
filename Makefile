.PHONY: test format clean lint

PY_FILES = $(shell find . -name '*.py')

test:
		python -m pytest

format:
		yapf --in-place $(PY_FILES)

lint:
		flake8 $(PY_FILES)

clean:
		find . -name '*.pyc' -exec rm --force {} +
		find . -name '*.pyo' -exec rm --force {} +
		name '*~' -exec rm --force  {}

profile:
		python -m memory_profiler arbin_extract.py

type:
		python -m mypy arbin_extract.py --ignore-missing-imports
		python -m mypy sql_functions.py --ignore-missing-imports
		python -m mypy data_join.py --ignore-missing-imports
