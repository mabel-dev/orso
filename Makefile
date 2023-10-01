lint:
	python -m pip install --quiet --upgrade pycln isort black
	python -m pycln .
	python -m isort .
	python -m black .

update:
	python -m pip install --quiet --upgrade -r requirements.txt
	python -m pip install --quiet --upgrade -r tests/requirements.txt

test:
	python -m pytest

ci:
	python -m pip install --quiet --upgrade mypy
	python -m mypy --ignore-missing-imports --python-version 3.10 --no-strict-optional --check-untyped-defs orso