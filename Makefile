lint:
	python -m pip install --upgrade pip uv
	python -m uv pip install --quiet --upgrade pycln isort ruff yamllint cython-lint
#	python -m yamllint .
#	cython-lint orso/compute/*.pyx
	python -m ruff check --fix --exit-zero
	python -m pycln .
	python -m isort .
	python -m ruff format orso

update:
	python -m pip install --upgrade pip uv
	python -m uv pip install --upgrade -r tests/requirements.txt
	python -m uv pip install --upgrade -r requirements.txt

test:
	python -m pip install --quiet --upgrade pytest coverage
	python -m coverage run -m pytest --color=yes
	python -m coverage report --include=orso/** --fail-under=60 -m

ci:
	python -m pip install --quiet --upgrade mypy
	python -m mypy --ignore-missing-imports --python-version 3.10 --no-strict-optional --check-untyped-defs orso

compile:
	python -m pip install --upgrade pip uv
	python -m uv pip install --upgrade numpy cython setuptools
	python setup.py build_ext --inplace