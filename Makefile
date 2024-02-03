lint:
	python -m pip install --quiet --upgrade pycln isort black yamllint
	# python -m yamllint .
	python -m pycln .
	python -m isort .
	python -m black .

update:
	python -m pip install --quiet --upgrade -r requirements.txt
	python -m pip install --quiet --upgrade -r tests/requirements.txt

test:
	python -m pip install --quiet --upgrade pytest coverage
	python -m coverage run -m pytest --color=yes
	python -m coverage report --include=orso/** --fail-under=60 -m

ci:
	python -m pip install --quiet --upgrade mypy
	python -m mypy --ignore-missing-imports --python-version 3.10 --no-strict-optional --check-untyped-defs orso

compile:
	python setup.py build_ext --inplace