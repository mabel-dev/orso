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
	python -m uv pip install --upgrade numpy cython setuptools nanobind
	python setup.py build_ext --inplace

# Vendored draken headers (DESIGN.md §2, §3). `sync-draken` copies them from an
# opteryx-core checkout (default ../opteryx-core, override with DRAKEN_SOURCE=path) and
# records the commit in native/draken/SOURCE; `check-draken` fails if the copy has
# drifted from that record. Never edit the copies by hand.
sync-draken:
	python tools/sync_draken.py $(if $(DRAKEN_SOURCE),--source $(DRAKEN_SOURCE))

check-draken:
	python tools/sync_draken.py --check

# Micro-benchmarks. `make bench` prints the table; BASELINE=path compares against a
# saved run; `make bench-baseline` records one for the current version.
bench:
	python benchmarks/bench.py $(if $(BASELINE),-c $(BASELINE)) $(if $(FILTER),-k $(FILTER))

bench-baseline:
	python benchmarks/bench.py -o benchmarks/baseline_$$(python -c 'import orso; print(orso.__version__)').json