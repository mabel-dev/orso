#!/bin/bash
set -ex

cd $GITHUB_WORKSPACE/io
cd io

# Handle Python version with potential 't' suffix (e.g., 3.14t)
PYTHON_VERSION_NO_T=${PYTHON_VERSION/t/}
PYTHON_VERSION_TAG=${PYTHON_VERSION/./}

# Only build for the specified Python version
PYBIN="/opt/python/cp${PYTHON_VERSION_TAG}-cp${PYTHON_VERSION_TAG}/bin"

# Install necessary packages
"${PYBIN}/python" -m pip install -U setuptools wheel numpy cython

# Build the wheel
"${PYBIN}/python" setup.py bdist_wheel

# Repair the wheel using auditwheel
for whl in dist/*.whl; do
    auditwheel repair "$whl" -w dist/
done