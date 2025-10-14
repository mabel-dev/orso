#!/bin/bash
# Script to simplify version comparison benchmarking for Orso
#
# Usage:
#   ./compare_versions.sh <baseline_version> <current_version>
#
# Example:
#   ./compare_versions.sh 0.0.225 0.0.227

set -e

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <baseline_version> <current_version>"
    echo ""
    echo "Example:"
    echo "  $0 0.0.225 0.0.227"
    echo ""
    echo "This will:"
    echo "  1. Install baseline version and run benchmarks"
    echo "  2. Install current version and run benchmarks"
    echo "  3. Compare the results"
    exit 1
fi

BASELINE_VERSION=$1
CURRENT_VERSION=$2

BASELINE_FILE="benchmark_v${BASELINE_VERSION}.json"
CURRENT_FILE="benchmark_v${CURRENT_VERSION}.json"

echo "=========================================="
echo "Orso Version Performance Comparison"
echo "=========================================="
echo "Baseline: $BASELINE_VERSION"
echo "Current:  $CURRENT_VERSION"
echo ""

# Check if python and pip are available
if ! command -v python &> /dev/null; then
    echo "Error: python not found"
    exit 1
fi

# Create a temporary virtual environment
TEMP_VENV=$(mktemp -d)/venv
echo "Creating temporary virtual environment: $TEMP_VENV"
python -m venv "$TEMP_VENV"
source "$TEMP_VENV/bin/activate"

# Benchmark baseline version
echo ""
echo "=========================================="
echo "Step 1: Benchmarking baseline version $BASELINE_VERSION"
echo "=========================================="
pip install -q "orso==$BASELINE_VERSION"
python tests/test_benchmark_suite.py -o "$BASELINE_FILE"

# Benchmark current version
echo ""
echo "=========================================="
echo "Step 2: Benchmarking current version $CURRENT_VERSION"
echo "=========================================="
pip install -q --upgrade "orso==$CURRENT_VERSION"
python tests/test_benchmark_suite.py -o "$CURRENT_FILE" -c "$BASELINE_FILE"

# Cleanup
deactivate
rm -rf "$TEMP_VENV"

echo ""
echo "=========================================="
echo "Comparison Complete!"
echo "=========================================="
echo "Results saved to:"
echo "  Baseline: $BASELINE_FILE"
echo "  Current:  $CURRENT_FILE"
echo ""
echo "To view results later, run:"
echo "  python tests/test_benchmark_suite.py -c $BASELINE_FILE"
echo "  # (with current version installed)"
