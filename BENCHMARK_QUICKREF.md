# Benchmark Suite Quick Reference

## Basic Usage

```bash
# Run full benchmark suite
python tests/test_benchmark_suite.py

# Save to specific file
python tests/test_benchmark_suite.py -o my_results.json

# Compare versions
python tests/test_benchmark_suite.py -o current.json -c baseline.json
```

## Version Comparison Workflow

```bash
# 1. Create baseline
python tests/test_benchmark_suite.py -o baseline.json

# 2. Switch version (one of these):
pip install orso==<new_version>
git checkout <branch>
# or make code changes

# 3. Compare
python tests/test_benchmark_suite.py -o current.json -c baseline.json
```

## Using the Shell Script

```bash
# Compare any two published versions
./compare_versions.sh 0.0.225 0.0.227
```

## Programmatic Usage

```python
from tests.test_benchmark_suite import PerformanceBenchmark, compare_results

# Run benchmarks
benchmark = PerformanceBenchmark()
results = benchmark.run_all_benchmarks()
benchmark.save_results("results.json")

# Compare results
compare_results("baseline.json", "current.json")
```

## What's Benchmarked

### Core Operations
1. **Arrow Conversion** - to/from PyArrow (5 tests)
2. **Append Operations** - row insertion (3 tests)
3. **Iteration** - row access patterns (4 tests)
4. **Display** - output formatting (5 tests)

### Performance Targets
- Arrow conversion: > 500k rows/sec
- Append: > 100k rows/sec
- Iteration: > 500k rows/sec
- Display: < 1 second

## Output Interpretation

### Speedup Ratio
- `>= 1.0` = Faster ✓
- `< 1.0` = Slower ✗

### Rows/Second Change
- Positive % = Improvement ✓
- Negative % = Regression ✗

## Examples

See `examples/benchmark_usage.py` for:
- Custom benchmarks
- Programmatic result access
- Workflow examples

## Documentation

Full documentation: `BENCHMARK_SUITE.md`
