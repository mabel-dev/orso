# Orso Performance Benchmark Suite

This document describes the comprehensive performance benchmark suite for comparing different versions of Orso DataFrame.

## Overview

The benchmark suite (`tests/test_benchmark_suite.py`) tests the **core functionalities** of Orso:

1. **Conversion to and from Arrow** - Testing PyArrow interoperability
2. **Appending of values** - Testing row insertion performance
3. **Iterating of a DataFrame** - Testing row access patterns
4. **Display of a DataFrame** - Testing output formatting

Everything else is glue and sugar - these are the fundamental operations that determine Orso's performance.

## Running the Benchmark Suite

### Basic Usage

Run all benchmarks and save results:

```bash
python tests/test_benchmark_suite.py
```

This will:
- Run all benchmark categories
- Print results to stdout
- Save detailed metrics to `benchmark_results.json`

### Custom Output File

Specify a custom output file:

```bash
python tests/test_benchmark_suite.py -o my_results.json
```

### Comparing Versions

To compare two versions of Orso:

1. **Create a baseline** with the current version:
   ```bash
   python tests/test_benchmark_suite.py -o baseline.json
   ```

2. **Switch to a different version** (upgrade, downgrade, or checkout different branch)

3. **Run benchmarks again and compare**:
   ```bash
   python tests/test_benchmark_suite.py -o current.json -c baseline.json
   ```

The comparison will show:
- Speedup ratios (higher is better)
- Rows per second changes (percentage improvement/regression)
- Visual indicators (✓ for improvements, ✗ for regressions)

## Benchmark Categories

### 1. Arrow Conversion

Tests bidirectional Arrow conversion performance:

- **from_arrow_small_mixed**: 10k rows with mixed types (int, string, float, bool)
- **from_arrow_large**: 100k rows standard conversion
- **to_arrow_standard**: 100k rows with standard types
- **to_arrow_decimal**: 50k rows with Decimal types (PyArrow bottleneck)
- **to_arrow_wide**: 100k rows × 20 columns

### 2. Append Operations

Tests row insertion performance:

- **append_dict**: 50k rows appending dictionaries
- **append_dict_with_schema**: 50k rows with RelationSchema validation
- **append_arrow_workflow**: Complete workflow (append + Arrow conversion)

### 3. Iteration

Tests different iteration patterns:

- **materialized**: Iterating pre-materialized DataFrame (100k rows)
- **lazy_generator**: Iterating lazy/generator-based DataFrame (100k rows)
- **with_column_access**: Iteration with column value access (100k rows)
- **fetchall**: Bulk fetch all rows (100k rows)

### 4. Display

Tests output formatting performance:

- **small_with_types**: 10 rows with type annotations
- **medium_with_types**: 100 rows with type annotations
- **wide_table**: 20 rows × 50 columns
- **markdown**: Markdown format output (50 rows)
- **lazy_dataframe**: Display from lazy DataFrame (10 rows)

## Output Format

Results are saved in JSON format with the following structure:

```json
{
  "version": "0.0.227",
  "benchmarks": {
    "category_name": {
      "test_name": {
        "rows": 100000,
        "time_seconds": 0.0172,
        "rows_per_second": 5816031.7,
        ...
      }
    }
  }
}
```

## Integration with Test Suite

The benchmark suite also provides individual test functions compatible with the existing test runner:

```bash
python tests/test_benchmark_suite.py
```

This will run:
- `test_arrow_conversion()` - Validates Arrow conversion meets minimum performance
- `test_append_performance()` - Validates append meets minimum performance  
- `test_iteration_performance()` - Validates iteration meets minimum performance
- `test_display_performance()` - Validates display completes quickly

Each test has performance assertions to catch regressions.

## Performance Targets

Current minimum performance targets:

- **Arrow conversion**: > 500k rows/sec (standard types)
- **Append operations**: > 100k rows/sec
- **Iteration**: > 500k rows/sec (materialized)
- **Display**: < 1 second (100 rows with types)

## Tips for Benchmarking

1. **Warm-up runs**: The suite includes warm-up iterations for operations that involve import overhead (like Arrow conversion)

2. **Consistent environment**: Run benchmarks on the same hardware with minimal background processes

3. **Multiple runs**: For critical comparisons, run the suite multiple times and average the results

4. **Version switching**: When comparing versions, use:
   ```bash
   # Save current version baseline
   python tests/test_benchmark_suite.py -o v1_baseline.json
   
   # Switch version (pip install, git checkout, etc.)
   
   # Compare with new version
   python tests/test_benchmark_suite.py -o v2_current.json -c v1_baseline.json
   ```

## Understanding Results

### Speedup Ratio
- **> 1.0**: New version is faster ✓
- **< 1.0**: New version is slower ✗
- **= 1.0**: No change

### Rows per Second
- **Positive %**: Performance improvement ✓
- **Negative %**: Performance regression ✗

### Known Limitations

- **Decimal types**: PyArrow's Decimal handling is slower (~700-900k rows/sec) compared to standard types (4-5M rows/sec). This is a PyArrow limitation, not Orso.
- **Display operations**: Time includes formatting and string operations, not just data access.

## Example Comparison Output

```
Performance Comparison
Baseline: 0.0.227
Current:  0.0.228
================================================================================

ARROW_CONVERSION:
--------------------------------------------------------------------------------
  ✓ to_arrow_standard:
      Baseline: 0.0172s
      Current:  0.0165s
      Speedup:  1.04x
  ✓ to_arrow_standard (rows/sec):
      Baseline: 5,816,031
      Current:  6,060,606
      Change:   +4.2%
```

### Latest Comparison: 0.0.226 → 0.0.228

Command:
```bash
python tests/test_benchmark_suite.py -o current.json -c baseline.json
```

- **Arrow conversion:** `to_arrow_standard` is 3.2× faster (16.4M rows/sec vs 5.1M), with decimal and wide workloads up 7–26%.
- **Append operations:** `append_dict` and the schema variant are 83% and 47% faster; the append+Arrow workflow nearly 1.6×.
- **Iteration:** Materialized, lazy, and column-access loops are 2–7% faster than 0.0.226, and `fetchall` more than doubles throughput (+125%).
- **Display:** All table renderers sped up—`small_with_types` 1.8×, `wide_table` 1.7×, and markdown/lazy outputs ~6–31%.

## Related Files

- `tests/test_performance.py` - Original performance tests
- `benchmark_opteryx_patterns.py` - Opteryx-specific usage patterns
- `PERFORMANCE_IMPROVEMENTS.md` - Documentation of past improvements
