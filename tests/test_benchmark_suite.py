#!/usr/bin/env python3
"""
Comprehensive performance benchmark suite for comparing Orso versions.

This suite tests the core functionalities:
1. Conversion to and from Arrow
2. Appending of values  
3. Iterating of a DataFrame
4. Display of a DataFrame

Usage:
    python tests/test_benchmark_suite.py

    The suite will generate a JSON report with performance metrics that can be
    compared across different versions of Orso.
"""

import json
import os
import sys
import time
from decimal import Decimal
from typing import Dict, List, Any

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame
from orso.schema import RelationSchema, FlatColumn
from orso.types import OrsoTypes


class PerformanceBenchmark:
    """Performance benchmark runner for Orso DataFrame operations."""
    
    def __init__(self):
        self.results: Dict[str, Any] = {
            "version": self._get_orso_version(),
            "benchmarks": {}
        }
    
    def _get_orso_version(self) -> str:
        """Get the current Orso version."""
        try:
            import orso
            return orso.__version__
        except (ImportError, AttributeError):
            return "unknown"
    
    def _record_result(self, category: str, test_name: str, metrics: Dict[str, Any]):
        """Record a benchmark result."""
        if category not in self.results["benchmarks"]:
            self.results["benchmarks"][category] = {}
        self.results["benchmarks"][category][test_name] = metrics
    
    def benchmark_arrow_to_dataframe(self):
        """Benchmark Arrow to DataFrame conversion."""
        print("\n" + "=" * 80)
        print("BENCHMARK: Arrow to DataFrame Conversion")
        print("=" * 80)
        
        import pyarrow
        
        # Test 1: Small dataset with mixed types
        schema1 = pyarrow.schema([
            ('id', pyarrow.int64()),
            ('name', pyarrow.string()),
            ('value', pyarrow.float64()),
            ('flag', pyarrow.bool_())
        ])
        data1 = {
            'id': list(range(10_000)),
            'name': [f'name_{i}' for i in range(10_000)],
            'value': [i * 1.5 for i in range(10_000)],
            'flag': [i % 2 == 0 for i in range(10_000)]
        }
        table1 = pyarrow.table(data1, schema=schema1)
        
        start = time.perf_counter()
        df1 = DataFrame.from_arrow(table1)
        elapsed1 = time.perf_counter() - start
        rows_per_sec1 = 10_000 / elapsed1
        
        print(f"  Small mixed types (10k rows): {elapsed1:.4f}s ({rows_per_sec1:.0f} rows/sec)")
        
        # Test 2: Large dataset
        num_rows = 100_000
        data2 = {
            'id': list(range(num_rows)),
            'name': [f'name_{i}' for i in range(num_rows)],
            'value': [i * 1.5 for i in range(num_rows)]
        }
        table2 = pyarrow.table(data2)
        
        # Warm up
        _ = DataFrame.from_arrow(table2)
        
        start = time.perf_counter()
        df2 = DataFrame.from_arrow(table2)
        elapsed2 = time.perf_counter() - start
        rows_per_sec2 = num_rows / elapsed2
        
        print(f"  Large dataset (100k rows): {elapsed2:.4f}s ({rows_per_sec2:.0f} rows/sec)")
        
        self._record_result("arrow_conversion", "from_arrow_small_mixed", {
            "rows": 10_000,
            "time_seconds": elapsed1,
            "rows_per_second": rows_per_sec1
        })
        
        self._record_result("arrow_conversion", "from_arrow_large", {
            "rows": num_rows,
            "time_seconds": elapsed2,
            "rows_per_second": rows_per_sec2
        })
    
    def benchmark_dataframe_to_arrow(self):
        """Benchmark DataFrame to Arrow conversion."""
        print("\n" + "=" * 80)
        print("BENCHMARK: DataFrame to Arrow Conversion")
        print("=" * 80)
        
        # Test 1: Standard types
        schema1 = ['id', 'name', 'value', 'flag']
        rows1 = [(i, f'name_{i}', i * 1.5, i % 2 == 0) for i in range(100_000)]
        df1 = DataFrame(rows=rows1, schema=schema1)
        
        # Warm up
        _ = df1.arrow()
        
        start = time.perf_counter()
        arrow_table1 = df1.arrow()
        elapsed1 = time.perf_counter() - start
        rows_per_sec1 = 100_000 / elapsed1
        
        print(f"  Standard types (100k rows): {elapsed1:.4f}s ({rows_per_sec1:.0f} rows/sec)")
        
        # Test 2: With Decimals
        schema2 = ['id', 'price']
        rows2 = [(i, Decimal('99.99')) for i in range(50_000)]
        df2 = DataFrame(rows=rows2, schema=schema2)
        
        # Warm up
        _ = df2.arrow()
        
        start = time.perf_counter()
        arrow_table2 = df2.arrow()
        elapsed2 = time.perf_counter() - start
        rows_per_sec2 = 50_000 / elapsed2
        
        print(f"  With Decimals (50k rows): {elapsed2:.4f}s ({rows_per_sec2:.0f} rows/sec)")
        
        # Test 3: Large dataset
        schema3 = ['col' + str(i) for i in range(20)]
        rows3 = [tuple(range(20)) for _ in range(100_000)]
        df3 = DataFrame(rows=rows3, schema=schema3)
        
        # Warm up
        _ = df3.arrow()
        
        start = time.perf_counter()
        arrow_table3 = df3.arrow()
        elapsed3 = time.perf_counter() - start
        rows_per_sec3 = 100_000 / elapsed3
        
        print(f"  Wide table (100k rows x 20 cols): {elapsed3:.4f}s ({rows_per_sec3:.0f} rows/sec)")
        
        self._record_result("arrow_conversion", "to_arrow_standard", {
            "rows": 100_000,
            "time_seconds": elapsed1,
            "rows_per_second": rows_per_sec1
        })
        
        self._record_result("arrow_conversion", "to_arrow_decimal", {
            "rows": 50_000,
            "time_seconds": elapsed2,
            "rows_per_second": rows_per_sec2
        })
        
        self._record_result("arrow_conversion", "to_arrow_wide", {
            "rows": 100_000,
            "columns": 20,
            "time_seconds": elapsed3,
            "rows_per_second": rows_per_sec3
        })
    
    def benchmark_append_operations(self):
        """Benchmark DataFrame append operations."""
        print("\n" + "=" * 80)
        print("BENCHMARK: Append Operations")
        print("=" * 80)
        
        # Test 1: Append with dict
        schema1 = ['id', 'name', 'value', 'flag']
        df1 = DataFrame(rows=[], schema=schema1)
        
        num_rows = 50_000
        start = time.perf_counter()
        for i in range(num_rows):
            df1.append({'id': i, 'name': f'name_{i}', 'value': i * 1.5, 'flag': i % 2 == 0})
        elapsed1 = time.perf_counter() - start
        rows_per_sec1 = num_rows / elapsed1
        
        print(f"  Dict append (50k rows): {elapsed1:.4f}s ({rows_per_sec1:.0f} rows/sec)")
        
        # Test 2: Append with dict using RelationSchema
        schema2 = RelationSchema(
            name='test',
            columns=[
                FlatColumn(name='id', type=OrsoTypes.INTEGER),
                FlatColumn(name='name', type=OrsoTypes.VARCHAR),
                FlatColumn(name='value', type=OrsoTypes.DOUBLE),
            ]
        )
        df2 = DataFrame(rows=[], schema=schema2)
        
        num_rows = 50_000
        start = time.perf_counter()
        for i in range(num_rows):
            df2.append({'id': i, 'name': f'name_{i}', 'value': float(i)})
        elapsed2 = time.perf_counter() - start
        rows_per_sec2 = num_rows / elapsed2
        
        print(f"  Dict append with schema (50k rows): {elapsed2:.4f}s ({rows_per_sec2:.0f} rows/sec)")
        
        # Test 3: Batch append workflow
        df3 = DataFrame(rows=[], schema=schema1)
        
        append_start = time.perf_counter()
        for i in range(num_rows):
            df3.append({'id': i, 'name': f'name_{i}', 'value': i * 1.5, 'flag': i % 2 == 0})
        append_end = time.perf_counter()
        
        arrow_start = time.perf_counter()
        arrow_table = df3.arrow()
        arrow_end = time.perf_counter()
        
        total_time = arrow_end - append_start
        rows_per_sec3 = num_rows / total_time
        
        print(f"  Append + Arrow workflow (50k rows): {total_time:.4f}s ({rows_per_sec3:.0f} rows/sec)")
        print(f"    - Append: {append_end - append_start:.4f}s")
        print(f"    - Arrow: {arrow_end - arrow_start:.4f}s")
        
        self._record_result("append_operations", "append_dict", {
            "rows": num_rows,
            "time_seconds": elapsed1,
            "rows_per_second": rows_per_sec1
        })
        
        self._record_result("append_operations", "append_dict_with_schema", {
            "rows": num_rows,
            "time_seconds": elapsed2,
            "rows_per_second": rows_per_sec2
        })
        
        self._record_result("append_operations", "append_arrow_workflow", {
            "rows": num_rows,
            "total_time_seconds": total_time,
            "append_time_seconds": append_end - append_start,
            "arrow_time_seconds": arrow_end - arrow_start,
            "rows_per_second": rows_per_sec3
        })
    
    def benchmark_iteration(self):
        """Benchmark DataFrame iteration."""
        print("\n" + "=" * 80)
        print("BENCHMARK: DataFrame Iteration")
        print("=" * 80)
        
        # Test 1: Iterate over materialized DataFrame
        schema1 = ['id', 'name', 'value']
        rows1 = [(i, f'name_{i}', i * 1.5) for i in range(100_000)]
        df1 = DataFrame(rows=rows1, schema=schema1)
        df1.materialize()
        
        start = time.perf_counter()
        count = 0
        for row in df1:
            count += 1
        elapsed1 = time.perf_counter() - start
        rows_per_sec1 = count / elapsed1
        
        print(f"  Materialized (100k rows): {elapsed1:.4f}s ({rows_per_sec1:.0f} rows/sec)")
        
        # Test 2: Iterate over lazy DataFrame
        def row_generator():
            for i in range(100_000):
                yield (i, f'name_{i}', i * 1.5)
        
        df2 = DataFrame(rows=row_generator(), schema=schema1)
        
        start = time.perf_counter()
        count = 0
        for row in df2:
            count += 1
        elapsed2 = time.perf_counter() - start
        rows_per_sec2 = count / elapsed2
        
        print(f"  Lazy/Generator (100k rows): {elapsed2:.4f}s ({rows_per_sec2:.0f} rows/sec)")
        
        # Test 3: Iterate and access values
        df3 = DataFrame(rows=rows1, schema=schema1)
        df3.materialize()
        
        start = time.perf_counter()
        total = 0
        for row in df3:
            total += row[0]  # Access first column
        elapsed3 = time.perf_counter() - start
        rows_per_sec3 = 100_000 / elapsed3
        
        print(f"  With column access (100k rows): {elapsed3:.4f}s ({rows_per_sec3:.0f} rows/sec)")
        
        # Test 4: fetchall vs iteration
        df4 = DataFrame(rows=rows1, schema=schema1)
        
        start = time.perf_counter()
        all_rows = df4.fetchall()
        elapsed4 = time.perf_counter() - start
        rows_per_sec4 = len(all_rows) / elapsed4
        
        print(f"  fetchall (100k rows): {elapsed4:.4f}s ({rows_per_sec4:.0f} rows/sec)")
        
        self._record_result("iteration", "materialized", {
            "rows": count,
            "time_seconds": elapsed1,
            "rows_per_second": rows_per_sec1
        })
        
        self._record_result("iteration", "lazy_generator", {
            "rows": count,
            "time_seconds": elapsed2,
            "rows_per_second": rows_per_sec2
        })
        
        self._record_result("iteration", "with_column_access", {
            "rows": 100_000,
            "time_seconds": elapsed3,
            "rows_per_second": rows_per_sec3
        })
        
        self._record_result("iteration", "fetchall", {
            "rows": len(all_rows),
            "time_seconds": elapsed4,
            "rows_per_second": rows_per_sec4
        })
    
    def benchmark_display(self):
        """Benchmark DataFrame display operations."""
        print("\n" + "=" * 80)
        print("BENCHMARK: DataFrame Display")
        print("=" * 80)
        
        from tests import cities
        
        # Test 1: Small display with types
        df1 = DataFrame(cities.values).head(10)
        
        start = time.perf_counter()
        output1 = df1.display(limit=10, show_types=True)
        elapsed1 = time.perf_counter() - start
        
        print(f"  Small display (10 rows, with types): {elapsed1:.4f}s")
        
        # Test 2: Larger display
        schema2 = ['id', 'name', 'value', 'flag', 'score']
        rows2 = [(i, f'name_{i}', i * 1.5, i % 2 == 0, i * 0.1) for i in range(1000)]
        df2 = DataFrame(rows=rows2, schema=schema2)
        
        start = time.perf_counter()
        output2 = df2.display(limit=100, show_types=True)
        elapsed2 = time.perf_counter() - start
        
        print(f"  Medium display (100 of 1k rows, with types): {elapsed2:.4f}s")
        
        # Test 3: Wide table display
        schema3 = ['col' + str(i) for i in range(50)]
        rows3 = [tuple(range(50)) for _ in range(100)]
        df3 = DataFrame(rows=rows3, schema=schema3)
        
        start = time.perf_counter()
        output3 = df3.display(limit=20)
        elapsed3 = time.perf_counter() - start
        
        print(f"  Wide table (20 rows x 50 cols): {elapsed3:.4f}s")
        
        # Test 4: Markdown output
        start = time.perf_counter()
        output4 = df2.markdown(limit=50)
        elapsed4 = time.perf_counter() - start
        
        print(f"  Markdown output (50 rows): {elapsed4:.4f}s")
        
        # Test 5: Display on lazy DataFrame
        def row_generator():
            for i in range(1000):
                yield (i, f'name_{i}', i * 1.5)
        
        df5 = DataFrame(rows=row_generator(), schema=['id', 'name', 'value'])
        
        start = time.perf_counter()
        output5 = df5.display(limit=10)
        elapsed5 = time.perf_counter() - start
        
        print(f"  Lazy DataFrame display (10 rows): {elapsed5:.4f}s")
        
        self._record_result("display", "small_with_types", {
            "rows": 10,
            "time_seconds": elapsed1,
            "output_length": len(output1)
        })
        
        self._record_result("display", "medium_with_types", {
            "rows": 100,
            "time_seconds": elapsed2,
            "output_length": len(output2)
        })
        
        self._record_result("display", "wide_table", {
            "rows": 20,
            "columns": 50,
            "time_seconds": elapsed3,
            "output_length": len(output3)
        })
        
        self._record_result("display", "markdown", {
            "rows": 50,
            "time_seconds": elapsed4,
            "output_length": len(output4)
        })
        
        self._record_result("display", "lazy_dataframe", {
            "rows": 10,
            "time_seconds": elapsed5,
            "output_length": len(output5)
        })
    
    def run_all_benchmarks(self):
        """Run all benchmarks."""
        print("\n" + "=" * 80)
        print(f"Orso Performance Benchmark Suite - Version {self.results['version']}")
        print("=" * 80)
        
        self.benchmark_arrow_to_dataframe()
        self.benchmark_dataframe_to_arrow()
        self.benchmark_append_operations()
        self.benchmark_iteration()
        self.benchmark_display()
        
        print("\n" + "=" * 80)
        print("BENCHMARK COMPLETE")
        print("=" * 80)
        
        return self.results
    
    def save_results(self, filename: str = "benchmark_results.json"):
        """Save benchmark results to a JSON file."""
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\nResults saved to: {filename}")


def compare_results(baseline_file: str, current_file: str):
    """Compare two benchmark result files and show differences."""
    with open(baseline_file, 'r') as f:
        baseline = json.load(f)
    
    with open(current_file, 'r') as f:
        current = json.load(f)
    
    print("\n" + "=" * 80)
    print(f"Performance Comparison")
    print(f"Baseline: {baseline.get('version', 'unknown')}")
    print(f"Current:  {current.get('version', 'unknown')}")
    print("=" * 80)
    
    for category in baseline.get("benchmarks", {}):
        if category not in current.get("benchmarks", {}):
            continue
        
        print(f"\n{category.upper()}:")
        print("-" * 80)
        
        for test_name in baseline["benchmarks"][category]:
            if test_name not in current["benchmarks"][category]:
                continue
            
            baseline_test = baseline["benchmarks"][category][test_name]
            current_test = current["benchmarks"][category][test_name]
            
            # Compare time if available
            if "time_seconds" in baseline_test and "time_seconds" in current_test:
                baseline_time = baseline_test["time_seconds"]
                current_time = current_test["time_seconds"]
                speedup = baseline_time / current_time
                
                status = "✓" if speedup >= 1.0 else "✗"
                print(f"  {status} {test_name}:")
                print(f"      Baseline: {baseline_time:.4f}s")
                print(f"      Current:  {current_time:.4f}s")
                print(f"      Speedup:  {speedup:.2f}x")
            
            # Compare rows per second if available
            if "rows_per_second" in baseline_test and "rows_per_second" in current_test:
                baseline_rps = baseline_test["rows_per_second"]
                current_rps = current_test["rows_per_second"]
                improvement = (current_rps - baseline_rps) / baseline_rps * 100
                
                status = "✓" if improvement >= 0 else "✗"
                print(f"  {status} {test_name} (rows/sec):")
                print(f"      Baseline: {baseline_rps:,.0f}")
                print(f"      Current:  {current_rps:,.0f}")
                print(f"      Change:   {improvement:+.1f}%")


# Test functions for compatibility with existing test runner
def test_arrow_conversion():
    """Test Arrow conversion performance."""
    benchmark = PerformanceBenchmark()
    benchmark.benchmark_arrow_to_dataframe()
    benchmark.benchmark_dataframe_to_arrow()
    
    # Validate minimum performance
    results = benchmark.results["benchmarks"]["arrow_conversion"]
    assert results["to_arrow_standard"]["rows_per_second"] > 500_000, \
        f"Arrow conversion too slow: {results['to_arrow_standard']['rows_per_second']:.0f} rows/sec"


def test_append_performance():
    """Test append performance."""
    benchmark = PerformanceBenchmark()
    benchmark.benchmark_append_operations()
    
    # Validate minimum performance
    results = benchmark.results["benchmarks"]["append_operations"]
    assert results["append_dict"]["rows_per_second"] > 100_000, \
        f"Append too slow: {results['append_dict']['rows_per_second']:.0f} rows/sec"


def test_iteration_performance():
    """Test iteration performance."""
    benchmark = PerformanceBenchmark()
    benchmark.benchmark_iteration()
    
    # Validate minimum performance
    results = benchmark.results["benchmarks"]["iteration"]
    assert results["materialized"]["rows_per_second"] > 500_000, \
        f"Iteration too slow: {results['materialized']['rows_per_second']:.0f} rows/sec"


def test_display_performance():
    """Test display performance."""
    benchmark = PerformanceBenchmark()
    benchmark.benchmark_display()
    
    # Validate reasonable performance (display should complete quickly)
    results = benchmark.results["benchmarks"]["display"]
    assert results["medium_with_types"]["time_seconds"] < 1.0, \
        f"Display too slow: {results['medium_with_types']['time_seconds']:.4f}s"


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Orso performance benchmarks")
    parser.add_argument("--output", "-o", default="benchmark_results.json",
                        help="Output file for benchmark results")
    parser.add_argument("--compare", "-c", help="Compare with baseline results file")
    args = parser.parse_args()
    
    # Run benchmarks
    benchmark = PerformanceBenchmark()
    benchmark.run_all_benchmarks()
    benchmark.save_results(args.output)
    
    # Compare if requested
    if args.compare:
        compare_results(args.compare, args.output)
