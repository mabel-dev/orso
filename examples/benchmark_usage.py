#!/usr/bin/env python3
"""
Example script showing how to use the benchmark suite to compare Orso versions.

This script demonstrates:
1. Running benchmarks and saving results
2. Comparing two sets of results
3. Programmatic access to benchmark data
"""

import json
import sys
import os

# Add the parent directory to the path to import from tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from tests.test_benchmark_suite import PerformanceBenchmark, compare_results


def example_run_benchmark():
    """Example: Run benchmark and save results."""
    print("Example 1: Running benchmark suite")
    print("=" * 80)
    
    benchmark = PerformanceBenchmark()
    results = benchmark.run_all_benchmarks()
    
    # Save results with version-specific name
    version = results.get('version', 'unknown')
    filename = f"benchmark_v{version}.json"
    benchmark.save_results(filename)
    
    print(f"\nResults saved to: {filename}")
    return filename


def example_load_and_analyze():
    """Example: Load results and perform custom analysis."""
    print("\n\nExample 2: Loading and analyzing results")
    print("=" * 80)
    
    # Create a benchmark result first
    benchmark = PerformanceBenchmark()
    benchmark.benchmark_arrow_to_dataframe()
    benchmark.benchmark_dataframe_to_arrow()
    
    results = benchmark.results
    
    # Extract Arrow conversion performance
    arrow_benchmarks = results['benchmarks']['arrow_conversion']
    
    print("\nArrow Conversion Performance Summary:")
    print("-" * 80)
    
    for test_name, metrics in arrow_benchmarks.items():
        if 'rows_per_second' in metrics:
            print(f"{test_name:30s}: {metrics['rows_per_second']:>15,.0f} rows/sec")
    
    # Calculate average performance for "to_arrow" operations
    to_arrow_tests = [v for k, v in arrow_benchmarks.items() if k.startswith('to_arrow')]
    avg_to_arrow = sum(t['rows_per_second'] for t in to_arrow_tests) / len(to_arrow_tests)
    
    print(f"\n{'Average to_arrow performance':30s}: {avg_to_arrow:>15,.0f} rows/sec")


def example_comparison_workflow():
    """Example: Complete workflow for version comparison."""
    print("\n\nExample 3: Version comparison workflow")
    print("=" * 80)
    
    # Simulate baseline version
    print("\nStep 1: Create baseline benchmark...")
    benchmark1 = PerformanceBenchmark()
    benchmark1.benchmark_append_operations()
    benchmark1.save_results("example_baseline.json")
    print("  ✓ Baseline saved to example_baseline.json")
    
    # Simulate new version (in reality, you would switch versions here)
    print("\nStep 2: Run benchmarks on new version...")
    print("  (In practice, you would: pip install orso==<new_version>)")
    benchmark2 = PerformanceBenchmark()
    benchmark2.benchmark_append_operations()
    benchmark2.save_results("example_current.json")
    print("  ✓ Current version results saved to example_current.json")
    
    # Compare results
    print("\nStep 3: Compare results...")
    compare_results("example_baseline.json", "example_current.json")


def example_custom_benchmark():
    """Example: Create a custom benchmark for specific use case."""
    print("\n\nExample 4: Custom benchmark for specific use case")
    print("=" * 80)
    
    import time
    from orso.dataframe import DataFrame
    
    # Custom benchmark: Test your specific workload
    print("\nCustom workload: Creating DataFrame from dicts and converting to Arrow")
    
    num_rows = 10_000
    data = [
        {'user_id': i, 'score': i * 1.5, 'status': 'active' if i % 2 == 0 else 'inactive'}
        for i in range(num_rows)
    ]
    
    # Time DataFrame creation
    start = time.perf_counter()
    df = DataFrame(data)
    creation_time = time.perf_counter() - start
    
    # Time Arrow conversion
    start = time.perf_counter()
    arrow_table = df.arrow()
    conversion_time = time.perf_counter() - start
    
    print(f"\nResults for {num_rows:,} rows:")
    print(f"  DataFrame creation: {creation_time:.4f}s ({num_rows/creation_time:,.0f} rows/sec)")
    print(f"  Arrow conversion:   {conversion_time:.4f}s ({num_rows/conversion_time:,.0f} rows/sec)")
    print(f"  Total workflow:     {creation_time + conversion_time:.4f}s")
    
    # Save custom results
    custom_results = {
        "version": "custom",
        "workload": "dict_to_dataframe_to_arrow",
        "rows": num_rows,
        "creation_time_seconds": creation_time,
        "conversion_time_seconds": conversion_time,
        "creation_rows_per_second": num_rows / creation_time,
        "conversion_rows_per_second": num_rows / conversion_time
    }
    
    with open("example_custom.json", 'w') as f:
        json.dump(custom_results, f, indent=2)
    
    print("\n  ✓ Custom results saved to example_custom.json")


def example_programmatic_access():
    """Example: Programmatically access benchmark results."""
    print("\n\nExample 5: Programmatic access to results")
    print("=" * 80)
    
    # Run a quick benchmark
    benchmark = PerformanceBenchmark()
    benchmark.benchmark_iteration()
    
    # Access results programmatically
    results = benchmark.results
    iteration_results = results['benchmarks']['iteration']
    
    # Find the fastest iteration method
    fastest = max(iteration_results.items(), 
                  key=lambda x: x[1].get('rows_per_second', 0))
    
    print(f"\nFastest iteration method: {fastest[0]}")
    print(f"  Performance: {fastest[1]['rows_per_second']:,.0f} rows/sec")
    
    # Find slowest
    slowest = min(iteration_results.items(), 
                  key=lambda x: x[1].get('rows_per_second', float('inf')))
    
    print(f"\nSlowest iteration method: {slowest[0]}")
    print(f"  Performance: {slowest[1]['rows_per_second']:,.0f} rows/sec")
    
    # Calculate ratio
    ratio = fastest[1]['rows_per_second'] / slowest[1]['rows_per_second']
    print(f"\nPerformance ratio: {ratio:.1f}x difference")


def cleanup_examples():
    """Clean up example files."""
    import os
    files = [
        'example_baseline.json',
        'example_current.json', 
        'example_custom.json'
    ]
    for f in files:
        if os.path.exists(f):
            os.remove(f)
            print(f"Cleaned up: {f}")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("Orso Benchmark Suite - Usage Examples")
    print("=" * 80)
    
    # Run examples
    # example_run_benchmark()
    example_load_and_analyze()
    example_comparison_workflow()
    example_custom_benchmark()
    example_programmatic_access()
    
    # Cleanup
    print("\n" + "=" * 80)
    print("Cleaning up example files...")
    print("=" * 80)
    cleanup_examples()
    
    print("\n" + "=" * 80)
    print("Examples complete!")
    print("=" * 80)
    print("\nFor full benchmark suite, run:")
    print("  python tests/test_benchmark_suite.py")
    print("\nFor version comparison, run:")
    print("  python tests/test_benchmark_suite.py -o baseline.json")
    print("  # <switch version>")
    print("  python tests/test_benchmark_suite.py -o current.json -c baseline.json")
    print("=" * 80 + "\n")
