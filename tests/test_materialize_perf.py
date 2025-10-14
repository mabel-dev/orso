#!/usr/bin/env python3
"""
Quick script to test materialize performance improvements.
"""
import os
import sys
import time

from orso.dataframe import DataFrame

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def test_materialize_performance():
    """Test that repeated materialize calls are fast with the optimization"""
    
    schema = ['id', 'name', 'value']
    num_rows = 100_000
    
    # Create DataFrame from a generator (lazy)
    def row_generator():
        for i in range(num_rows):
            yield (i, f'name_{i}', i * 1.5)
    
    df = DataFrame(rows=row_generator(), schema=schema)
    
    # Time multiple materialize calls - should be fast after first call
    times = []
    for i in range(5):
        start = time.perf_counter()
        df.materialize()
        end = time.perf_counter()
        times.append(end - start)
        print(f"Materialize call {i+1}: {times[-1]:.4f} seconds")
    
    # First call should be slow (does the work), subsequent calls should be near-instant
    first_time = times[0]
    subsequent_avg = sum(times[1:]) / len(times[1:])
    
    print(f"First materialize: {first_time:.4f}s")
    print(f"Subsequent avg: {subsequent_avg:.6f}s")
    print(f"Speedup ratio: {first_time / subsequent_avg:.1f}x")

    # Subsequent calls should be at least 20x faster
    assert first_time / subsequent_avg > 20, f"Not fast enough: {first_time / subsequent_avg:.1f}x"
    
    # Test that we can still access the data
    assert len(df) == num_rows
    assert df.rowcount == num_rows

def test_property_access_performance():
    """Test that property access is fast when already materialized"""
    
    schema = ['id', 'name', 'value']
    num_rows = 50_000
    rows = [(i, f'name_{i}', i * 1.5) for i in range(num_rows)]
    
    df = DataFrame(rows=rows, schema=schema)
    
    # Time multiple len() calls - should be fast since already materialized
    start = time.perf_counter()
    for _ in range(1000):
        _ = len(df)
    end = time.perf_counter()
    
    avg_time = (end - start) / 1000
    print(f"Average len() call: {avg_time*1000:.3f}ms")
    
    # Should be very fast (< 1ms on average)
    assert avg_time < 0.001, f"len() too slow: {avg_time*1000:.3f}ms"

if __name__ == "__main__":
    print("Testing materialize performance improvements...")
    test_materialize_performance()
    print("\nTesting property access performance...")
    test_property_access_performance()
    print("\nAll tests passed!")
