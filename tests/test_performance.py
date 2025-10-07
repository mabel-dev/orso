"""
Test performance improvements for DataFrame append and Arrow conversion.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import time
from orso.dataframe import DataFrame


def test_append_performance():
    """Test that append is reasonably fast"""
    schema = ['id', 'name', 'value', 'flag']
    df = DataFrame(rows=[], schema=schema)
    
    num_rows = 10000
    start = time.perf_counter()
    for i in range(num_rows):
        df.append({'id': i, 'name': f'name_{i}', 'value': i * 1.5, 'flag': i % 2 == 0})
    end = time.perf_counter()
    
    append_time = end - start
    rows_per_sec = num_rows / append_time
    
    # Should be able to append at least 500k rows/sec (conservative target)
    assert rows_per_sec > 500_000, f"Append too slow: {rows_per_sec:.0f} rows/sec"
    assert len(df) == num_rows


def test_to_arrow_performance():
    """Test that Arrow conversion is reasonably fast"""
    schema = ['id', 'name', 'value', 'flag']
    
    # Create a DataFrame with test data
    num_rows = 10000
    rows = [
        (i, f'name_{i}', i * 1.5, i % 2 == 0) 
        for i in range(num_rows)
    ]
    df = DataFrame(rows=rows, schema=schema)
    
    # Warm up (trigger imports)
    _ = df.arrow()
    
    # Time the Arrow conversion
    start = time.perf_counter()
    arrow_table = df.arrow()
    end = time.perf_counter()
    
    arrow_time = end - start
    rows_per_sec = num_rows / arrow_time
    
    # Should be able to convert at least 1M rows/sec (conservative target, after warmup)
    assert rows_per_sec > 1_000_000, f"Arrow conversion too slow: {rows_per_sec:.0f} rows/sec"
    assert arrow_table.num_rows == num_rows


def test_buffering_workflow():
    """Test the typical buffering workflow: append multiple rows then convert to Arrow"""
    schema = ['id', 'name', 'value', 'flag']
    df = DataFrame(rows=[], schema=schema)
    
    num_rows = 50000
    
    # Simulate buffering: append many rows
    append_start = time.perf_counter()
    for i in range(num_rows):
        df.append({'id': i, 'name': f'name_{i}', 'value': i * 1.5, 'flag': i % 2 == 0})
    append_end = time.perf_counter()
    
    # Convert to Arrow (simulating write to Parquet)
    arrow_start = time.perf_counter()
    arrow_table = df.arrow()
    arrow_end = time.perf_counter()
    
    total_time = arrow_end - append_start
    rows_per_sec = num_rows / total_time
    
    # Combined workflow should handle at least 500k rows/sec
    assert rows_per_sec > 500_000, f"Buffering workflow too slow: {rows_per_sec:.0f} rows/sec"
    assert arrow_table.num_rows == num_rows


if __name__ == "__main__":
    from tests import run_tests

    run_tests()
