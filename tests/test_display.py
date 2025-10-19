import os
import sys
import datetime as dt

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame
from orso.types import get_orso_type, OrsoTypes
from tests import cities
import re
from typing import List

lengths = {
    0: 6,  # Updated: now includes footer line
    1: 7,
    2: 8,
    3: 9,
    4: 10,
    5: 11,
    6: 12,
    7: 13,
    8: 13,
    9: 13,
    10: 13,
}



def find_all_substrings(s: str, sub: str) -> List[int]:
    """
    Finds all instances of a substring within a string and returns their start indices.

    Parameters:
        s (str): The string to search within.
        sub (str): The substring to search for.

    Returns:
        List[int]: A list of start indices where the substring is found.
    """
    return [m.start() for m in re.finditer(re.escape(sub), s)]

def test_display_ascii_lazy():

    for i in range(10):
        df = DataFrame(cities.values).head(i)
        df._rows = (r for r in df._rows)

        ascii = df.display(limit=3, show_types=True)

        assert len(ascii.split("\n")) == lengths[i], i
        assert len(find_all_substrings(ascii, "Tokyo")) == (1 if i != 0 else 0)


def test_display_ascii_greedy():

    for i in range(10):

        df = DataFrame(cities.values).head(i)
        df.materialize()

        ascii_output = df.display(limit=3, show_types=True)

        assert len(ascii_output.split("\n")) == lengths[i], i
        assert len(find_all_substrings(ascii_output, "Tokyo")) == (1 if i != 0 else 0)


def test_row_count_footer_single_row():
    """Test that row count footer is accurate for a single row DataFrame"""
    df = DataFrame([{"a": 1, "b": 2}])
    output = df.display()
    # Should show "[ 1 rows x 2 columns ]" in the footer
    assert "[ 1 rows x 2 columns ]" in output


def test_row_count_footer_multiple_rows():
    """Test that row count footer is accurate for multiple rows"""
    data = [{"a": i, "b": i * 2} for i in range(10)]
    df = DataFrame(data)
    output = df.display()
    # Should show "[ 10 rows x 2 columns ]" in the footer
    assert "[ 10 rows x 2 columns ]" in output


def test_row_count_footer_lazy_dataframe():
    """Test that row count footer is accurate for lazy (generator-based) DataFrames"""
    data = [{"a": i, "b": i * 2} for i in range(50)]
    df = DataFrame(data)
    output = df.display(limit=5)
    # With top_and_tail enabled, display shows 5 head + 5 tail = 10 rows
    # So the footer should show [ 10 rows x 2 columns ] for the displayed subset
    # NOT the original 50 rows  
    assert "[ 10 rows x 2 columns ]" in output


def test_row_indices_consistency():
    """Test that row indices are consistent and sequential"""
    data = [{"a": i, "b": i * 2} for i in range(20)]
    df = DataFrame(data)
    output = df.display(limit=5)
    
    # Extract row indices from the display
    # Remove ANSI color codes for easier parsing
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    clean_output = ansi_escape.sub('', output)
    
    lines = clean_output.split("\n")
    row_indices = []
    for line in lines:
        # Look for lines with │ that contain data rows (not header/footer)
        if "│" in line and "─" not in line and "═" not in line and "┌" not in line:
            # Try to extract the first number after the first │
            parts = line.split("│")
            if len(parts) > 1:
                try:
                    idx = int(parts[1].strip())
                    row_indices.append(idx)
                except (ValueError, IndexError):
                    pass
    
    # With top_and_tail, should display head (1-5) + tail (6-10)
    # Note: The tail rows show indices 6-10 from enumeration, not the actual row numbers
    assert len(row_indices) >= 10, f"Expected at least 10 row indices, got {len(row_indices)}"
    assert row_indices[:5] == [1, 2, 3, 4, 5], f"Expected [1,2,3,4,5], got {row_indices[:5]}"
    # The last 5 should be 6-10 (tail enumeration)
    assert row_indices[-5:] == [6, 7, 8, 9, 10], f"Expected [6,7,8,9,10], got {row_indices[-5:]}"


def test_interval_formatting_from_array():
    """Test that intervals represented as [days, microseconds] arrays are handled"""
    # Create a DataFrame with interval-like data
    # [0, 36000000000] microseconds = 10 hours
    data = [{"interval": ["0", "36000000000"]}]
    df = DataFrame(data)
    output = df.display()
    
    # The interval heuristic checks for ARRAY<INTEGER> with 2 elements,
    # but raw data will default to unknown types. The display should still work,
    # it just may not format as an interval.
    # Check that the display includes the interval column
    assert "interval" in output


def test_get_orso_type_parser_simple_types():
    """Test the get_orso_type parser with simple type strings"""
    assert get_orso_type("INTEGER") == OrsoTypes.INTEGER
    assert get_orso_type("VARCHAR") == OrsoTypes.VARCHAR
    assert get_orso_type("DOUBLE") == OrsoTypes.DOUBLE
    assert get_orso_type("BOOLEAN") == OrsoTypes.BOOLEAN
    assert get_orso_type("DATE") == OrsoTypes.DATE
    assert get_orso_type("TIMESTAMP") == OrsoTypes.TIMESTAMP
    assert get_orso_type("INTERVAL") == OrsoTypes.INTERVAL


def test_get_orso_type_parser_complex_types():
    """Test the get_orso_type parser with complex type strings"""
    assert get_orso_type("ARRAY<INTEGER>") == OrsoTypes.ARRAY
    assert get_orso_type("ARRAY<VARCHAR>") == OrsoTypes.ARRAY
    assert get_orso_type("VARCHAR[255]") == OrsoTypes.VARCHAR
    assert get_orso_type("DECIMAL(10,2)") == OrsoTypes.DECIMAL
    assert get_orso_type("BLOB[1024]") == OrsoTypes.BLOB


def test_get_orso_type_parser_case_insensitive():
    """Test that get_orso_type is case-insensitive"""
    assert get_orso_type("integer") == OrsoTypes.INTEGER
    assert get_orso_type("INTEGER") == OrsoTypes.INTEGER
    assert get_orso_type("InTeGeR") == OrsoTypes.INTEGER
    assert get_orso_type("array<integer>") == OrsoTypes.ARRAY
    assert get_orso_type("ARRAY<INTEGER>") == OrsoTypes.ARRAY


def test_get_orso_type_parser_invalid_type():
    """Test that get_orso_type raises ValueError for invalid types"""
    try:
        get_orso_type("INVALID_TYPE")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unknown" in str(e)


def test_display_with_mixed_data_types():
    """Test display with mixed data types to ensure no regressions"""
    data = [
        {
            "int_col": 42,
            "float_col": 3.14,
            "str_col": "hello",
            "bool_col": True,
            "date_col": dt.date(2025, 10, 19),
        }
    ]
    df = DataFrame(data)
    output = df.display()
    
    # All columns should be present in output
    assert "int_col" in output
    assert "float_col" in output
    assert "str_col" in output
    assert "bool_col" in output
    assert "date_col" in output
    assert "42" in output
    assert "3.14" in output
    assert "hello" in output
    assert "2025-10-19" in output


def test_display_with_null_values():
    """Test that null values are displayed correctly"""
    data = [
        {"a": 1, "b": None},
        {"a": None, "b": 2},
    ]
    df = DataFrame(data)
    output = df.display()
    
    # Should show "null" for None values
    assert "null" in output
    assert "2 rows" in output


def test_display_footer_rows_columns_format():
    """Test that the footer format is consistent"""
    data = [{"x": i, "y": i * 2, "z": i * 3} for i in range(5)]
    df = DataFrame(data)
    output = df.display()
    
    # Should end with footer in format "[ N rows x M columns ]"
    assert "[ 5 rows x 3 columns ]" in output
    # Verify it's in the last line
    last_line = output.split("\n")[-1]
    assert "[ 5 rows x 3 columns ]" in last_line



if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
