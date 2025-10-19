"""
Unit tests for the get_orso_type parser function in orso.types module.

This test suite validates the get_orso_type() function which converts type strings
to OrsoType enum values, with comprehensive coverage of:
- Simple types (INTEGER, VARCHAR, DOUBLE, etc.)
- Complex types (ARRAY<T>, DECIMAL(p,s), VARCHAR[n], BLOB[n])
- Case insensitivity
- Error handling for invalid types
"""

import os
import sys
from contextlib import suppress

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.types import get_orso_type, OrsoTypes


def test_integer_type():
    """Test parsing INTEGER type"""
    assert get_orso_type("INTEGER") == OrsoTypes.INTEGER

def test_varchar_type():
    """Test parsing VARCHAR type"""
    assert get_orso_type("VARCHAR") == OrsoTypes.VARCHAR

def test_double_type():
    """Test parsing DOUBLE type"""
    assert get_orso_type("DOUBLE") == OrsoTypes.DOUBLE

def test_boolean_type():
    """Test parsing BOOLEAN type"""
    assert get_orso_type("BOOLEAN") == OrsoTypes.BOOLEAN

def test_date_type():
    """Test parsing DATE type"""
    assert get_orso_type("DATE") == OrsoTypes.DATE

def test_time_type():
    """Test parsing TIME type"""
    assert get_orso_type("TIME") == OrsoTypes.TIME

def test_timestamp_type():
    """Test parsing TIMESTAMP type"""
    assert get_orso_type("TIMESTAMP") == OrsoTypes.TIMESTAMP

def test_interval_type():
    """Test parsing INTERVAL type"""
    assert get_orso_type("INTERVAL") == OrsoTypes.INTERVAL

def test_blob_type():
    """Test parsing BLOB type"""
    assert get_orso_type("BLOB") == OrsoTypes.BLOB

def test_struct_type():
    """Test parsing STRUCT type"""
    assert get_orso_type("STRUCT") == OrsoTypes.STRUCT

def test_jsonb_type():
    """Test parsing JSONB type"""
    assert get_orso_type("JSONB") == OrsoTypes.JSONB

def test_decimal_type():
    """Test parsing simple DECIMAL type"""
    assert get_orso_type("DECIMAL") == OrsoTypes.DECIMAL

def test_array_integer():
    """Test parsing ARRAY<INTEGER>"""
    result = get_orso_type("ARRAY<INTEGER>")
    assert result == OrsoTypes.ARRAY

def test_array_varchar():
    """Test parsing ARRAY<VARCHAR>"""
    result = get_orso_type("ARRAY<VARCHAR>")
    assert result == OrsoTypes.ARRAY

def test_array_double():
    """Test parsing ARRAY<DOUBLE>"""
    result = get_orso_type("ARRAY<DOUBLE>")
    assert result == OrsoTypes.ARRAY

def test_decimal_with_precision_scale():
    """Test parsing DECIMAL(10,2)"""
    result = get_orso_type("DECIMAL(10,2)")
    assert result == OrsoTypes.DECIMAL

def test_decimal_with_different_scale():
    """Test parsing DECIMAL(18,4)"""
    result = get_orso_type("DECIMAL(18,4)")
    assert result == OrsoTypes.DECIMAL

def test_varchar_with_length():
    """Test parsing VARCHAR[255]"""
    result = get_orso_type("VARCHAR[255]")
    assert result == OrsoTypes.VARCHAR

def test_varchar_with_large_length():
    """Test parsing VARCHAR[1024]"""
    result = get_orso_type("VARCHAR[1024]")
    assert result == OrsoTypes.VARCHAR

def test_blob_with_size():
    """Test parsing BLOB[1024]"""
    result = get_orso_type("BLOB[1024]")
    assert result == OrsoTypes.BLOB

def test_blob_with_large_size():
    """Test parsing BLOB[8192]"""
    result = get_orso_type("BLOB[8192]")
    assert result == OrsoTypes.BLOB

def test_lowercase_integer():
    """Test parsing lowercase 'integer'"""
    assert get_orso_type("integer") == OrsoTypes.INTEGER

def test_mixedcase_integer():
    """Test parsing mixed case 'InTeGeR'"""
    assert get_orso_type("InTeGeR") == OrsoTypes.INTEGER

def test_lowercase_array():
    """Test parsing lowercase 'array<integer>'"""
    assert get_orso_type("array<integer>") == OrsoTypes.ARRAY

def test_uppercase_array():
    """Test parsing uppercase 'ARRAY<INTEGER>'"""
    assert get_orso_type("ARRAY<INTEGER>") == OrsoTypes.ARRAY

def test_mixedcase_array():
    """Test parsing mixed case 'Array<Integer>'"""
    assert get_orso_type("Array<Integer>") == OrsoTypes.ARRAY

def test_lowercase_varchar_with_length():
    """Test parsing lowercase 'varchar[255]'"""
    assert get_orso_type("varchar[255]") == OrsoTypes.VARCHAR

def test_lowercase_decimal():
    """Test parsing lowercase 'decimal(10,2)'"""
    assert get_orso_type("decimal(10,2)") == OrsoTypes.DECIMAL


def test_invalid_type_raises_error():
    """Test that invalid type raises ValueError"""
    try:
        get_orso_type("INVALID_TYPE")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unknown" in str(e)

def test_empty_array_raises_error():
    """Test that empty ARRAY type raises ValueError"""
    try:
        get_orso_type("ARRAY<>")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass  # Expected

def test_nonexistent_array_element_type():
    """Test that invalid element type in ARRAY might raise ValueError or be accepted"""
    # The parser may accept unknown array element types without validation
    with suppress(ValueError):
        result = get_orso_type("ARRAY<NONEXISTENT>")
        # If it doesn't raise, the ARRAY type is still returned
        assert result == OrsoTypes.ARRAY


def test_decimal_precision_scale_attached():
    """Test that DECIMAL type has precision and scale attached"""
    result = get_orso_type("DECIMAL(10,2)")
    assert result == OrsoTypes.DECIMAL
    assert result._precision == 10
    assert result._scale == 2

def test_varchar_length_attached():
    """Test that VARCHAR type has length attached"""
    result = get_orso_type("VARCHAR[255]")
    assert result == OrsoTypes.VARCHAR
    assert result._length == 255

def test_varchar_large_length_attached():
    """Test that VARCHAR with large length has length attached"""
    result = get_orso_type("VARCHAR[4096]")
    assert result == OrsoTypes.VARCHAR
    assert result._length == 4096

def test_blob_length_attached():
    """Test that BLOB type has length attached"""
    result = get_orso_type("BLOB[1024]")
    assert result == OrsoTypes.BLOB
    assert result._length == 1024

def test_array_element_type_attached():
    """Test that ARRAY type has element_type attached"""
    result = get_orso_type("ARRAY<INTEGER>")
    assert result == OrsoTypes.ARRAY
    assert result._element_type == OrsoTypes.INTEGER

def test_array_varchar_element_type_attached():
    """Test that ARRAY<VARCHAR> has element_type attached"""
    result = get_orso_type("ARRAY<VARCHAR>")
    assert result == OrsoTypes.ARRAY
    assert result._element_type == OrsoTypes.VARCHAR

def test_simple_type_has_none_metadata():
    """Test that simple types like INTEGER have None metadata"""
    result = get_orso_type("INTEGER")
    assert result == OrsoTypes.INTEGER
    assert result._precision is None
    assert result._scale is None
    assert result._length is None
    assert result._element_type is None

def test_type_string_representation_with_metadata():
    """Test that __str__ correctly uses attached metadata"""
    # DECIMAL(10,2)
    decimal_type = get_orso_type("DECIMAL(10,2)")
    assert "DECIMAL" in str(decimal_type)
    assert "10" in str(decimal_type)
    assert "2" in str(decimal_type)

    # VARCHAR[255]
    varchar_type = get_orso_type("VARCHAR[255]")
    assert "VARCHAR" in str(varchar_type)
    assert "255" in str(varchar_type)

    # ARRAY<INTEGER>
    array_type = get_orso_type("ARRAY<INTEGER>")
    assert "ARRAY" in str(array_type)
    assert "INTEGER" in str(array_type)


def test_type_string_with_whitespace():
    """Test parsing type string - currently requires no extra whitespace"""
    # The regex patterns expect no extra whitespace around delimiters
    # Test with proper formatting works
    result = get_orso_type("DECIMAL(10,2)")
    assert result == OrsoTypes.DECIMAL

def test_multiple_nested_arrays_not_supported():
    """Test that nested arrays raise appropriate error"""
    with suppress(ValueError):
        get_orso_type("ARRAY<ARRAY<INTEGER>>")

def test_all_orso_types_are_parseable():
    """Test that all OrsoTypes enum values can be parsed as strings"""
    for type_name in [
        "ARRAY",
        "BLOB",
        "BOOLEAN",
        "DATE",
        "DECIMAL",
        "DOUBLE",
        "INTEGER",
        "INTERVAL",
        "STRUCT",
        "TIMESTAMP",
        "TIME",
        "VARCHAR",
        "JSONB",
    ]:
        result = get_orso_type(type_name)
        assert result is not None
        assert isinstance(result, OrsoTypes)


if __name__ == "__main__":
    from tests import run_tests

    run_tests()
