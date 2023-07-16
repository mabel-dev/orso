import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.exceptions import DataValidationError
from orso.schema import RelationSchema
from orso.schema import FlatColumn
from tests import cities


def test_find_column():
    column = cities.schema.find_column("language")
    assert column.name == "language", column.name

    column = cities.schema.find_column("geolocation")
    assert column is None, column


def test_all_column_names():
    column_names = cities.schema.all_column_names()
    assert "name" in column_names
    assert "language" in column_names
    assert "geolocation" not in column_names


def test_schema_persistance():
    as_dict = cities.schema.to_dict()
    from_dict = RelationSchema.from_dict(as_dict)

    assert from_dict == cities.schema
    assert as_dict == from_dict.to_dict()


def test_validate_with_valid_data():
    # Test with valid data
    data = {
        "name": "New York",
        "population": 8623000,
        "country": "United States",
        "founded": "1624",
        "area": 783.8,
        "language": "English",
    }
    assert cities.schema.validate(data) == True


def test_validate_with_missing_column():
    # Test with missing column
    data = {
        "name": "London",
        "population": 8908081,
        "country": "United Kingdom",
        "founded": "43 AD",
        "language": "English",
    }
    with pytest.raises(DataValidationError):
        cities.schema.validate(data)


def test_validate_with_nullable_column():
    # Test with nullable column and value is None
    data = {
        "name": "Paris",
        "population": 2187526,
        "country": "France",
        "founded": None,
        "area": 105.4,
        "language": "French",
    }
    assert cities.schema.validate(data)


def test_validate_with_non_nullable_column():
    # Test with non-nullable column and value is None
    data = {
        "name": None,
        "population": 13929286,
        "country": "Japan",
        "founded": "1457",
        "area": 2187.66,
        "language": "Japanese",
    }
    with pytest.raises(DataValidationError):
        cities.schema.validate(data)


def test_validate_with_wrong_type():
    # Test with column value of wrong type
    data = {
        "name": "Berlin",
        "population": 3769495,
        "country": "Germany",
        "founded": "1237",
        "area": "891.8",  # Expected type is double
        "language": "German",
    }
    with pytest.raises(DataValidationError):
        cities.schema.validate(data)


def test_validate_with_invalid_data_type():
    # Test with invalid data type (not a MutableMapping)
    data = [1, 2, 3]
    with pytest.raises(TypeError):
        cities.schema.validate(data)


def test_schema_iterations():
    schema = cities.schema

    assert schema.num_columns == 6

    for i, column in enumerate(schema.columns):
        assert column == schema.column(i)
        assert column == schema.column(column.name)


import pytest


def test_add_method_combines_columns():
    # Arrange
    col1 = FlatColumn(name="col1", type=0)
    col2 = FlatColumn(name="col2", type=0)
    col3 = FlatColumn(name="col3", type=0)

    schema1 = RelationSchema(name="Schema1", columns=[col1, col2])
    schema2 = RelationSchema(name="Schema2", columns=[col2, col3])

    # Act
    combined_schema = schema1 + schema2

    # Assert
    expected_columns = [col1, col2, col3]
    assert combined_schema.columns == expected_columns


def test_add_method_preserves_original_schemas():
    # Arrange
    col1 = FlatColumn(name="col1", type=0)
    col2 = FlatColumn(name="col2", type=0)
    col3 = FlatColumn(name="col3", type=0)
    schema1 = RelationSchema(name="Schema1", columns=[col1, col2])
    schema2 = RelationSchema(name="Schema2", columns=[col2, col3])

    # Act
    combined_schema = schema1 + schema2

    # Assert
    assert schema1.columns == [col1, col2]
    assert schema2.columns == [col2, col3]


def test_add_method_with_duplicate_columns():
    # Arrange
    col1 = FlatColumn(name="col1", type=0)
    col2 = FlatColumn(name="col2", type=0)
    col3 = FlatColumn(name="col3", type=0)
    schema1 = RelationSchema(name="Schema1", columns=[col1, col2])
    schema2 = RelationSchema(name="Schema2", columns=[col1, col2, col3])

    # Act
    combined_schema = schema1 + schema2

    # Assert
    expected_columns = [col1, col2, col3]
    assert combined_schema.columns == expected_columns


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
