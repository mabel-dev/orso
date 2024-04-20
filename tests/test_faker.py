import os
import sys
import pytest


sys.path.insert(1, os.path.join(sys.path[0], ".."))


from orso.schema import RelationSchema, FlatColumn, ColumnDisposition
from orso.types import OrsoTypes

from orso.faker import generate_fake_data, generate_random_row


def test_generate_fake_data():
    # Create a simple schema for the test
    columns = [
        FlatColumn(name="ID", type=OrsoTypes.INTEGER),
        FlatColumn(name="Name", type=OrsoTypes.VARCHAR, disposition=ColumnDisposition.NAME),
        FlatColumn(name="Age", type=OrsoTypes.INTEGER, disposition=ColumnDisposition.AGE),
    ]
    schema = RelationSchema(name="TestSchema", columns=columns)

    # Generate fake data
    df = generate_fake_data(schema, size=50)

    # Test the DataFrame size
    assert df.rowcount == 50

    # Test the row structure based on the schema
    for row in df:
        assert len(row) == len(columns)
        for index, column in enumerate(columns):
            if column.type == OrsoTypes.INTEGER:
                assert isinstance(row[index], int) or row[index] is None
            elif column.type == OrsoTypes.VARCHAR:
                assert isinstance(row[index], str) or row[index] is None


def test_generate_random_row():
    # Create a simple schema for the test
    columns = [
        FlatColumn(name="ID", type=OrsoTypes.INTEGER),
        FlatColumn(name="Name", type=OrsoTypes.VARCHAR, disposition=ColumnDisposition.NAME),
        FlatColumn(name="Age", type=OrsoTypes.INTEGER, disposition=ColumnDisposition.AGE),
    ]
    schema = RelationSchema(name="TestSchema", columns=columns)

    # Generate a random row
    row = generate_random_row(schema)

    # Test the row structure based on the schema
    assert len(row) == len(columns)
    for index, column in enumerate(columns):
        if column.type == OrsoTypes.INTEGER:
            assert isinstance(row[index], int) or row[index] is None
        elif column.type == OrsoTypes.VARCHAR:
            assert isinstance(row[index], str) or row[index] is None


def test_nullable_columns():
    # Create a schema with nullable and non-nullable columns
    columns = [
        FlatColumn(name="Nullable", type=OrsoTypes.BOOLEAN, nullable=True),
        FlatColumn(name="NotNullable", type=OrsoTypes.BOOLEAN, nullable=False),
    ]
    schema = RelationSchema(name="TestNullableSchema", columns=columns)

    # Generate fake data (use a large size for statistical significance)
    df = generate_fake_data(schema, size=10000)

    # Count the frequency of None values
    none_count = {0: 0, 1: 0}
    for row in df:
        for index, value in enumerate(row):
            if value is None:
                none_count[index] += 1

    # Check frequency of None values in nullable column
    assert 70 < none_count[0] < 130, none_count[0]  # Should be close to 1% of 10000
    # Check that non-nullable columns don't contain None
    assert none_count[1] == 0, none_count[1]


def test_fakeable_types():
    import decimal
    import datetime

    # Create a schema with all fakeable types
    columns = [
        FlatColumn(name="IntCol", type=OrsoTypes.INTEGER),
        FlatColumn(name="VarCharCol", type=OrsoTypes.VARCHAR),
        FlatColumn(name="BoolCol", type=OrsoTypes.BOOLEAN),
        FlatColumn(name="DecimalCol", type=OrsoTypes.DECIMAL),
        FlatColumn(name="DoubleCol", type=OrsoTypes.DOUBLE),
        FlatColumn(name="TimestampCol", type=OrsoTypes.TIMESTAMP),
    ]
    schema = RelationSchema(name="TestFakeableTypesSchema", columns=columns)

    # Generate a single row
    row = generate_random_row(schema)

    # Check the data types of the values in the row
    type_checks = {
        OrsoTypes.INTEGER: int,
        OrsoTypes.VARCHAR: str,
        OrsoTypes.BOOLEAN: bool,
        OrsoTypes.DECIMAL: decimal.Decimal,
        OrsoTypes.DOUBLE: float,
        OrsoTypes.TIMESTAMP: datetime.datetime,
    }

    for index, column in enumerate(columns):
        assert isinstance(row[index], type_checks[column.type]) or row[index] is None


def test_unfakeable_types():
    # Create a schema with an unfakeable type (OrsoTypes.STRUCT)
    columns = [
        FlatColumn(name="StructCol", type=OrsoTypes.STRUCT),
    ]
    schema = RelationSchema(name="TestUnfakeableTypesSchema", columns=columns)

    # Attempt to generate a single row and expect an exception
    with pytest.raises(TypeError) as excinfo:
        generate_random_row(schema)

    # Attempt to generate fake data and expect an exception
    with pytest.raises(TypeError) as excinfo:
        generate_fake_data(schema, size=1)


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
