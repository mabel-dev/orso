import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pyarrow
from orso.types import OrsoTypes

from orso.schema import FlatColumn


def test_column_to_field():
    column = FlatColumn(name="test", type=OrsoTypes.VARCHAR)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.string()

    column = FlatColumn(name="test", type=OrsoTypes.INTEGER)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.int64()

    column = FlatColumn(name="test", type=OrsoTypes.DOUBLE)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.float64()

    column = FlatColumn(name="test", type=OrsoTypes.BOOLEAN)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.bool_()

    column = FlatColumn(name="test", type=OrsoTypes.TIMESTAMP)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.timestamp("us"),  arrow_field.type

    column = FlatColumn(name="test", type=OrsoTypes.DATE)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.date64(), arrow_field.type

    column = FlatColumn(name="test", type=OrsoTypes.BLOB)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.binary()

    column = FlatColumn(name="test", type=OrsoTypes.DECIMAL)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.decimal128(28, 21), arrow_field.type

def test_array_column_to_field():
    column = FlatColumn(name="test", type=OrsoTypes.ARRAY, element_type=OrsoTypes.VARCHAR)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.string())

    column = FlatColumn(name="test", type=OrsoTypes.ARRAY, element_type=OrsoTypes.INTEGER)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.int64())

    column = FlatColumn(name="test", type=OrsoTypes.ARRAY, element_type=OrsoTypes.DOUBLE)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.float64())

    column = FlatColumn(name="test", type=OrsoTypes.ARRAY, element_type=OrsoTypes.BOOLEAN)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.bool_())

    column = FlatColumn(name="test", type=OrsoTypes.ARRAY, element_type=OrsoTypes.TIMESTAMP)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.timestamp("us"))

    column = FlatColumn(name="test", type=OrsoTypes.ARRAY, element_type=OrsoTypes.BLOB)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.binary())


def test_column_to_field_name():
    column = FlatColumn(name="test", type="ARRAY<VARCHAR>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.string())

    column = FlatColumn(name="test", type="ARRAY<INTEGER>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.int64())

    column = FlatColumn(name="test", type="ARRAY<DOUBLE>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.float64())

    column = FlatColumn(name="test", type="ARRAY<BOOLEAN>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.bool_())

    column = FlatColumn(name="test", type="ARRAY<TIMESTAMP>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.timestamp("us"))

    column = FlatColumn(name="test", type="ARRAY<BLOB>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.binary())

def test_field_to_column():
    arrow_field = pyarrow.field("test", pyarrow.list_(pyarrow.int64()))
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.ARRAY
    assert column.element_type == OrsoTypes.INTEGER, column.element_type

    arrow_field = pyarrow.field("test", pyarrow.string())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.VARCHAR
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.int64())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.INTEGER
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.float64())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.DOUBLE
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.bool_())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.BOOLEAN
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.timestamp("us"))
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.TIMESTAMP
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.date32())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.DATE, column.type
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.binary())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.BLOB
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.decimal128(28, 21))
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.DECIMAL
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.list_(pyarrow.string()))
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.ARRAY
    assert column.element_type == OrsoTypes.VARCHAR

    arrow_field = pyarrow.field("test", pyarrow.list_(pyarrow.binary()))
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == OrsoTypes.ARRAY
    assert column.element_type == OrsoTypes.BLOB



if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
