import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import datetime
import pyarrow
import numpy

from orso.types import OrsoTypes

from orso.schema import FlatColumn
from orso.schema import FunctionColumn
from orso.schema import ConstantColumn
from orso.schema import DictionaryColumn
from orso.schema import SparseColumn
from orso.schema import RLEColumn

from orso.exceptions import ColumnDefinitionError


def test_flat_column_basic_init():
    column = FlatColumn(name="test", type=OrsoTypes.VARCHAR)
    assert column.name == "test"
    assert column.type == OrsoTypes.VARCHAR
    assert column.description is None
    assert column.aliases == []
    assert column.nullable is True


def test_flat_column_custom_values():
    column = FlatColumn(
        name="test",
        type="varchar",
        description="description",
        aliases=["alias1", "alias2"],
        nullable=False,
        precision=10,
        scale=5,
    )
    assert column.name == "test"
    assert column.type == OrsoTypes.VARCHAR
    assert column.description == "description"
    assert column.aliases == ["alias1", "alias2"]
    assert column.nullable is False
    assert column.precision == 10
    assert column.scale == 5


def test_flat_column_with_deprecated_type_list():
    with pytest.warns(UserWarning, match="Column type LIST will be deprecated"):
        column = FlatColumn(name="test", type="list")
    assert column.type == OrsoTypes.ARRAY


def test_flat_column_with_deprecated_type_numeric():
    with pytest.warns(UserWarning, match="Column type NUMERIC will be deprecated"):
        column = FlatColumn(name="test", type="numeric")
    assert column.type == OrsoTypes.DOUBLE


def test_flat_column_with_unknown_type():
    with pytest.raises(ValueError, match="Unknown column type"):
        FlatColumn(name="test", type="unknown")


def test_flat_column_materialize():
    flat_column = FlatColumn(name="athena", type=OrsoTypes.INTEGER)

    with pytest.raises(TypeError):
        flat_column.materialize()

    assert flat_column.name == "athena"
    assert flat_column.type == OrsoTypes.INTEGER


def test_flat_column_string_representation():
    column = FlatColumn(name="test", type=OrsoTypes.VARCHAR)
    assert str(column) == column.identity


def test_flat_column_missing_attribute():
    with pytest.raises(ColumnDefinitionError):
        FlatColumn()  # Missing required attributes like name and type


def test_columns_with_unknown_parameters():
    FlatColumn(name="athena", type=OrsoTypes.INTEGER, alpha="betty")
    FunctionColumn(name="aries", type=OrsoTypes.DATE, binding=datetime.date.today, sketty="yum")


def test_column_with_valid_default():
    col = FlatColumn(name="valid", type=OrsoTypes.INTEGER, default="1")
    assert col.default == 1

    col = FlatColumn(name="valid", type=OrsoTypes.INTEGER, default=1)
    assert col.default == 1


def test_column_with_invalid_default():
    with pytest.raises(ValueError):
        FlatColumn(name="invalid", type=OrsoTypes.INTEGER, default="green")


def test_flat_column_from_arrow():
    field_name = "test_field"
    arrow_type = pyarrow.string()
    nullable = True
    arrow_field = pyarrow.field(field_name, arrow_type, nullable)

    # Convert the arrow_field to a FlatColumn object
    column = FlatColumn.from_arrow(arrow_field)

    # Check that the name is correctly transferred
    assert column.name == arrow_field.name

    # Check that the type is correctly mapped
    assert column.type == OrsoTypes.VARCHAR, column.type

    # Check that the nullable property is correctly transferred
    assert column.nullable == arrow_field.nullable


def test_column_type_mapping():
    fc = FlatColumn(name="athena", type="INTEGER")
    assert fc.type == OrsoTypes.INTEGER
    assert fc.type.__class__ == OrsoTypes

    fc = FlatColumn(name="athens", type="LIST")
    assert fc.type == OrsoTypes.ARRAY, fc.type

    fc = FlatColumn(name="athens", type="NUMERIC")
    assert fc.type == OrsoTypes.DOUBLE, fc.type

    fc = FlatColumn(name="athled", type=0)
    assert fc.type == 0, fc.type

    with pytest.raises(ValueError):
        FlatColumn(name="able", type="LEFT")


def test_missing_column_missing_name():
    with pytest.raises(ColumnDefinitionError):
        FlatColumn()


def test_type_checks():
    from decimal import Decimal
    from orso.schema import RelationSchema

    TEST_DATA = {
        OrsoTypes.VARCHAR: "string",
        OrsoTypes.INTEGER: 100,
        OrsoTypes.BOOLEAN: True,
        OrsoTypes.DATE: datetime.date.today(),
        OrsoTypes.ARRAY: ["a", "b", "c"],
        OrsoTypes.DOUBLE: 10.00,
        OrsoTypes.TIMESTAMP: datetime.datetime.utcnow(),
        OrsoTypes.TIME: datetime.time.min,
        OrsoTypes.BLOB: b"blob",
        OrsoTypes.DECIMAL: Decimal("3.7"),
        OrsoTypes.STRUCT: {"a": 1},
        OrsoTypes.INTERVAL: datetime.timedelta(days=1),
    }

    columns = []
    for t, v in TEST_DATA.items():
        columns.append(FlatColumn(name=str(t), type=t))

    schema = RelationSchema(name="test", columns=columns)
    schema.validate({str(k): v for k, v in TEST_DATA.items()})


def test_function_column():
    func_column = FunctionColumn(name="aries", type=OrsoTypes.DATE, binding=datetime.date.today)

    assert func_column.name == "aries"
    assert func_column.type == OrsoTypes.DATE
    assert func_column.length == 1
    assert func_column.aliases == []
    assert func_column.description == None

    values = func_column.materialize()

    assert len(values) == 1
    assert values[0] == datetime.date.today()

    func_column.length = 10
    values = func_column.materialize()

    assert len(values) == 10
    assert all(v == datetime.date.today() for v in values)


def test_constant_column():
    FACT: str = "The god of merchants, shepherds and messengers."

    const_column = ConstantColumn(name="hermes", type=OrsoTypes.VARCHAR, value=FACT)

    assert const_column.name == "hermes"
    assert const_column.type == OrsoTypes.VARCHAR
    assert const_column.value == FACT
    assert const_column.length == 1
    assert const_column.aliases == []
    assert const_column.description == None

    values = const_column.materialize()

    assert len(values) == 1
    assert values[0] == FACT

    const_column.length = 10
    values = const_column.materialize()

    assert len(values) == 10
    assert all(v == FACT for v in values)


def test_dict_column():
    MONTH_LENGTHS: list = ["31", "28", "31", "30", "31", "30", "31", "31", "30", "31", "30", "31"]

    dict_column = DictionaryColumn(name="pan", type=OrsoTypes.VARCHAR, values=MONTH_LENGTHS)

    assert dict_column.name == "pan"
    assert dict_column.type == OrsoTypes.VARCHAR

    assert sorted(dict_column.values) == ["28", "30", "31"]
    assert list(dict_column.encoding) == [2, 0, 2, 1, 2, 1, 2, 2, 1, 2, 1, 2]

    values = dict_column.materialize()

    assert list(values) == MONTH_LENGTHS


def test_rle_column():
    SEASON_LENGTHS: list = ["31", "30", "31", "31", "30", "31", "31", "31", "31"]

    rle_column = RLEColumn(name="season", type=OrsoTypes.VARCHAR, values=SEASON_LENGTHS)

    assert rle_column.name == "season"
    assert rle_column.type == OrsoTypes.VARCHAR

    assert numpy.array_equal(rle_column.values, ["31", "30", "31", "30", "31"])
    assert rle_column.lengths == [1, 1, 2, 1, 4]

    values = rle_column.materialize()

    assert list(values) == SEASON_LENGTHS


def test_sparse_column():
    VARYING_LENGTHS: list = ["31", None, "31", None, None, "31", "30", "31", None]

    sparse_column = SparseColumn(name="varying", type=OrsoTypes.VARCHAR, values=VARYING_LENGTHS)

    assert sparse_column.name == "varying"
    assert sparse_column.type == OrsoTypes.VARCHAR

    assert numpy.array_equal(sparse_column.values, numpy.array(["31", "31", "31", "30", "31"]))
    assert numpy.array_equal(sparse_column.indices, numpy.array([0, 2, 5, 6, 7]))

    values = sparse_column.materialize()

    assert numpy.array_equal(values, numpy.array(VARYING_LENGTHS))


import numpy


def test_sparse_column_multiply():
    # Initialize the sparse column
    original_values = [1, None, 2, None, None, 3, 4, 5, None]
    sparse_col = SparseColumn(name="test", type=OrsoTypes.INTEGER, values=original_values)

    # Perform the operation on compressed values
    sparse_col.values = sparse_col.values * 2

    # Materialize and compare
    materialized_values = sparse_col.materialize()
    expected_values = [v * 2 if v is not None else None for v in original_values]

    # Convert to numpy array for direct comparison
    expected_values_np = numpy.array(expected_values, dtype=object)

    numpy.testing.assert_array_equal(materialized_values, expected_values_np)


# Constant Column Test
def test_constant_column_multiply():
    constant_col = ConstantColumn(name="const", type=OrsoTypes.INTEGER, length=5, value=3)
    constant_col.values *= 2
    materialized_values = constant_col.materialize()
    expected_values_np = numpy.full((5,), 6)
    numpy.testing.assert_array_equal(materialized_values, expected_values_np)


# Dictionary Column Test
def test_dictionary_column_multiply():
    original_values = [1, 3, 2, 2, 3, 1]
    dict_col = DictionaryColumn(name="dict", type=OrsoTypes.INTEGER, values=original_values)
    dict_col.values = dict_col.values * 2
    materialized_values = dict_col.materialize()
    expected_values = [v * 2 for v in original_values]
    expected_values_np = numpy.array(expected_values)
    numpy.testing.assert_array_equal(materialized_values, expected_values_np)


# RLE Column Test
def test_rle_column_multiply():
    original_values = [1, 1, 2, 2, 3, 3]
    rle_col = RLEColumn(name="rle", type=OrsoTypes.INTEGER, values=original_values)
    rle_col.values *= 2
    materialized_values = rle_col.materialize()
    expected_values = [v * 2 for v in original_values]
    expected_values_np = numpy.array(expected_values)
    numpy.testing.assert_array_equal(materialized_values, expected_values_np)


def test_to_flatcolumn_basic():
    """
    Test that to_flatcolumn returns a new FlatColumn object.
    """
    flat_column = FlatColumn(
        name="id",
        type=OrsoTypes.INTEGER,
        description="An ID column",
        aliases=["ID"],
        nullable=False,
        precision=5,
    )

    new_column = flat_column.to_flatcolumn()
    assert isinstance(new_column, FlatColumn)
    assert new_column is not flat_column


def test_to_flatcolumn_from_function_column():
    """
    Test that to_flatcolumn returns a new FlatColumn object.
    """
    func_column = FunctionColumn(
        name="virtual_id", type=OrsoTypes.INTEGER, binding=lambda: 10, length=10
    )

    new_column = func_column.to_flatcolumn()
    assert isinstance(new_column, FlatColumn)
    assert new_column is not func_column


def test_to_flatcolumn_preserve_attributes():
    """
    Test that to_flatcolumn preserves the attributes.
    """
    flat_column = FlatColumn(
        name="id",
        type=OrsoTypes.INTEGER,
        element_type=OrsoTypes.INTEGER,
        description="An ID column",
        aliases=["ID"],
        nullable=False,
        precision=5,
    )

    new_column = flat_column.to_flatcolumn()

    for field in [
        f
        for f in dir(FlatColumn)
        if (f[0] != "_" and isinstance(getattr(flat_column, f), (int, str, float, list, OrsoTypes)))
    ]:
        assert getattr(new_column, field) == getattr(flat_column, field), field


def test_to_json_and_back():
    """
    Test that to_flatcolumn preserves the attributes.
    """
    flat_column = FlatColumn(
        name="id",
        type=OrsoTypes.INTEGER,
        description="An ID column",
        aliases=["ID"],
        nullable=False,
        precision=5,
    )
    as_json = flat_column.to_json()
    as_column = FlatColumn.from_json(as_json)

    for field in [
        f
        for f in dir(FlatColumn)
        if (f[0] != "_" and isinstance(getattr(flat_column, f), (int, str, float, list, OrsoTypes)))
    ]:
        assert getattr(as_column, field) == getattr(flat_column, field), field


def test_aliasing():
    col = FlatColumn(name="alpha", type=OrsoTypes.VARCHAR)

    assert col.all_names == ["alpha"]

    col.aliases = ["beta"]
    assert set(col.all_names) == {"alpha", "beta"}

    col.aliases = ["gamma", "delta"]
    assert set(col.all_names) == {"alpha", "gamma", "delta"}


def test_minimum_definition():
    col = FlatColumn(name="a")


def test_arrow_conversion():
    from tests.cities import schema as city_schema
    from pyarrow import schema as arrow_schema

    _arrow_schema = arrow_schema([col.arrow_field for col in city_schema.columns])

    print(_arrow_schema)


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
