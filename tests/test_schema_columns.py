import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import datetime
import pyarrow

from orso.types import OrsoTypes

from orso.schema import FlatColumn
from orso.schema import FunctionColumn
from orso.schema import ConstantColumn
from orso.schema import DictionaryColumn

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
    assert fc.type == 0

    with pytest.raises(ValueError):
        FlatColumn(name="able", type="LEFT")


def test_missing_columns():
    with pytest.raises(ColumnDefinitionError):
        FlatColumn(name="brian")


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

    assert sorted(dict_column.dictionary) == ["28", "30", "31"]
    assert list(dict_column.encoding) == [2, 0, 2, 1, 2, 1, 2, 2, 1, 2, 1, 2]

    values = dict_column.materialize()

    assert list(values) == MONTH_LENGTHS


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
