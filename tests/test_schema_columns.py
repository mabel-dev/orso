import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import datetime

from orso.types import OrsoTypes

from orso.schema import FlatColumn
from orso.schema import FunctionColumn
from orso.schema import ConstantColumn
from orso.schema import DictionaryColumn


def test_flat_column_materialize():
    flat_column = FlatColumn("athena", OrsoTypes.INTEGER)

    with pytest.raises(TypeError):
        flat_column.materialize()

    assert flat_column.name == "athena"
    assert flat_column.type == OrsoTypes.INTEGER


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
    test_flat_column_materialize()
    test_function_column()
    test_constant_column()
    test_dict_column()

    print("✅ okay")
