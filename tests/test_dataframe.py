import pytest

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.row import Row
from orso.dataframe import Dataframe

@pytest.fixture
def schema():
    return {
        "A": {"type": int, "nullable": False},
        "B": {"type": str, "nullable": True},
        "C": {"type": float, "nullable": False},
    }

@pytest.fixture
def rows():
    row_factory = Row.create_class({
        "A": {"type": int, "nullable": False},
        "B": {"type": str, "nullable": True},
        "C": {"type": float, "nullable": False},
    })
    return (
        row_factory([1, "a", 1.1]),
        row_factory([2, "b", 2.2]),
        row_factory([3, "c", 3.3]),
        row_factory([4, None, 4.4]),
        row_factory([5, "e", 5.5]),
    )

@pytest.fixture
def dataframe(schema, rows):
    return Dataframe(schema, rows)

def test_dataframe_materialize(dataframe):
    dataframe.materialize()
    assert isinstance(dataframe._rows, list)

def test_dataframe_collect(dataframe):
    result = dataframe.collect(["A", "C"])
    assert result == ([1, 2, 3, 4, 5], [1.1, 2.2, 3.3, 4.4, 5.5])

def test_dataframe_slice(dataframe):
    result = dataframe.slice(offset=1, length=2)
    assert len(result) == 2

def test_dataframe_iter(dataframe):
    assert len(list(dataframe)) == 5

def test_dataframe_len(dataframe):
    assert len(dataframe) == 5
