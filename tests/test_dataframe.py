import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame
from orso.row import Row


@pytest.fixture
def schema():
    return {
        "A": {"type": int, "nullable": False},
        "B": {"type": str, "nullable": True},
        "C": {"type": float, "nullable": False},
    }


@pytest.fixture
def rows():
    row_factory = Row.create_class(
        {
            "A": {"type": int, "nullable": False},
            "B": {"type": str, "nullable": True},
            "C": {"type": float, "nullable": False},
        }
    )
    return (
        row_factory([1, "a", 1.1]),
        row_factory([2, "b", 2.2]),
        row_factory([3, "c", 3.3]),
        row_factory([4, None, 4.4]),
        row_factory([5, "e", 5.5]),
    )


@pytest.fixture
def dataframe(schema, rows):
    return DataFrame(rows=rows, schema=schema)


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


def test_dataframe_user_init():
    # fmt:off
    cities = [
        {"name": "Tokyo", "population": 13929286, "country": "Japan", "founded": "1457", "area": 2191, "language": "Japanese"},
        {"name": "London", "population": 8982000, "country": "United Kingdom", "founded": "43 AD", "area": 1572, "language": "English"},
        {"name": "New York City", "population": 8399000, "country": "United States", "founded": "1624", "area": 468.9, "language": "English"},
        {"name": "Mumbai", "population": 18500000, "country": "India", "founded": "7th century BC", "area": 603.4, "language": "Hindi, English"},
        {"name": "Cape Town", "population": 433688, "country": "South Africa", "founded": "1652", "area": 400, "language": "Afrikaans, English"},
    ]
    # fmt:on
    df = DataFrame(cities)
    assert df.column_names == ("name", "population", "country", "founded", "area", "language")
    assert df.rowcount == 5


test_dataframe_user_init()
