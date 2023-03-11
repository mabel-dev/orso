import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame
from orso.row import Row


def create_schema():
    return {
        "A": {"type": int, "nullable": False},
        "B": {"type": str, "nullable": True},
        "C": {"type": float, "nullable": False},
    }


def create_rows():
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


def create_dataframe():
    schema = create_schema()
    rows = create_rows()
    return DataFrame(rows=rows, schema=schema)


def test_dataframe_materialize():
    dataframe = create_dataframe()
    dataframe.materialize()
    assert isinstance(dataframe._rows, list)


def test_dataframe_collect():
    dataframe = create_dataframe()
    result = dataframe.collect(["A", "C"])
    assert result == ([1, 2, 3, 4, 5], [1.1, 2.2, 3.3, 4.4, 5.5])


def test_dataframe_slice():
    dataframe = create_dataframe()
    result = dataframe.slice(offset=1, length=2)
    assert len(result) == 2


def test_dataframe_iter():
    dataframe = create_dataframe()
    assert len(list(dataframe)) == 5


def test_dataframe_len():
    dataframe = create_dataframe()
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


def test_dataframe_filter():
    # Filter rows where column A is greater than 2
    dataframe = create_dataframe()
    mask = [row[0] > 2 for row in dataframe]
    filtered_dataframe = dataframe.filter(mask)
    assert len(filtered_dataframe) == 3
    assert filtered_dataframe.collect(["A"]) == ([3, 4, 5],)


if __name__ == "__main__":  # prgama: nocover
    test_dataframe_materialize()
    test_dataframe_collect()
    test_dataframe_slice()
    test_dataframe_iter()
    test_dataframe_len()
    test_dataframe_user_init()
    test_dataframe_filter()

    print("✅ okay")
