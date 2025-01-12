import os
import sys

import pyarrow
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import orso
from orso.dataframe import DataFrame
from orso.exceptions import DataValidationError
from orso.row import Row
from tests import cities
from orso.schema import RelationSchema, FlatColumn
from orso.types import OrsoTypes


def create_schema():
    return RelationSchema(
        name="dataset",
        columns=[
            FlatColumn(name="A", type=OrsoTypes.INTEGER, nullable=False),
            FlatColumn(name="B", type=OrsoTypes.VARCHAR),
            FlatColumn(name="C", type=OrsoTypes.DOUBLE, nullable=False),
        ],
    )


def create_rows():
    row_factory = Row.create_class(schema=create_schema())
    return [
        row_factory([1, "a", 1.1]),
        row_factory([2, "b", 2.2]),
        row_factory([3, "c", 3.3]),
        row_factory([4, None, 4.4]),
        row_factory([5, "e", 5.5]),
    ]


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
    assert result.tolist() == [[1, 2, 3, 4, 5], [1.1, 2.2, 3.3, 4.4, 5.5]], result

    result = dataframe.collect("A")
    assert result.tolist() == [1, 2, 3, 4, 5], result


def test_dataframe_get_item():
    dataframe = create_dataframe()
    result = dataframe["A", "C"]
    assert result.tolist() == [[1, 2, 3, 4, 5], [1.1, 2.2, 3.3, 4.4, 5.5]], result

    result = dataframe[0, 2]
    assert result.tolist() == [[1, 2, 3, 4, 5], [1.1, 2.2, 3.3, 4.4, 5.5]], result

    result = dataframe["A", 2]
    assert result.tolist() == [[1, 2, 3, 4, 5], [1.1, 2.2, 3.3, 4.4, 5.5]], result

    result = dataframe["A"]
    assert result.tolist() == [1, 2, 3, 4, 5], result

    result = dataframe[0]
    assert result.tolist() == [1, 2, 3, 4, 5], result


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
    df = DataFrame(cities.values)
    assert df.column_names == ("name", "population", "country", "founded", "area", "language")
    assert df.rowcount == 20, df


def test_dataframe_head():
    df = DataFrame(cities.values)

    # Test default head() behavior (first 5 rows)
    head = df.head()
    assert head.rowcount == 5
    assert head.row(0)[0] == "Tokyo"

    # Test head() with size parameter
    head = df.head(3)
    assert head.rowcount == 3
    assert head.row(0)[0] == "Tokyo"


def test_dataframe_tail():
    df = DataFrame(cities.values)

    # Test default tail() behavior (last 5 rows)
    head = df.tail()
    assert head.rowcount == 5
    assert head.row(0)[0] == "Dubai", head.row(0)[0]

    # Test tail() with size parameter
    head = df.tail(3)
    assert head.rowcount == 3
    assert head.row(0)[0] == "Stockholm", head.row(0)[0]


def test_dataframe_filter():
    # Filter rows where column A is greater than 2
    dataframe = create_dataframe()
    mask = [row[0] > 2 for row in dataframe]
    filtered_dataframe = dataframe.filter(mask)
    assert len(filtered_dataframe) == 3
    assert filtered_dataframe.collect("A").tolist() == [3, 4, 5], filtered_dataframe.collect(["A"])


def test_take():
    # Create a DataFrame object with some data
    df = create_dataframe()

    # Test taking a subset of rows
    indexes = [0, 2, 4]
    expected_rows = [
        (1, "a", 1.1),
        (3, "c", 3.3),
        (5, "e", 5.5),
    ]
    result = df.take(indexes)
    result_rows = result.fetchall()
    assert result_rows == expected_rows, result_rows

    # Test taking all rows in the DataFrame
    indexes = [0, 1, 2, 3, 4]

    expected_rows = [
        (1, "a", 1.1),
        (2, "b", 2.2),
        (3, "c", 3.3),
        (4, None, 4.4),
        (5, "e", 5.5),
    ]
    result = df.take(indexes)
    result_rows = result.fetchall()
    assert result_rows == expected_rows, result_rows

    # Test taking an empty subset of rows
    indexes = []
    expected_rows = []
    result = df.take(indexes)
    assert result.fetchall() == expected_rows


def test_dataframe_hash():
    df1 = create_dataframe()
    df2 = create_dataframe()
    df3 = DataFrame(schema=create_schema(), rows=create_rows()[:4])

    assert hash(df1) == hash(df1)
    assert hash(df1) == hash(df2)
    assert hash(df1) != hash(df3)


def test_to_arrow():
    # Create a DataFrame
    df = create_dataframe()

    # Convert to PyArrow
    arrow_table = df.arrow()

    # Check the column names
    assert arrow_table.column_names == ["A", "B", "C"]

    # Check the number of rows
    assert arrow_table.num_rows == 5

    # Check the schema
    assert arrow_table.schema == pyarrow.schema(
        [("A", pyarrow.int64()), ("B", pyarrow.string()), ("C", pyarrow.float64())]
    )

    # Check the values
    expected_values = [(1, "a", 1.1), (2, "b", 2.2), (3, "c", 3.3), (4, None, 4.4), (5, "e", 5.5)]
    for i, col in enumerate(arrow_table.itercolumns()):
        assert col.to_pylist() == [v[i] for v in expected_values]


def test_to_arrow_with_size():
    df = create_dataframe()
    table = df.arrow(size=4)
    assert table.num_rows == 4, table

    df = create_dataframe()
    table = df.arrow(size=0)
    assert table.num_rows == 0, table


def test_appending():
    df = orso.DataFrame(schema=cities.schema)

    assert len(df) == 0
    # valid data item
    df.append(
        {
            "name": "Perth",
            "population": 2059484,
            "country": "Australia",
            "founded": "1829",
            "area": 6412.3,
            "language": "English",
        }
    )
    assert len(df) == 1

    # founded is nullable
    df.append(
        {
            "name": "Hobart",
            "population": 240342,
            "country": "Australia",
            "founded": None,  # actually 1804,
            "area": 1357.7,
            "language": "English",
        }
    )
    # country is not nullable
    with pytest.raises(DataValidationError):
        df.append(
            {
                "name": "Darwin",
                "population": 147255,
                "country": None,
                "founded": "1869",
                "area": 112.01,
                "language": "English",
            }
        )

    # population is an int
    with pytest.raises(DataValidationError):
        df.append(
            {
                "name": "Brisbane",
                "population": "2470394",  # is an int
                "country": "Australia",
                "founded": "1824",
                "area": 5905.9,
                "language": "English",
            }
        )


def test_profile():
    df = create_dataframe()
    profile = df.profile.to_dataframe()
    assert isinstance(profile, DataFrame)


def test_build_and_then_profile():
    df = orso.DataFrame(schema=cities.schema)
    for city in cities.values:
        df.append(city)

    p = df.profile.to_dataframe()
    assert p.rowcount == df.columncount
    assert p.collect("count").tolist() == [df.rowcount] * df.columncount


def test_describe():
    df = create_dataframe()
    desc = df.description
    assert len(desc) == df.columncount
    assert desc == [
        ("A", "INTEGER", None, None, None, None, False),
        ("B", "VARCHAR", None, None, None, None, True),
        ("C", "DOUBLE", None, None, None, None, False),
    ]


def test_distinct():
    df = orso.DataFrame(schema=cities.schema)
    for city in cities.values:
        df.append(city)
    for city in cities.values:
        df.append(city)

    assert df.distinct().rowcount == len(cities.values)


def test_to_map_and_to_dict():
    df = orso.DataFrame(cities.values)
    for row in df:
        assert isinstance(row, tuple)
        assert isinstance(row.as_map, tuple)
        assert isinstance(row.as_dict, dict)
        assert dict(row.as_map) == row.as_dict


def test_group_by():
    import opteryx

    gb = opteryx.query("SELECT * FROM $satellites").group_by("planetId").count()
    assert gb.shape == (7, 2)


def test_adding_dicts_in_wrong_order():
    df = orso.DataFrame(
        schema=RelationSchema(
            name="test",
            columns=[
                FlatColumn(name="column_1", type=OrsoTypes.INTEGER),
                FlatColumn(name="column_2", type=OrsoTypes.VARCHAR),
            ],
        )
    )

    # insert records with the dicts in different orders
    df.append({"column_1": 1, "column_2": "one"})
    df.append({"column_2": "two", "column_1": 2})

    # we should have them in the correct order when we extract them
    assert df.collect(["column_1", "column_2"]).tolist() == [[1, 2], ["one", "two"]], df.collect(
        ["column_1", "column_2"]
    )


def test_bytes():
    import opteryx
    import time

    gb = opteryx.query("SELECT * FROM $astronauts")
    t = time.monotonic_ns()
    first_bytes = gb.nbytes()
    first = time.monotonic_ns() - t
    t = time.monotonic_ns()
    second_bytes = gb.nbytes()
    second = time.monotonic_ns() - t

    assert first_bytes == second_bytes
    assert first > second


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
