import os
import sys

import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame
from orso.row import Row

import orso

print(dir(orso))


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
        {"name": "Paris", "population": 2148000, "country": "France", "founded": "3rd century BC", "area": 105.4, "language": "French"},
        {"name": "Beijing", "population": 21710000, "country": "China", "founded": "1045", "area": 16410.54, "language": "Mandarin"},
        {"name": "Rio de Janeiro", "population": 6747815, "country": "Brazil", "founded": "1 March 1565", "area": 1264, "language": "Portuguese"}
    ]
    # fmt:on
    df = DataFrame(cities)
    assert df.column_names == ("name", "population", "country", "founded", "area", "language")
    assert df.rowcount == 8


def test_dataframe_head():
    # fmt:off
    cities = [
        {"name": "Tokyo", "population": 13929286, "country": "Japan", "founded": "1457", "area": 2191, "language": "Japanese"},
        {"name": "London", "population": 8982000, "country": "United Kingdom", "founded": "43 AD", "area": 1572, "language": "English"},
        {"name": "New York City", "population": 8399000, "country": "United States", "founded": "1624", "area": 468.9, "language": "English"},
        {"name": "Mumbai", "population": 18500000, "country": "India", "founded": "7th century BC", "area": 603.4, "language": "Hindi, English"},
        {"name": "Cape Town", "population": 433688, "country": "South Africa", "founded": "1652", "area": 400, "language": "Afrikaans, English"},
        {"name": "Paris", "population": 2148000, "country": "France", "founded": "3rd century BC", "area": 105.4, "language": "French"},
        {"name": "Beijing", "population": 21710000, "country": "China", "founded": "1045", "area": 16410.54, "language": "Mandarin"},
        {"name": "Rio de Janeiro", "population": 6747815, "country": "Brazil", "founded": "1 March 1565", "area": 1264, "language": "Portuguese"}
    ]
    # fmt:on
    df = DataFrame(cities)

    # Test default head() behavior (first 5 rows)
    head = df.head()
    assert head.rowcount == 5
    assert head.row(0)[0] == "Tokyo"

    # Test head() with size parameter
    head = df.head(3)
    assert head.rowcount == 3
    assert head.row(0)[0] == "Tokyo"


def test_dataframe_tail():
    # fmt:off
    cities = [
        {"name": "Tokyo", "population": 13929286, "country": "Japan", "founded": "1457", "area": 2191, "language": "Japanese"},
        {"name": "London", "population": 8982000, "country": "United Kingdom", "founded": "43 AD", "area": 1572, "language": "English"},
        {"name": "New York City", "population": 8399000, "country": "United States", "founded": "1624", "area": 468.9, "language": "English"},
        {"name": "Mumbai", "population": 18500000, "country": "India", "founded": "7th century BC", "area": 603.4, "language": "Hindi, English"},
        {"name": "Cape Town", "population": 433688, "country": "South Africa", "founded": "1652", "area": 400, "language": "Afrikaans, English"},
        {"name": "Paris", "population": 2148000, "country": "France", "founded": "3rd century BC", "area": 105.4, "language": "French"},
        {"name": "Beijing", "population": 21710000, "country": "China", "founded": "1045", "area": 16410.54, "language": "Mandarin"},
        {"name": "Rio de Janeiro", "population": 6747815, "country": "Brazil", "founded": "1 March 1565", "area": 1264, "language": "Portuguese"}
    ]
    # fmt:on
    df = DataFrame(cities)

    # Test default tail() behavior (last 5 rows)
    head = df.tail()
    assert head.rowcount == 5
    assert head.row(0)[0] == "Mumbai"

    # Test tail() with size parameter
    head = df.tail(3)
    assert head.rowcount == 3
    assert head.row(0)[0] == "Paris"


def test_dataframe_filter():
    # Filter rows where column A is greater than 2
    dataframe = create_dataframe()
    mask = [row[0] > 2 for row in dataframe]
    filtered_dataframe = dataframe.filter(mask)
    assert len(filtered_dataframe) == 3
    assert filtered_dataframe.collect(["A"]) == ([3, 4, 5],)


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
    assert result._schema == create_schema()
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
    assert result._schema == create_schema()
    result_rows = result.fetchall()
    assert result_rows == expected_rows, result_rows

    # Test taking an empty subset of rows
    indexes = []
    expected_rows = []
    result = df.take(indexes)
    assert result._schema == create_schema()
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
    table = df.arrow(size=3)
    assert table.num_rows == 3, table

    df = create_dataframe()
    table = df.arrow(size=0)
    assert table.num_rows == 0, table


if __name__ == "__main__":  # prgama: nocover
    test_dataframe_materialize()
    test_dataframe_collect()
    test_dataframe_slice()
    test_dataframe_iter()
    test_dataframe_len()
    test_dataframe_user_init()
    test_dataframe_filter()
    test_take()
    test_dataframe_hash()
    test_to_arrow()
    test_to_arrow_with_size()
    test_dataframe_head()
    test_dataframe_tail()

    print("✅ okay")
