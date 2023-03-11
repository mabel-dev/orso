import os
import sys

import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame
from orso import converters


def test_from_arrow():
    # Create some sample data
    table1 = pyarrow.Table.from_arrays(
        [
            pyarrow.array([1, 2, 3]),
            pyarrow.array(["a", "b", "c"]),
            pyarrow.array([True, False, True]),
        ],
        ["id", "name", "active"],
    )
    table2 = pyarrow.Table.from_arrays(
        [
            pyarrow.array([4, 5, 6]),
            pyarrow.array(["d", "e", "f"]),
            pyarrow.array([False, True, False]),
        ],
        ["id", "name", "active"],
    )
    tables = [table1, table2]

    # Test the function with a limit of 4 rows
    result = converters.from_arrow(tables, 4)

    # Verify that the result has the correct number of rows and columns
    assert len(result) == 4
    assert len(result.column_names) == 3

    # Verify that the result has the correct column names and data types
    assert result.column_names == ("id", "name", "active")


def test_from_arrow_with_single_table():
    # Create a PyArrow table with some data
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    schema = pyarrow.schema([("name", pyarrow.string()), ("age", pyarrow.int64())])
    table = pyarrow.Table.from_pydict({f.name: [r[i] for r in data] for i, f in enumerate(schema)})

    # Create an instance of MyClass from the PyArrow table
    obj = DataFrame.from_arrow(table)

    # Check that the instance has the correct rows and schema
    expected_rows = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    expected_schema = {
        "name": {"type": str, "nullable": True},
        "age": {"type": int, "nullable": True},
    }
    obj.materialize()
    assert obj._rows == expected_rows, obj._rows
    assert obj._schema == expected_schema, obj._schema


def test_from_arrow_with_multiple_tables():
    # Create two PyArrow tables with some data
    data1 = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    data2 = [("Dan", 40), ("Eve", 45), ("Frank", 50)]
    data3 = [("Ava", 15), ("Ethan", 20), ("Olivia", 55)]
    schema1 = pyarrow.schema([("name", pyarrow.string()), ("age", pyarrow.int64())])
    schema2 = pyarrow.schema([("name", pyarrow.string()), ("age", pyarrow.int64())])
    schema3 = pyarrow.schema([("name", pyarrow.string()), ("age", pyarrow.int64())])
    table1 = pyarrow.Table.from_pydict(
        {f.name: [r[i] for r in data1] for i, f in enumerate(schema1)}
    )
    table2 = pyarrow.Table.from_pydict(
        {f.name: [r[i] for r in data2] for i, f in enumerate(schema2)}
    )
    table3 = pyarrow.Table.from_pydict(
        {f.name: [r[i] for r in data3] for i, f in enumerate(schema3)}
    )

    # Create an instance of MyClass from a generator of PyArrow tables
    tables = (table1, table2, table3)
    obj = DataFrame.from_arrow(tables)

    # Check that the instance has the correct rows and schema
    expected_rows = [
        ("Alice", 25),
        ("Bob", 30),
        ("Charlie", 35),
        ("Dan", 40),
        ("Eve", 45),
        ("Frank", 50),
        ("Ava", 15),
        ("Ethan", 20),
        ("Olivia", 55),
    ]
    expected_schema = {
        "name": {"type": str, "nullable": True},
        "age": {"type": int, "nullable": True},
    }
    obj.materialize()
    assert obj._rows == expected_rows, obj._rows
    assert obj._schema == expected_schema


if __name__ == "__main__":  # pragma: no cover
    test_from_arrow()
    test_from_arrow_with_single_table()
    test_from_arrow_with_multiple_tables()
    print("✅ okay")
