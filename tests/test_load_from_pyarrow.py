import os
import sys

import pyarrow
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame


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
    test_from_arrow_with_single_table()
    test_from_arrow_with_multiple_tables()
    print("✅ okay")
