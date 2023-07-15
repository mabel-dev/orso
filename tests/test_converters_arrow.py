import os
import sys

import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], ".."))


from orso import converters
from orso.dataframe import DataFrame
from orso.types import OrsoTypes


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
    rows, schema = converters.from_arrow(tables, 4)
    result = DataFrame(rows=rows, schema=schema)

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
    rows, schema = converters.from_arrow(table)
    obj = DataFrame(rows=rows, schema=schema)

    # Check that the instance has the correct rows and schema
    expected_rows = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    expected_columns = ["name", "age"]
    expected_types = [OrsoTypes.VARCHAR, OrsoTypes.INTEGER]
    obj.materialize()
    assert obj._rows == expected_rows, obj._rows
    assert [c.name for c in obj._schema.columns] == expected_columns
    assert [c.type for c in obj._schema.columns] == expected_types


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
    rows, schema = converters.from_arrow(tables)
    obj = DataFrame(rows=rows, schema=schema)

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
    expected_columns = ["name", "age"]
    expected_types = [OrsoTypes.VARCHAR, OrsoTypes.INTEGER]
    obj.materialize()
    assert obj._rows == expected_rows, obj._rows
    assert [c.name for c in obj._schema.columns] == expected_columns
    assert [c.type for c in obj._schema.columns] == expected_types


def test_from_arrow_none():
    rows, schema = converters.from_arrow(None)
    assert rows == []
    assert schema == {}


def test_opteryx_arrow_small():
    import opteryx
    import orso

    planets = opteryx.query("SELECT * FROM $planets")
    planets_arrow = planets.arrow()
    assert isinstance(planets_arrow, pyarrow.Table)
    assert planets_arrow.shape == (9, 20)

    planets2 = orso.DataFrame.from_arrow(planets_arrow)
    assert isinstance(planets2, orso.DataFrame)
    assert planets2.shape == (9, 20)


def test_opteryx_arrow_medium():
    import opteryx
    import orso

    fake = opteryx.query("SELECT * FROM FAKE(100000, 100);")
    fake_arrow = fake.arrow()
    assert isinstance(fake_arrow, pyarrow.Table)
    assert fake_arrow.shape == (100000, 100), fake_arrow.shape

    fake2 = orso.DataFrame.from_arrow(fake_arrow)
    assert isinstance(fake2, orso.DataFrame)
    assert fake2.shape == (100000, 100)


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
