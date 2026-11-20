import os
import sys

import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], ".."))


from orso import converters
from orso.dataframe import DataFrame
from orso.types import OrsoTypes
import pytest


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
    rows, schema = converters.from_arrow(tables, 3)
    result = DataFrame(rows=rows, schema=schema)

    # Verify that the result has the correct number of rows and columns
    assert len(result) == 3, len(result)
    assert len(result.column_names) == 3, len(result.column_names)

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
    assert list(rows) == [], rows
    assert schema == {}


def test_opteryx_arrow_small():
    # opteryx has no Windows wheels; skip rather than fail the suite there
    opteryx = pytest.importorskip("opteryx")
    import orso

    planets = opteryx.query("SELECT * FROM $planets")
    planets_arrow = planets.arrow()
    assert isinstance(planets_arrow, pyarrow.Table)
    assert planets_arrow.shape == (9, 20)

    planets2 = orso.DataFrame.from_arrow(planets_arrow)
    assert isinstance(planets2, orso.DataFrame)
    assert planets2.shape == (9, 20)


def test_opteryx_arrow_medium():
    # opteryx has no Windows wheels; skip rather than fail the suite there
    opteryx = pytest.importorskip("opteryx")
    import orso

    fake = opteryx.query("SELECT * FROM FAKE(100000, 100) AS FK;")
    fake_arrow = fake.arrow()
    assert isinstance(fake_arrow, pyarrow.Table)
    assert fake_arrow.shape == (100000, 100), fake_arrow.shape

    fake2 = orso.DataFrame.from_arrow(fake_arrow)
    assert isinstance(fake2, orso.DataFrame)
    assert fake2.shape == (100000, 100)


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()


def test_arrow_jsonb_dicts_are_written_as_json_bytes():
    """0.0.x wrote dict/list/tuple/set values in a JSONB column as JSON bytes (on append),
    so the column stayed binary; a dict handed to a binary column must not raise
    'Expected bytes, got a dict'."""
    import pytest

    pa = pytest.importorskip("pyarrow")
    from orso import DataFrame
    from orso.schema import FlatColumn, RelationSchema
    from orso.types import OrsoTypes

    for orso_type in (OrsoTypes.JSONB, OrsoTypes.BLOB):
        schema = RelationSchema(
            name="t",
            columns=[
                FlatColumn(name="a", type=OrsoTypes.INTEGER),
                FlatColumn(name="b", type=orso_type),
            ],
        )
        rows = [(1, {"x": 1}), (2, [1, {"y": 2}]), (3, (1, 2)), (4, None), (5, b"raw")]
        df = DataFrame(rows=rows, schema=schema)
        table = df.arrow()
        assert table.schema.field("b").type == pa.binary()
        assert table.column("b").to_pylist() == [
            b'{"x":1}',
            b'[1,{"y":2}]',
            b"[1,2]",
            None,
            b"raw",
        ]
        assert df.arrow(size=1).column("b").to_pylist() == [b'{"x":1}']


def test_arrow_infers_when_non_binary_column_holds_containers():
    import pytest

    pytest.importorskip("pyarrow")
    from orso import DataFrame
    from orso.schema import FlatColumn, RelationSchema
    from orso.types import OrsoTypes

    for orso_type in (OrsoTypes.VARCHAR, OrsoTypes.ARRAY, OrsoTypes.STRUCT):
        schema = RelationSchema(
            name="t",
            columns=[
                FlatColumn(name="a", type=OrsoTypes.INTEGER),
                FlatColumn(name="b", type=orso_type),
            ],
        )
        df = DataFrame(rows=[(1, {"x": 1}), (2, {"x": 2})], schema=schema)
        assert df.arrow().column("b").to_pylist() == [{"x": 1}, {"x": 2}]


def test_arrow_v1_path_writes_jsonb_dicts_as_json_bytes():
    import pytest

    pa = pytest.importorskip("pyarrow")
    from orso import DataFrame
    from orso import converters
    from orso.schema import FlatColumn, RelationSchema
    from orso.types import OrsoTypes

    schema = RelationSchema(
        name="t",
        columns=[
            FlatColumn(name="a", type=OrsoTypes.INTEGER),
            FlatColumn(name="b", type=OrsoTypes.JSONB),
        ],
    )
    df = DataFrame(rows=[(1, {"x": 1}), (2, {"x": 2})], schema=schema)
    df.materialize()
    table = converters._to_arrow_v1(df, len(df))
    assert table.column("b").to_pylist() == [b'{"x":1}', b'{"x":2}']
    assert table.column("b").type == pa.binary()
    assert table.column("a").type == pa.int64()


def test_arrow_string_column_of_json_strings_with_a_dict_among_them():
    """0.1.7 raised 'Expected bytes, got a dict' at `pa.array(values, type=field.type)`:
    a VARCHAR column of JSON strings with a dict among them cannot be inferred either,
    so the error was re-raised. The dict must become JSON text and the column stay a string."""
    import datetime
    import decimal

    import pytest

    pa = pytest.importorskip("pyarrow")
    from orso import DataFrame
    from orso import converters
    from orso.schema import FlatColumn, RelationSchema
    from orso.types import OrsoTypes

    schema = RelationSchema(
        name="t",
        columns=[
            FlatColumn(name="a", type=OrsoTypes.INTEGER),
            FlatColumn(name="b", type=OrsoTypes.VARCHAR),
        ],
    )
    rows = [
        (1, '{"x":1}'),
        (2, {"x": 2}),
        (3, None),
        (4, [1, {"at": datetime.datetime(2026, 1, 1)}, decimal.Decimal("1.5")]),
    ]
    expected = ['{"x":1}', '{"x":2}', None, '[1,{"at":"2026-01-01T00:00:00"},"1.5"]']

    df = DataFrame(rows=rows, schema=schema)
    table = df.arrow()
    assert table.schema.field("b").type == pa.string()
    assert table.column("b").to_pylist() == expected

    df.materialize()
    table = converters._to_arrow_v1(df, len(df))
    assert table.schema.field("b").type == pa.string()
    assert table.column("b").to_pylist() == expected


def test_arrow_jsonb_dicts_holding_datetimes_stay_binary():
    """0.1.7 fell back to inferring a struct when a JSONB dict held a datetime."""
    import datetime

    import pytest

    pa = pytest.importorskip("pyarrow")
    from orso import DataFrame
    from orso.schema import FlatColumn, RelationSchema
    from orso.types import OrsoTypes

    schema = RelationSchema(name="t", columns=[FlatColumn(name="b", type=OrsoTypes.JSONB)])
    df = DataFrame(rows=[({"at": datetime.date(2026, 1, 1)},), ({1, 2},), (b"raw",)], schema=schema)
    table = df.arrow()
    assert table.schema.field("b").type == pa.binary()
    assert table.column("b").to_pylist() == [b'{"at":"2026-01-01"}', b"[1,2]", b"raw"]
