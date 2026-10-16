"""
Phase 3 of the v2 rewrite (DESIGN.md §10): Arrow import and export through the C Data
Interface, and native decimals and nested values in the record store.

The contract under test: a natively built Arrow array equals what pyarrow builds from
v1's Python list for that column (or raises the same exception), and a natively
imported row equals pyarrow's to_pylist() row, value for value and type for type.
"""

import datetime
import decimal
import os
import random
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

pa = pytest.importorskip("pyarrow")

import orso
from orso import _native
from orso import converters
from orso.row import _value_size
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

D = decimal.Decimal
UTC = datetime.timezone.utc
rng = random.Random(11)


def typed(name, otype, **kw):
    return FlatColumn(name=name, type=otype, **kw)


def store_frame(schema, rows):
    df = orso.DataFrame(schema=schema)
    for r in rows:
        df.append(r)
    assert df._frame is not None
    return df


def reference_table(df, schema):
    """What v1 + the v2 fixes would build: pyarrow converting each column's Python list."""
    columns = list(zip(*df.slice(0)._rows)) if len(df) else [() for _ in df.column_names]
    arrays, fields = [], []
    for name, column, values in zip(df.column_names, schema.columns, columns):
        values = list(values)
        if column.type == OrsoTypes._MISSING_TYPE or (column.type == OrsoTypes.STRUCT and not column.fields):
            array = pa.array(values)
            fields.append(pa.field(name, array.type))
        else:
            field = column.arrow_field
            array = pa.array(values, type=field.type)
            fields.append(field)
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def assert_export_matches(df, schema):
    """df.arrow() equals the reference, or raises the same exception type it does."""
    try:
        expected = reference_table(df, schema)
    except Exception as err:
        with pytest.raises(type(err)):
            df.arrow()
        return
    assert_tables_equal(df.arrow(), expected)


def assert_tables_equal(a, b):
    assert a.schema == b.schema, (a.schema, b.schema)
    assert a.num_rows == b.num_rows
    for name in a.column_names:
        assert a.column(name).to_pylist() == b.column(name).to_pylist(), name
    assert a.equals(b)


# ---------------------------------------------------------------------------------
# decimals and nested values in the store
# ---------------------------------------------------------------------------------

DECIMALS = [
    "0", "-0", "0.00", "-0.00", "0E-10", "-0E-38", "1", "-1", "1.5", "1.50", "-3.45",
    "123456789012345678901234567890.12345678", "99999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999", "0.0000001", "0.000001", "1.23E-8", "-1.2E-12",
    "1E-38", "1E+3", "1.5E+2", "NaN", "-Infinity", "sNaN",
    "100000000000000000000000000000000000000",  # 39 digits: held as an object
]


@pytest.mark.parametrize("text", DECIMALS)
def test_decimal_round_trip_and_size(text):
    value = D(text)
    df = store_frame(["d"], [(value,)])
    back = df.row(0)[0]
    assert type(back) is D
    assert str(back) == str(value) and back.as_tuple() == value.as_tuple()
    assert _native.value_nbytes(value) == _value_size(value)
    assert df.nbytes() == df.slice(0)._rows[0].nbytes()


def test_random_decimals_size_like_str():
    for _ in range(2000):
        digits = rng.randint(1, 38)
        coefficient = rng.randint(0, 10**digits - 1) * rng.choice([1, -1])
        value = D(coefficient).scaleb(-rng.randint(0, 38))
        df = store_frame(["d"], [(value,)])
        assert str(df.row(0)[0]) == str(value)
        assert _native.value_nbytes(value) == _value_size(value), value


NESTED = [
    [], (), {}, [1, "x", None, 2.5, True], (1, (2, (3, [4]))), {"a": [1, {"b": ("c", b"d")}]},
    {"long key that is more than twelve bytes": "and a long value too, past the inline limit"},
    [[[[[[[[[[1]]]]]]]]]], [D("1.20"), datetime.date(2024, 1, 2), datetime.datetime(2024, 1, 2, 3)],
    {1: "int key", None: "none key"}, [{"x"}, frozenset({1})], ["\ud800"],
]


@pytest.mark.parametrize("value", NESTED, ids=lambda v: repr(v)[:30])
def test_nested_round_trip(value):
    df = store_frame(["v"], [(value,)])
    back = df.row(0)[0]
    assert back == value and type(back) is type(value)
    try:
        expected = _value_size(value)
    except TypeError:
        with pytest.raises(TypeError):
            _native.value_nbytes(value)
    else:
        assert _native.value_nbytes(value) == expected


def test_nested_values_are_copied_not_aliased():
    # v1 stored the caller's list itself, so mutating it later changed the frame; the
    # record store holds a copy (an observable difference, and arguably a fix)
    payload = [1, 2]
    df = store_frame(["v"], [(payload,)])
    payload.append(3)
    assert df.row(0)[0] == [1, 2]


# ---------------------------------------------------------------------------------
# export: typed frames
# ---------------------------------------------------------------------------------


def rand_ts():
    return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=rng.randint(-(10**16), 10**16))


TYPED = [
    (typed("i", OrsoTypes.INTEGER), lambda: rng.choice([0, 1, -1, 2**63 - 1, -(2**63), rng.randint(-1000, 1000)])),
    (typed("f", OrsoTypes.DOUBLE), lambda: rng.choice([0.0, -0.0, 1.5, float("inf"), rng.random(), 2**53, -(2**53), 7])),
    (typed("b", OrsoTypes.BOOLEAN), lambda: rng.choice([True, False])),
    (typed("s", OrsoTypes.VARCHAR), lambda: rng.choice(["", "short", "a" * 40, "ünïcödé text, long enough", "🐻"])),
    (typed("y", OrsoTypes.BLOB), lambda: rng.choice([b"", b"x" * 13, bytes(range(256))])),
    (typed("t", OrsoTypes.TIMESTAMP), rand_ts),
    (typed("d", OrsoTypes.DATE), lambda: rng.choice([datetime.date(1, 1, 1), datetime.date(9999, 12, 31), rand_ts().date()])),
    (typed("tm", OrsoTypes.TIME), lambda: datetime.time(rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59), rng.randint(0, 999999))),
    (typed("iv", OrsoTypes.INTERVAL), lambda: datetime.timedelta(microseconds=rng.randint(-(10**14), 10**14))),
    (typed("dec", OrsoTypes.DECIMAL, precision=12, scale=3), lambda: D(rng.randint(-10**9, 10**9)).scaleb(-rng.randint(0, 3))),
    (typed("arr", OrsoTypes.ARRAY, element_type=OrsoTypes.INTEGER), lambda: [rng.randint(-5, 5) for _ in range(rng.randint(0, 4))]),
    (typed("arrs", OrsoTypes.ARRAY, element_type=OrsoTypes.VARCHAR), lambda: tuple(rng.choice(["a", "b" * 20]) for _ in range(rng.randint(0, 3)))),
    (typed("st", OrsoTypes.STRUCT, fields=[FlatColumn(name="x", type=OrsoTypes.INTEGER), FlatColumn(name="y", type=OrsoTypes.VARCHAR)]),
     lambda: rng.choice([{"x": 1, "y": "a"}, {"x": None}, {"y": "only y", "extra": 5}, {}])),
    (typed("j", OrsoTypes.JSONB), lambda: rng.choice([{"k": 1}, [1, 2], b"{}"])),
    (typed("n", OrsoTypes.NULL), lambda: None),
]


def twin_typed_frames(schema, rows):
    """The rows as a list frame (handed in, unvalidated) and, for the rows that pass
    v1's validation, a store frame (appended)."""
    names = [c.name for c in schema.columns]
    listed = orso.DataFrame(rows=[tuple(r[n] for n in names) for r in rows], schema=schema)
    store = orso.DataFrame(schema=schema)
    for r in rows:
        try:
            store.append(r)
        except Exception:
            pass
    return listed, store


@pytest.mark.parametrize("column, make", TYPED, ids=lambda c: getattr(c, "name", ""))
def test_typed_export_matches_pyarrow(column, make):
    schema = RelationSchema(name="t", columns=[column])
    rows = [{column.name: (None if rng.random() < 0.15 else make())} for _ in range(300)]
    for df in twin_typed_frames(schema, rows):
        assert_export_matches(df, schema)
        for size in (0, 1, 17):
            try:
                expected = reference_table(df.slice(0, size), schema)
            except Exception as err:
                with pytest.raises(type(err)):
                    df.arrow(size=size)
                continue
            assert_tables_equal(df.arrow(size=size), expected)


def test_whole_mixed_schema_and_views():
    schema = RelationSchema(name="t", columns=[c for c, _ in TYPED])
    rows = [{c.name: (None if rng.random() < 0.1 else make()) for c, make in TYPED} for _ in range(200)]
    listed, store = twin_typed_frames(schema, rows)
    for df in (listed, store, store.slice(37, 50), listed.slice(37, 50)):
        assert_export_matches(df, schema)


@pytest.mark.parametrize(
    "column, bad",
    [
        (typed("i", OrsoTypes.INTEGER), [1, None, float("nan")]),      # pyarrow truncates 1.5 but refuses NaN
        (typed("f", OrsoTypes.DOUBLE), [1.0, 2**53 + 1]),               # inexact int
        (typed("dec", OrsoTypes.DECIMAL, precision=5, scale=1), [D("1.55")]),   # lossy rescale
        (typed("dec", OrsoTypes.DECIMAL, precision=3, scale=0), [D("12345")]),  # too many digits
        (typed("s", OrsoTypes.VARCHAR), [1]),
        (typed("t", OrsoTypes.TIMESTAMP), [datetime.date(2024, 1, 1)]),
    ],
)
def test_export_raises_what_pyarrow_raises(column, bad):
    # store frames validate on append, so build an untyped frame and export it typed
    schema = RelationSchema(name="t", columns=[column])
    df = orso.DataFrame(rows=[(v,) for v in bad], schema=schema)
    with pytest.raises(Exception) as native:
        df.arrow()
    with pytest.raises(Exception) as reference:
        pa.array(bad, type=column.arrow_field.type)
    assert type(native.value) is type(reference.value)


def test_typed_export_with_objects_falls_back_to_pyarrow():
    numpy = pytest.importorskip("numpy")
    schema = RelationSchema(name="t", columns=[typed("i", OrsoTypes.INTEGER)])
    df = orso.DataFrame(rows=[(numpy.int64(5),), (6,)], schema=schema)
    assert df.arrow().column("i").to_pylist() == [5, 6]


# ---------------------------------------------------------------------------------
# export: list-of-names frames infer like pyarrow
# ---------------------------------------------------------------------------------

INFER = [
    [1, 2, None], [1.5, None], [True, False], ["a", None, "b" * 30], [b"x", None],
    [datetime.datetime(2024, 1, 1)], [datetime.date(2024, 1, 1)], [datetime.time(1, 2, 3, 4)],
    [datetime.timedelta(days=-1, seconds=3)], [None, None], [],
    [1, 2.5], [D("1.5"), D("22.75")], [[1, 2], [3]], [{"a": 1}, {"a": 2, "b": "x"}],
    [1, "a"], [1, True], [2**63], [datetime.datetime(2024, 1, 1, tzinfo=UTC)],
]


@pytest.mark.parametrize("values", INFER, ids=lambda v: repr(v)[:30])
def test_inferred_export_matches_pyarrow(values):
    store = orso.DataFrame(rows=[], schema=["c"])
    for v in values:
        store.append((v,))
    listed = orso.DataFrame(rows=[(v,) for v in values], schema=["c"])
    try:
        expected = pa.Table.from_arrays([pa.array(values)], ["c"])
    except Exception as err:
        for df in (store, listed):
            with pytest.raises(type(err)):
                df.arrow()
        return
    for df in (store, listed):
        assert_tables_equal(df.arrow(), expected)


# ---------------------------------------------------------------------------------
# import
# ---------------------------------------------------------------------------------


def arrow_columns():
    n = 50
    def nulls(values, p=0.2):
        return [None if rng.random() < p else v for v in values]
    ints = [rng.randint(-(2**31), 2**31 - 1) for _ in range(n)]
    ts = [rand_ts() for _ in range(n)]
    # nanosecond timestamps only reach 1677..2262
    ts_ns = [datetime.datetime(2000, 1, 1) + datetime.timedelta(microseconds=rng.randint(-(10**15), 10**15)) for _ in range(n)]
    return {
        "i8": pa.array(nulls([v % 100 for v in ints]), pa.int8()),
        "i16": pa.array(nulls([v % 30000 for v in ints]), pa.int16()),
        "i32": pa.array(nulls(ints), pa.int32()),
        "i64": pa.array(nulls([v * 10**9 for v in ints]), pa.int64()),
        "u8": pa.array(nulls([abs(v) % 256 for v in ints]), pa.uint8()),
        "u32": pa.array(nulls([abs(v) for v in ints]), pa.uint32()),
        "u64": pa.array(nulls([2**64 - 1 - abs(v) for v in ints]), pa.uint64()),
        "f32": pa.array(nulls([v / 7 for v in ints]), pa.float32()),
        "f64": pa.array(nulls([v / 7 for v in ints]), pa.float64()),
        "f16": pa.array(nulls([1.5] * n), pa.float16()),
        "b": pa.array(nulls([v % 2 == 0 for v in ints])),
        "s": pa.array(nulls([f"value {v} " * (v % 4) for v in ints])),
        "ls": pa.array(nulls(["é" * (abs(v) % 20) for v in ints]), pa.large_string()),
        "bin": pa.array(nulls([str(v).encode() * 3 for v in ints]), pa.binary()),
        "lbin": pa.array(nulls([b"x" * (abs(v) % 30) for v in ints]), pa.large_binary()),
        "d32": pa.array(nulls([t.date() for t in ts]), pa.date32()),
        "d64": pa.array(nulls([t.date() for t in ts]), pa.date64()),
        "ts_s": pa.array(nulls([t.replace(microsecond=0) for t in ts]), pa.timestamp("s")),
        "ts_ms": pa.array(nulls(ts), pa.timestamp("ms")),
        "ts_us": pa.array(nulls(ts), pa.timestamp("us")),
        "ts_ns": pa.array(nulls(ts_ns), pa.timestamp("ns")),
        "ts_tz": pa.array(nulls(ts), pa.timestamp("us", tz="Europe/London")),
        "t32s": pa.array(nulls([t.time().replace(microsecond=0) for t in ts]), pa.time32("s")),
        "t32ms": pa.array(nulls([t.time() for t in ts]), pa.time32("ms")),
        "t64us": pa.array(nulls([t.time() for t in ts]), pa.time64("us")),
        "t64ns": pa.array(nulls([t.time() for t in ts]), pa.time64("ns")),
        "dur_s": pa.array(nulls([datetime.timedelta(seconds=v) for v in ints]), pa.duration("s")),
        "dur_us": pa.array(nulls([datetime.timedelta(microseconds=v) for v in ints]), pa.duration("us")),
        "dur_ns": pa.array(nulls([datetime.timedelta(microseconds=v) for v in ints]), pa.duration("ns")),
        "mdn": pa.array(nulls([datetime.timedelta(days=1)] * n), pa.month_day_nano_interval()),
        "dec": pa.array(nulls([D(v).scaleb(-4) for v in ints]), pa.decimal128(14, 4)),
        "dec256": pa.array(nulls([D(v) for v in ints]), pa.decimal256(40, 0)),
        "null": pa.nulls(n),
        "list": pa.array(nulls([[v, None, -v] for v in ints])),
        "llist": pa.array(nulls([[str(v)] for v in ints]), pa.large_list(pa.string())),
        "fsl": pa.array(nulls([[v, v] for v in ints]), pa.list_(pa.int64(), 2)),
        "struct": pa.array(nulls([{"a": v, "b": [str(v)], "c": {"d": v % 2 == 0}} for v in ints])),
    }


@pytest.mark.parametrize(
    "array, orso_type",
    [
        (pa.array([[("k", 1)], None, []], pa.map_(pa.string(), pa.int64())), OrsoTypes.ARRAY),
        (pa.array(["x", None, "y", "x"]).dictionary_encode(), OrsoTypes.VARCHAR),
    ],
    ids=["map", "dictionary"],
)
def test_map_and_dictionary_columns_import(array, orso_type):
    # v1's schema conversion crashed on map columns and refused dictionary-encoded ones,
    # so neither imported; v2 describes them (map as ARRAY, dictionary by its value type)
    table = pa.table({"c": array})
    df = orso.DataFrame.from_arrow(table)
    assert df.schema.columns[0].type == orso_type
    assert_rows_match(list(df), rows_of(table))


def rows_of(table):
    names = table.column_names
    return [tuple(r[c] for c in names) for r in table.to_pylist()]


def assert_rows_match(got, expected):
    assert len(got) == len(expected)
    for g, e in zip(got, expected):
        assert tuple(g) == e, (g, e)
        assert [type(x) for x in g] == [type(x) for x in e], (g, e)


def test_import_matches_to_pylist_for_every_type():
    table = pa.table(arrow_columns())
    expected = rows_of(table)
    df = orso.DataFrame.from_arrow(table)
    assert df._frame is not None
    assert_rows_match(list(df), expected)
    rows, schema = converters.from_arrow(table)
    assert_rows_match(list(rows), expected)


def test_import_sliced_chunked_and_batched():
    table = pa.table(arrow_columns())
    chunked = pa.concat_tables([table.slice(0, 13), table.slice(13, 20), table.slice(33)])
    assert chunked.column(0).num_chunks == 3
    expected = rows_of(table)
    assert_rows_match(list(orso.DataFrame.from_arrow(chunked)), expected)
    sliced = table.slice(7, 21)
    assert_rows_match(list(orso.DataFrame.from_arrow(sliced)), rows_of(sliced))
    batch = table.to_batches()[0].slice(3, 9)
    assert_rows_match(list(orso.DataFrame.from_arrow(batch)), rows_of(pa.Table.from_batches([batch])))
    assert_rows_match(list(orso.DataFrame.from_arrow([table, table])), expected + expected)


def test_import_from_a_generator_stays_lazy():
    table = pa.table(arrow_columns())
    pulled = []

    def tables():
        for t in (table, table):
            pulled.append(t)
            yield t

    rows, _ = converters.from_arrow(tables())
    assert len(pulled) == 1  # the first table is read for its schema
    first = next(rows)
    assert len(pulled) == 1
    assert_rows_match([first] + list(rows), rows_of(table) * 2)


def test_import_size_converts_only_what_is_asked():
    table = pa.table({"x": list(range(10_000))})
    rows, _ = converters.from_arrow(table, size=25)
    assert rows.__length_hint__() == 25  # v1 converted all 10,000 to return 25
    assert [r[0] for r in rows] == list(range(25))


def test_out_of_range_temporal_raises_like_to_pylist():
    table = pa.table({"t": pa.array([10**15], pa.timestamp("s"))})
    with pytest.raises(Exception) as reference:
        table.to_pylist()
    with pytest.raises(type(reference.value)):
        list(orso.DataFrame.from_arrow(table))


def test_struct_with_duplicate_names_raises_like_to_pylist():
    struct = pa.StructArray.from_arrays([pa.array([1]), pa.array([2])], names=["a", "a"])
    table = pa.table({"s": struct})
    with pytest.raises(ValueError):
        list(orso.DataFrame.from_arrow(table))


# ---------------------------------------------------------------------------------
# round trips and the v2 fixes
# ---------------------------------------------------------------------------------


def test_arrow_round_trip_keeps_types():
    table = pa.table(
        {
            "i": pa.array([1, None, 3]),
            "s": ["a", None, "c" * 30],
            "t": pa.array([datetime.datetime(2024, 1, 1, 0, 0, 0, 5), None, datetime.datetime(1, 1, 1)], pa.timestamp("us")),
            "d": pa.array([datetime.date(2024, 1, 1), None, datetime.date(1, 1, 1)], pa.date32()),
            "dec": pa.array([D("1.20"), None, D("-3.45")], pa.decimal128(10, 2)),
            "l": pa.array([[1], None, []], pa.list_(pa.int64())),
        }
    )
    back = orso.DataFrame.from_arrow(table).arrow()
    assert back.column_names == table.column_names
    for name in table.column_names:
        assert back.column(name).to_pylist() == table.column(name).to_pylist(), name
    assert back.schema.field("d").type == pa.date32()  # v1 turned this column into date64


def test_untyped_column_is_inferred_not_forced_to_string():
    # v1 mapped a column with no declared type to string and failed on its first int
    schema = RelationSchema(name="t", columns=[FlatColumn(name="x"), typed("y", OrsoTypes.INTEGER)])
    df = orso.DataFrame(rows=[(1, 2), (3, 4)], schema=schema)
    table = df.arrow()
    assert table.schema.field("x").type == pa.int64() and table.column("x").to_pylist() == [1, 3]


def test_struct_without_fields_only_infers_that_column():
    # v1 dropped the declared schema for every column when one STRUCT had no fields
    schema = RelationSchema(
        name="t",
        columns=[typed("s", OrsoTypes.STRUCT), typed("d", OrsoTypes.DATE)],
    )
    df = orso.DataFrame(rows=[({"a": 1}, datetime.date(2024, 1, 1))], schema=schema)
    table = df.arrow()
    assert table.schema.field("d").type == pa.date32()
    assert table.schema.field("s").type == pa.struct([("a", pa.int64())])


def test_list_frames_with_ragged_rows_use_v1_export():
    df = orso.DataFrame(rows=[(1, 2), (3, 4)], schema=["a", "b"])
    assert df.arrow().num_rows == 2


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
