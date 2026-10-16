"""
The draken bridge (DESIGN.md §4.8, phase 7): DataFrame.to_draken / DataFrame.from_draken.

Needs draken, which ships inside opteryx-core and is not on PyPI on its own. These tests
skip without it. To run them against a local checkout, put a directory holding only a
`draken` symlink on the path (not the opteryx-core root, whose `opteryx` package would
shadow the PyPI opteryx the other tests use):

    mkdir -p /tmp/dk && ln -sfn ../opteryx-core/draken /tmp/dk/draken
    PYTHONPATH=/tmp/dk python -m pytest tests/test_draken_bridge.py

Oracles: a Morsel's own to_arrow() for what to_draken built, and orso's Arrow import of
that same Arrow table for what from_draken read.
"""

import ctypes
import datetime
import decimal
import gc
import os
import subprocess
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

draken = pytest.importorskip("draken")
pyarrow = pytest.importorskip("pyarrow")

import orso
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

dn = draken.draken_native
T = dn.DrakenType

TS = datetime.datetime(2026, 10, 16, 12, 30, 45, 123456)


def typed_schema():
    return RelationSchema(
        name="t",
        columns=[
            FlatColumn(name="i", type=OrsoTypes.INTEGER),
            FlatColumn(name="s", type=OrsoTypes.VARCHAR),
            FlatColumn(name="f", type=OrsoTypes.DOUBLE),
            FlatColumn(name="b", type=OrsoTypes.BOOLEAN),
            FlatColumn(name="ts", type=OrsoTypes.TIMESTAMP),
            FlatColumn(name="d", type=OrsoTypes.DATE),
            FlatColumn(name="t", type=OrsoTypes.TIME),
            FlatColumn(name="iv", type=OrsoTypes.INTERVAL),
            FlatColumn(name="dec", type=OrsoTypes.DECIMAL, precision=12, scale=3),
            FlatColumn(name="bl", type=OrsoTypes.BLOB),
        ],
    )


def typed_rows(n):
    rows = []
    for k in range(n):
        null = k % 5 == 4
        rows.append(
            {
                "i": None if null else k - 3,
                "s": None if null else ("short" if k % 2 else f"a longer string, number {k}, ünïcødé"),
                "f": None if null else k * 0.25,
                "b": None if null else k % 3 == 0,
                "ts": None if null else TS + datetime.timedelta(seconds=k, microseconds=k),
                "d": None if null else datetime.date(2000, 1, 1) + datetime.timedelta(days=k),
                "t": None if null else datetime.time(k % 24, k % 60, k % 60, k),
                "iv": None if null else datetime.timedelta(days=k, microseconds=7),
                "dec": None if null else decimal.Decimal(k) / 8,  # scales 0..3
                "bl": None if null else bytes([k % 256]) * (k % 20),
            }
        )
    return rows


def frames(rows, schema):
    """The same rows as a list frame and as a store frame."""
    listed = orso.DataFrame(rows=[tuple(r.values()) for r in rows], schema=schema)
    store = orso.DataFrame(rows=[], schema=schema)
    for r in rows:
        store.append(r)
    return {"list": listed, "store": store}


def as_dicts(df):
    names = df.column_names
    return [dict(zip(names, row)) for row in df.fetchall()]


def charged():
    lib = ctypes.CDLL(dn.__file__)
    lib.draken_mem_charged.restype = ctypes.c_int64
    return lib.draken_mem_charged()


# ----- to_draken -----------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["list", "store"])
def test_typed_frame_exports_natively(kind):
    rows = typed_rows(60)
    df = frames(rows, typed_schema())[kind]
    morsel = df.to_draken()
    assert morsel.num_rows == 60
    assert morsel.column_types == [
        T.INT64, T.VARCHAR, T.FLOAT64, T.BOOL, T.TIMESTAMP64, T.DATE32, T.TIME64, T.INTERVAL, T.DECIMAL128, T.VARBINARY,
    ]
    dec = morsel.column("dec")._nb
    assert (dec.logical_type_precision, dec.logical_type_scale) == (12, 3)
    for name in ("i", "s", "f", "b", "ts", "d", "t", "bl"):
        assert morsel.column(name).to_pylist() == [
            (r[name].replace(tzinfo=datetime.timezone.utc) if name == "ts" and r[name] else r[name]) for r in rows
        ], name
    assert morsel.column("iv").to_pylist() == [
        None if r["iv"] is None else (0, r["iv"] // datetime.timedelta(microseconds=1)) for r in rows
    ]
    assert morsel.column("dec").to_pylist() == [r["dec"] for r in rows]  # rescaled, equal


def test_arrow_view_of_export_matches_rows():
    rows = typed_rows(25)
    df = frames(rows, typed_schema())["store"]
    table = df.to_draken().to_arrow()
    out = table.to_pylist()
    for got, want in zip(out, rows):
        for name in ("i", "s", "f", "b", "ts", "d", "bl"):
            assert got[name] == want[name], name


def test_untyped_frame_infers_types():
    df = orso.DataFrame(
        [{"a": 1, "b": "x", "c": 1.5, "mixed": 1, "none": None}, {"a": 2, "b": None, "c": 2.5, "mixed": 2.5, "none": None}]
    )
    morsel = df.to_draken()
    assert morsel.column_types == [T.INT64, T.VARCHAR, T.FLOAT64, T.FLOAT64, T.NULL]
    assert morsel.column("mixed").to_pylist() == [1.0, 2.5]  # int and float: float, as arrow() does
    assert morsel.column("none").to_pylist() == [None, None]


def test_typed_all_null_column_keeps_its_type():
    schema = RelationSchema(name="t", columns=[FlatColumn(name="s", type=OrsoTypes.VARCHAR)])
    df = orso.DataFrame(rows=[(None,), (None,)], schema=schema)
    morsel = df.to_draken()
    assert morsel.column_types == [T.VARCHAR]
    assert morsel.column("s").to_pylist() == [None, None]


def test_nested_values_go_through_draken():
    df = orso.DataFrame([{"l": [1, 2]}, {"l": None}, {"l": [3]}])
    morsel = df.to_draken()
    assert morsel.column_types == [T.ARRAY]
    assert morsel.column("l").to_pylist() == [[1, 2], None, [3]]


def test_unrepresentable_column_is_a_clear_error():
    df = orso.DataFrame(rows=[(1,), ("x",)], schema=["a"])
    with pytest.raises(TypeError, match="column 'a'"):
        df.to_draken()


def test_size_and_views():
    rows = typed_rows(40)
    df = frames(rows, typed_schema())["store"]
    assert df.to_draken(size=7).num_rows == 7
    tail = df.slice(30, 10)
    morsel = tail.to_draken()
    assert morsel.column("i").to_pylist() == [r["i"] for r in rows[30:40]]
    assert morsel.column("s").to_pylist() == [r["s"] for r in rows[30:40]]
    empty = orso.DataFrame(rows=[], schema=typed_schema())
    assert empty.to_draken().num_rows == 0


def test_string_arena_is_per_column_and_compact():
    # long strings in two columns: each vector carries only its own column's bytes
    df = orso.DataFrame([{"a": "a" * 100 + str(k), "b": "b" * 50 + str(k)} for k in range(100)])
    morsel = df.to_draken()
    assert morsel.column("a").to_pylist() == ["a" * 100 + str(k) for k in range(100)]
    assert morsel.column("b").to_pylist() == ["b" * 50 + str(k) for k in range(100)]
    long_a = sum(100 + len(str(k)) for k in range(100))
    long_b = sum(50 + len(str(k)) for k in range(100))
    # slots plus each column's own payloads, not a copy of the frame's whole arena per column
    assert morsel.nbytes == 2 * 100 * 16 + long_a + long_b


def test_vectors_outlive_the_frame():
    df = orso.DataFrame([{"s": "x" * 40 + str(k), "i": k} for k in range(1000)])
    morsel = df.to_draken()
    del df
    gc.collect()
    assert morsel.column("s").to_pylist()[999] == "x" * 40 + "999"
    assert sum(morsel.column("i").to_pylist()) == sum(range(1000))


def test_exported_buffers_are_charged_to_draken_and_released():
    # draken grows a process-wide identity selection to the longest dense vector it has
    # seen and charges it once; grow it first so only orso's buffers are measured
    warm = dn.vector_int64_from_sequence(list(range(200_000)))
    del warm
    gc.collect()
    before = charged()
    df = orso.DataFrame([{"i": k, "s": "y" * 30} for k in range(200_000)])
    morsel = df.to_draken()
    during = charged()
    assert during - before >= 200_000 * 8  # at least the int64 column
    del morsel
    gc.collect()
    assert charged() == before


# ----- from_draken ---------------------------------------------------------------------


def oracle(morsel):
    return orso.DataFrame.from_arrow(morsel.to_arrow()).fetchall()


def test_round_trip_typed():
    rows = typed_rows(50)
    df = frames(rows, typed_schema())["store"]
    back = orso.DataFrame.from_draken(df.to_draken())
    assert back.column_names == df.column_names
    got = as_dicts(back)
    for g, w in zip(got, rows):
        assert g == w
    assert [c.type for c in back.schema.columns] == [c.type for c in typed_schema().columns]


def test_from_draken_matches_arrow_import():
    vectors = {
        "i8": dn.vector_int8_from_sequence([1, None, -3]),
        "i16": dn.vector_int16_from_sequence([1, 2, None]),
        "i32": dn.vector_int32_from_sequence([None, 2, 3]),
        "u32": dn.vector_uint32_from_sequence([1, 2, 4000000000]),
        "f32": dn.vector_float32_from_sequence([1.5, None, 2.25]),
        "nv": dn.vector_from_nvarchar_sequence([b"\xc3\xa9t\xc3\xa9", None, b"x" * 20]),
        "vb": dn.vector_from_bytes_sequence([b"\x00\x01", None, b"z" * 30]),
        "dec": dn.vector_decimal_from_sequence([decimal.Decimal("1.25"), None, decimal.Decimal("-7.5")], 9, 2),
        "date": dn.vector_date32_from_sequence([datetime.date(1999, 12, 31), None, datetime.date(2030, 1, 1)]),
        "ts": dn.vector_timestamp_from_sequence([TS, None, datetime.datetime(1970, 1, 1)]),
        "null": dn.vector_null_from_length(3),
    }
    morsel = draken.Morsel.from_vectors(list(vectors), list(vectors.values()))
    assert orso.DataFrame.from_draken(morsel).fetchall() == oracle(morsel)


def test_dict_and_constant_vectors():
    d = dn.vector_from_dict([10, 20, 30], [2, 0, 1, 0], None)
    c = dn.vector_from_constant(7, 4)
    s = dn.vector_from_string_dict_sequence([b"apple", b"banana" * 4, b"apple", None])
    morsel = draken.Morsel.from_vectors(["d", "c", "s"], [d, c, s])
    rows = orso.DataFrame.from_draken(morsel).fetchall()
    assert [tuple(r) for r in rows] == [
        (30, 7, "apple"),
        (10, 7, "banana" * 4),
        (20, 7, "apple"),
        (10, 7, None),
    ]


def test_types_without_a_slot_use_draken_values():
    import uuid

    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    vectors = {
        "uuid": dn.vector_uuid_from_sequence([u, None]),
        "months": dn.vector_interval_from_sequence([(1, 5), (0, 3)]),
        "arr": draken.vector_from_sequence([[1, 2], None], "ARRAY"),
        "big": dn.vector_uint64_from_sequence([2**63 + 5, 1]),
    }
    morsel = draken.Morsel.from_vectors(list(vectors), list(vectors.values()))
    rows = orso.DataFrame.from_draken(morsel).fetchall()
    for name, vector in vectors.items():
        assert [r[list(vectors).index(name)] for r in rows] == vector.to_pylist(), name


def test_many_morsels_and_the_cursor():
    a = draken.Morsel.from_vectors(["x"], [dn.vector_int64_from_sequence([1, 2])])
    b = draken.Morsel.from_vectors(["x"], [dn.vector_int64_from_sequence([3])])
    df = orso.DataFrame.from_draken([a, b])
    assert df.rowcount == 3
    assert df.fetchone() == (1,)
    assert [r[0] for r in df.fetchall()] == [2, 3]
    c = draken.Morsel.from_vectors(["y"], [dn.vector_int64_from_sequence([1])])
    with pytest.raises(ValueError, match="same columns"):
        orso.DataFrame.from_draken([a, c])


def test_from_draken_then_append_and_arrow():
    morsel = draken.Morsel.from_vectors(
        ["n", "s"], [dn.vector_int64_from_sequence([1, 2]), draken.vector_from_sequence(["a", "b" * 20], "VARCHAR")]
    )
    df = orso.DataFrame.from_draken(morsel)
    df.append({"n": 3, "s": "c"})
    assert df.arrow().to_pylist() == [{"n": 1, "s": "a"}, {"n": 2, "s": "b" * 20}, {"n": 3, "s": "c"}]


def test_without_draken_is_a_missing_dependency():
    code = (
        "import sys; sys.modules['draken'] = None\n"
        "import orso\n"
        "from orso.exceptions import MissingDependencyError\n"
        "try:\n"
        "    orso.DataFrame([{'a': 1}]).to_draken()\n"
        "except MissingDependencyError as e:\n"
        "    print('missing', e.dependency)\n"
    )
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out = subprocess.run([sys.executable, "-c", code], cwd=root, capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "missing draken"
