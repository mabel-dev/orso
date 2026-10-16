"""
Phase 2 of the v2 rewrite (DESIGN.md §10): the C Row base type, the native size
estimate, and the record store behind DataFrame.append().

The rule these tests enforce is that a store frame is indistinguishable from a v1 list
frame holding the same rows: same values and types back, same slices, same cursor
behaviour, same validation errors, same nbytes(). Where v2 deliberately differs (a row
of the wrong width is rejected at append), the test says so.
"""

import copy
import datetime
import decimal
import gc
import os
import random
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from ormsgpack import OPT_SERIALIZE_NUMPY
from ormsgpack import packb

import orso
from orso import _native
from orso.exceptions import DataValidationError
from orso.exceptions import ExcessColumnsInDataError
from orso.row import Row
from orso.row import _serialize
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

UTC = datetime.timezone.utc

SAMPLE_VALUES = [
    None, True, False,
    0, 1, 127, 128, 255, 256, 65535, 65536, 2**32 - 1, 2**32, 2**63 - 1, 2**63, 2**64 - 1,
    -1, -32, -33, -128, -129, -32768, -32769, -(2**31), -(2**31) - 1, -(2**63),
    0.0, -0.0, 1.5, float("inf"), float("nan"),
    "", "a", "a" * 12, "a" * 13, "a" * 31, "a" * 32, "a" * 255, "a" * 256, "a" * 70000,
    "é", "é" * 20, "日本語のテキスト", "emoji 🐻 longer than twelve",
    b"", b"x" * 12, b"x" * 13, b"x" * 300, b"\x00\xff" * 40000,
    datetime.datetime(2024, 1, 2, 3, 4, 5), datetime.datetime(2024, 1, 2, 3, 4, 5, 6),
    datetime.datetime(1, 1, 1), datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
    datetime.datetime(1969, 12, 31, 23, 59, 59, 500000),
    datetime.datetime(2024, 1, 1, tzinfo=UTC), datetime.datetime(2024, 1, 1, fold=1),
    datetime.date(1970, 1, 1), datetime.date(1, 1, 1), datetime.date(9999, 12, 31), datetime.date(1900, 2, 28),
    datetime.time(0, 0), datetime.time(23, 59, 59, 999999), datetime.time(12, tzinfo=UTC),
    datetime.timedelta(0), datetime.timedelta(days=-3, seconds=5, microseconds=7),
    datetime.timedelta(days=999999999), datetime.timedelta(days=-999999999),
    decimal.Decimal("99.99"), decimal.Decimal("-0"), decimal.Decimal("NaN"),
    [1, "a", None], (1, 2), {"k": [1, {"n": "v"}]}, {"set"}, frozenset({1}),
    bytearray(b"ab"), "\ud800",
]


def v1_value_size(value):
    return len(packb(value, option=OPT_SERIALIZE_NUMPY, default=_serialize))


def same(a, b):
    """Equal, including NaN, and of the same type."""
    if type(a) is not type(b):
        return False
    if isinstance(a, float) and a != a:
        return b != b
    if isinstance(a, decimal.Decimal) and a.is_nan():
        return b.is_nan()
    return a == b


# ---------------------------------------------------------------------------------
# size estimate
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize("value", SAMPLE_VALUES, ids=lambda v: f"{type(v).__name__}:{str(v)[:20]}")
def test_value_nbytes_matches_v1_serialiser(value):
    try:
        expected = v1_value_size(value)
    except TypeError as err:
        with pytest.raises(TypeError):
            _native.value_nbytes(value)
        return
    assert _native.value_nbytes(value) == expected


def test_int_beyond_64_bits_raises_like_v1():
    factory = Row.create_class(["a"])
    with pytest.raises(TypeError):
        factory((2**64,)).nbytes()


def test_row_nbytes_is_v1_record_size():
    factory = Row.create_class([f"c{i}" for i in range(20)])
    rng = random.Random(7)
    for _ in range(200):
        values = [rng.choice(SAMPLE_VALUES[:-2]) for _ in range(20)]  # skip bytearray, surrogate
        row = factory(values)
        try:
            expected = len(packb(tuple(row), option=OPT_SERIALIZE_NUMPY, default=_serialize)) + 14
        except TypeError:
            continue
        assert row.nbytes() == expected, values


# ---------------------------------------------------------------------------------
# Row
# ---------------------------------------------------------------------------------


def test_row_basics():
    factory = Row.create_class(("a", "b", "c"))
    row = factory({"a": 1, "c": "x", "extra": 9})
    assert row == (1, None, "x")
    assert isinstance(row, Row) and isinstance(row, tuple)
    assert row.as_dict == {"a": 1, "b": None, "c": "x"}
    assert row.as_map == (("a", 1), ("b", None), ("c", "x"))
    assert row.values == (1, None, "x") and type(row.values) is tuple
    assert row.keys() == ("a", "b", "c")
    assert row.get("c") == "x" and row.get("missing") is None and row.get("missing", 5) == 5
    assert factory([4, 5, 6]) == (4, 5, 6)
    assert factory(x for x in (7, 8, 9)) == (7, 8, 9)


def test_row_has_no_instance_dict():
    factory = Row.create_class(("a",))
    with pytest.raises(AttributeError):
        factory((1,)).__dict__


def test_row_hash_is_tuple_hash():
    # 3.14 caches a tuple's hash in the object; a natively built row must start "not
    # computed" or every row hashes to 0
    factory = Row.create_class(("a", "b"))
    rows = [factory((i, str(i))) for i in range(100)]
    assert [hash(r) for r in rows] == [hash((i, str(i))) for i in range(100)]
    assert len({hash(r) for r in rows}) == 100


def test_row_copy_and_deepcopy():
    factory = Row.create_class(("a", "b"))
    row = factory((1, [2, 3]))
    assert copy.copy(row) == row and type(copy.copy(row)) is factory
    deep = copy.deepcopy(row)
    assert deep == row and deep[1] is not row[1] and type(deep) is factory


def test_create_class_is_cached():
    assert Row.create_class(["x", "y"]) is Row.create_class(("x", "y"))


def test_bare_row_still_works():
    assert Row([1, 2]) == (1, 2)
    with pytest.raises(TypeError):
        Row({"a": 1})


# ---------------------------------------------------------------------------------
# store frames behave like list frames
# ---------------------------------------------------------------------------------


def mixed_rows(n, seed=3, sizable=False):
    rng = random.Random(seed)
    pool = [v for v in SAMPLE_VALUES if not isinstance(v, (set, frozenset, bytearray))]
    if sizable:  # values v1's serialiser can size (it raises on the rest)
        pool = [
            v
            for v in pool
            if v != "\ud800"
            and not (type(v) is int and v >= 2**64)
            and not (type(v) is datetime.time and v.tzinfo is not None)
        ]
    return [tuple(rng.choice(pool) for _ in range(4)) for _ in range(n)]


def twin_frames(rows, schema=("a", "b", "c", "d")):
    """The same rows as a store frame (appended) and a list frame (handed in)."""
    store = orso.DataFrame(rows=[], schema=list(schema))
    for r in rows:
        store.append(r)
    factory = Row.create_class(list(schema))
    listed = orso.DataFrame(rows=[factory(r) for r in rows], schema=list(schema))
    assert store._frame is not None and listed._frame is None
    return store, listed


def assert_same_rows(a, b):
    assert len(a) == len(b)
    for x, y in zip(a, b):
        assert type(x) is type(y)
        assert all(same(p, q) for p, q in zip(x, y)), (x, y)


def test_round_trip_every_value_type():
    rows = mixed_rows(500)
    store, listed = twin_frames(rows)
    assert_same_rows(list(store), list(listed))
    # appending invalidates the cursor (v1), so read through views
    assert_same_rows(store.slice(0).fetchall(), listed.slice(0).fetchall())


def test_store_frame_slices_match_list_slices():
    store, listed = twin_frames(mixed_rows(57))
    windows = [(0, None), (0, 5), (10, 0), (-5, None), (-5, 3), (-100, 200), (50, 50), (200, 5), (3, 1)]
    for offset, length in windows:
        assert_same_rows(list(store.slice(offset, length)), list(listed.slice(offset, length)))
    for size in (0, 1, 5, 57, 100):
        assert_same_rows(list(store.head(size)), list(listed.head(size)))
        assert_same_rows(list(store.tail(size)), list(listed.tail(size)))
    for batch in (1, 10, 57, 1000):
        assert [len(b) for b in store.to_batches(batch)] == [len(b) for b in listed.to_batches(batch)]
        assert_same_rows([r for b in store.to_batches(batch) for r in b], list(listed))


def test_store_frame_cursor_matches_list_cursor():
    store, listed = twin_frames(mixed_rows(25))
    # the store frame was built by appending, which invalidates a cursor (v1); views
    # of both kinds start with a fresh one
    store, listed = store.slice(0), listed.slice(0)
    assert_same_rows([store.fetchone()], [listed.fetchone()])
    assert_same_rows(store.fetchmany(7), listed.fetchmany(7))
    assert_same_rows(store.fetchmany(), listed.fetchmany())
    assert_same_rows(store.fetchall(), listed.fetchall())
    assert store.fetchall() == listed.fetchall() == []
    assert store.fetchone() is None and listed.fetchone() is None


def test_append_invalidates_cursor_on_both_kinds():
    store, listed = twin_frames(mixed_rows(3))
    for frame in (store, listed):
        frame.append((1, 2, 3, 4))
        with pytest.raises(Exception, match="append"):
            frame.fetchone()


def test_nbytes_matches_rows():
    store, listed = twin_frames(mixed_rows(300, sizable=True))
    assert store.nbytes() == listed.nbytes() == sum(r.nbytes() for r in listed)
    store.append((1, "x", None, 2.5))
    listed.append((1, "x", None, 2.5))
    assert store.nbytes() == listed.nbytes()
    assert store.slice(10, 20).nbytes() == listed.slice(10, 20).nbytes()


def test_reading_rows_turns_a_store_frame_into_a_list_frame():
    store, listed = twin_frames(mixed_rows(10))
    store = store.slice(0)
    store.fetchmany(3)
    rows = store._rows
    assert isinstance(rows, list) and store._frame is None
    assert_same_rows(rows, list(listed))
    # the cursor keeps its place across the switch
    listed.fetchmany(3)
    assert_same_rows(store.fetchall(), listed.fetchall())


def test_views_are_copy_on_write():
    store, _ = twin_frames(mixed_rows(20))
    view = store.slice(5, 5)
    view.append((9, 9, 9, 9))
    assert len(view) == 6 and len(store) == 20
    assert tuple(view.row(-1)) == (9, 9, 9, 9)
    store.append((8, 8, 8, 8))
    assert len(view) == 6 and len(store) == 21


def test_list_frame_with_rows_stays_a_list_frame_on_append():
    df = orso.DataFrame(rows=[(1, 2)], schema=["a", "b"])
    df.append((3, 4))
    assert df._frame is None and df._rows == [(1, 2), (3, 4)]


def test_wrong_width_row_is_rejected():
    # v1 stored it and arrow() failed later; v2 refuses it at append
    df = orso.DataFrame(rows=[], schema=["a", "b"])
    with pytest.raises(ValueError, match="2 fields"):
        df.append((1, 2, 3))
    assert len(df) == 0


def test_frame_holds_objects_without_leaking():
    sentinel = object()
    baseline = sys.getrefcount(sentinel)
    df = orso.DataFrame(rows=[], schema=["a"])
    for _ in range(100):
        df.append((sentinel,))
    assert sys.getrefcount(sentinel) == baseline + 100
    head = df.head(10)
    head.append((sentinel,))
    # copy-on-write takes its own reference to every object the shared store holds (a
    # superset of the view's: nested values can keep objects anywhere in the arena)
    assert sys.getrefcount(sentinel) == baseline + 100 + 100 + 1
    rows = df.fetchall() if df._cursor is not None else list(df)
    del df, head, rows
    gc.collect()
    assert sys.getrefcount(sentinel) == baseline


# ---------------------------------------------------------------------------------
# typed frames: v1's validation, natively
# ---------------------------------------------------------------------------------


def typed_schema():
    return RelationSchema(
        name="t",
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER, nullable=False),
            FlatColumn(name="name", type=OrsoTypes.VARCHAR),
            FlatColumn(name="when", type=OrsoTypes.TIMESTAMP),
            FlatColumn(name="doc", type=OrsoTypes.JSONB),
            FlatColumn(name="anything"),
        ],
    )


def test_typed_append_stores_in_schema_order():
    df = orso.DataFrame(schema=typed_schema())
    when = datetime.datetime(2024, 5, 6, 7, 8, 9)
    df.append({"doc": {"k": [1, 2]}, "when": when, "anything": decimal.Decimal("1.5"), "name": None, "id": 1})
    row = df.row(0)
    assert row == (1, None, when, b'{"k":[1,2]}', decimal.Decimal("1.5"))
    assert type(row[4]) is decimal.Decimal


@pytest.mark.parametrize(
    "record, error",
    [
        ({"id": None, "name": "a", "when": None, "doc": None, "anything": 1}, DataValidationError),
        ({"id": "1", "name": "a", "when": None, "doc": None, "anything": 1}, DataValidationError),
        ({"id": 1, "name": "a", "when": "2024-01-01", "doc": None, "anything": 1}, DataValidationError),
        ({"id": 1, "name": "a", "when": None, "doc": "not bytes", "anything": 1}, DataValidationError),
        ({"id": 1, "name": "a", "when": None, "doc": None}, DataValidationError),
        ({"id": 1, "name": "a", "when": None, "doc": None, "anything": 1, "extra": 2}, ExcessColumnsInDataError),
        ((1, "a", None, None, 1), TypeError),
    ],
)
def test_typed_append_raises_v1_errors(record, error):
    df = orso.DataFrame(schema=typed_schema())
    with pytest.raises(error) as raised:
        df.append(record)
    with pytest.raises(error) as expected:
        typed_schema().validate(dict(record) if isinstance(record, dict) else record)
    assert str(raised.value) == str(expected.value)
    assert len(df) == 0


def test_bool_passes_as_integer_like_v1():
    df = orso.DataFrame(schema=typed_schema())
    df.append({"id": True, "name": "a", "when": None, "doc": None, "anything": None})
    assert df.row(0)[0] is True


def test_typed_append_to_list_frame_validates_too():
    schema = typed_schema()
    df = orso.DataFrame(rows=[(1, "a", None, None, None)], schema=schema)
    df.append({"id": 2, "name": "b", "when": None, "doc": [1], "anything": None})
    assert df._frame is None and df._rows[-1] == (2, "b", None, b"[1]", None)
    with pytest.raises(DataValidationError):
        df.append({"id": None, "name": "b", "when": None, "doc": None, "anything": None})


def test_dataframe_from_dicts_matches_v1():
    dicts = [{"a": 1, "b": "x"}, {"b": "y", "a": 2}, {"a": 3}, {"a": 4, "b": "z", "c": "ignored"}]
    df = orso.DataFrame(dicts)
    assert df.column_names == ("a", "b")
    assert df._rows == [(1, "x"), (2, "y"), (3, None), (4, "z")]
    assert all(isinstance(r, Row) for r in df._rows)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()


def test_store_frame_collect_matches_list_frame_without_decoding_rows():
    pytest.importorskip("numpy")  # collect() and array-backed columns return numpy arrays
    store, listed = twin_frames(mixed_rows(40))
    for args in (("a",), (["a", "d"],), (2,), ([0, 1, 2, 3],), ("b", 7), ("c", 0)):
        x, y = store.collect(*args), listed.collect(*args)
        assert type(x) is type(y) and x.shape == y.shape and x.dtype == y.dtype
        assert repr(x) == repr(y), args
    for bad in (-1, 4):
        with pytest.raises(IndexError):
            store.collect(bad)
    assert store._frame is not None  # still a store frame: no rows were decoded
