"""
DataFrame.collect: the cases v1 tested against its Cython collector (orso.compute was
removed in phase 6), run on both kinds of frame. collect() still returns a numpy object
array of shape (columns, rows), or one row of it for a single column.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

numpy = pytest.importorskip("numpy")

import orso


def frames(rows, schema):
    listed = orso.DataFrame(rows=list(rows), schema=schema)
    store = orso.DataFrame(rows=[], schema=schema)
    for row in rows:
        store.append(row)
    return listed, store


@pytest.mark.parametrize("kind", ["list", "store"])
def test_collector(kind):
    df = dict(zip(("list", "store"), frames([(1, 2), (2, 1), (7, 8)], ["a", "b"])))[kind]
    columns = df.collect([1, 0])
    assert columns.shape == (2, 3) and columns.dtype == object
    assert sum(columns[0]) == 11 and sum(columns[1]) == 10


def test_collector_empty_input():
    df = orso.DataFrame(rows=[], schema=["a"])
    assert len(df.collect([])) == 0
    assert df.collect(["a"]).shape == (1, 0)


@pytest.mark.parametrize("kind", ["list", "store"])
def test_collector_single_column(kind):
    df = dict(zip(("list", "store"), frames([(5, 10)], ["a", "b"])))[kind]
    column = df.collect(1)
    assert column.shape == (1,) and list(column) == [10]


@pytest.mark.parametrize("kind", ["list", "store"])
def test_collector_large_data_and_limit(kind):
    df = dict(zip(("list", "store"), frames([(i, i * 2) for i in range(10000)], ["a", "b"])))[kind]
    columns = df.collect([1, 0])
    assert sum(columns[0]) == sum(i * 2 for i in range(10000))
    assert sum(columns[1]) == sum(range(10000))
    assert df.collect("b", limit=10).tolist() == [i * 2 for i in range(10)]


@pytest.mark.parametrize("kind", ["list", "store"])
def test_collector_bad_indices(kind):
    df = dict(zip(("list", "store"), frames([(1, 2), (3, 4)], ["a", "b"])))[kind]
    with pytest.raises(ValueError):  # not a column name
        df.collect([0.5, 1.5])
    with pytest.raises(IndexError, match="out of bounds"):
        df.collect([-1, 0])
    with pytest.raises(IndexError, match="out of bounds"):
        df.collect([100, 200])


@pytest.mark.parametrize("kind", ["list", "store"])
def test_collector_duplicate_indices(kind):
    df = dict(zip(("list", "store"), frames([(1, 2), (3, 4), (5, 6)], ["a", "b"])))[kind]
    columns = df.collect([1, 1, 0])
    assert [sum(c) for c in columns] == [12, 12, 9]


def test_collect_keeps_objects_whole():
    # nested lists must stay single elements, not be spread across the array
    df = orso.DataFrame(rows=[([1, 2],), ([3, 4],)], schema=["a"])
    column = df.collect("a")
    assert column.shape == (2,) and column[0] == [1, 2]


def test_collect_reference_counts():
    sentinel = object()
    before = sys.getrefcount(sentinel)
    df = orso.DataFrame(rows=[(sentinel,) for _ in range(100)], schema=["a"])
    column = df.collect("a")
    assert sys.getrefcount(sentinel) == before + 200  # the rows and the array
    del column
    assert sys.getrefcount(sentinel) == before + 100
