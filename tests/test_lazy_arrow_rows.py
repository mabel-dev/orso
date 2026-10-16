"""
Rows of a stream of Arrow tables (converters.from_arrow with a generator, which is how
the PyPI opteryx cursor receives every query result).

They are read one table at a time when iterated, as v1's generator was. A frame whose
rows have not been read yet skips Python rows altogether when it is materialised: the
remaining tables go into one native store, and arrow(), len() and the rest work from it.
Everything observable must match v1, including the cursor that shares the iterator.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

pyarrow = pytest.importorskip("pyarrow")

import orso
from orso import converters
from orso.converters import LazyArrowRows

TABLES = [
    pyarrow.table({"id": [1, 2, 3], "name": ["a", "b" * 20, None]}),
    pyarrow.table({"id": [4, 5], "name": ["d", "é"]}),
    pyarrow.table({"id": [6], "name": ["f"]}),
]
ALL = [(1, "a"), (2, "b" * 20), (3, None), (4, "d"), (5, "é"), (6, "f")]


def cursor_like(tables, size=None):
    """What the PyPI opteryx Cursor does: rows and cursor are the same iterator."""
    rows, schema = converters.from_arrow((t for t in tables), size=size)
    df = orso.DataFrame(rows=rows, schema=schema)
    df._cursor = iter(df._rows)
    return df, rows


def test_from_arrow_of_a_generator_is_lazy_rows():
    rows, _ = converters.from_arrow(t for t in TABLES)
    assert type(rows) is LazyArrowRows and not rows.started
    assert [tuple(r) for r in rows] == ALL and rows.started


def test_arrow_of_unread_rows_uses_one_store():
    df, rows = cursor_like(TABLES)
    table = df.arrow()
    assert table.to_pylist() == [{"id": i, "name": n} for i, n in ALL]
    assert df._frame is not None and df._py_rows is None  # never made Python rows
    assert df._decoded is None
    assert df.fetchone() is None and df.fetchall() == []  # v1: the shared iterator is spent


def test_len_shape_and_rowcount_skip_rows():
    df, _ = cursor_like(TABLES)
    assert len(df) == 6 and df.rowcount == 6 and df.shape == (6, 2)
    assert df._frame is not None and df._decoded is None


def test_size_limit_spans_tables():
    df, _ = cursor_like(TABLES, size=4)
    assert [tuple(r.values()) for r in df.arrow().to_pylist()] == ALL[:4]
    assert len(df) == 4 and [tuple(r) for r in df] == ALL[:4]


def test_partly_read_rows_keep_the_v1_path():
    df, rows = cursor_like(TABLES)
    assert tuple(df.fetchone()) == ALL[0]
    table = df.arrow()  # v1: the rest of the shared generator
    assert [tuple(r.values()) for r in table.to_pylist()] == ALL[1:]
    assert df._frame is None


def test_tables_of_different_widths_fall_back_to_rows():
    tables = TABLES[:2] + [pyarrow.table({"id": [9], "name": ["z"], "extra": [True]})]
    df, _ = cursor_like(tables)
    df.materialize()
    assert df._frame is None and isinstance(df._py_rows, list)
    assert [tuple(r)[:2] for r in df._py_rows] == ALL[:5] + [(9, "z")]


def test_other_methods_after_the_switch():
    df, _ = cursor_like(TABLES)
    assert tuple(df.row(4)) == ALL[4]
    assert [tuple(r) for r in df.slice(1, 2)] == ALL[1:3]
    assert [b.rowcount for b in df.to_batches(4)] == [4, 2]
    assert df.nbytes() > 0
    assert [tuple(r) for r in df] == ALL


def test_append_after_unread_rows():
    df, _ = cursor_like(TABLES)
    df.append({"id": 7, "name": "g"})
    assert len(df) == 7 and tuple(df.row(6)) == (7, "g")


def test_empty_stream():
    rows, schema = converters.from_arrow(t for t in [])
    assert list(rows) == []


def test_display_of_unread_rows():
    df, _ = cursor_like(TABLES)
    assert "é" in df.display(limit=10)
