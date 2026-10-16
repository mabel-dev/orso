"""
Concurrent use of one frame from many threads (DESIGN.md §6).

On a GIL build these pass trivially; on a free-threaded build (3.14t) they exercise the
frame and store critical sections: appends that grow and move the record buffer while
other threads read the same store through views, cursors, nbytes() and Arrow export.
"""

import os
import sys
import threading

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import orso
from orso._native import Frame, Plan
from orso.row import Row

WRITERS = 4
READERS = 4
PER_WRITER = 5000


def run_threads(targets):
    errors = []
    barrier = threading.Barrier(len(targets))

    def wrap(fn):
        def inner():
            barrier.wait()
            try:
                fn()
            except BaseException as err:  # surfaced below; a thread must not die silently
                errors.append(err)

        return inner

    threads = [threading.Thread(target=wrap(t)) for t in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors


def test_concurrent_appends_and_reads_on_one_store():
    plan = Plan(Row.create_class(["writer", "i", "text"]), None, None, None)
    frame = Frame(plan)
    frame.append({"writer": -1, "i": -1, "text": "seed"})
    done = threading.Event()

    def writer(w):
        def go():
            for i in range(PER_WRITER):
                # long enough to live in the arena, so the arena grows too
                frame.append({"writer": w, "i": i, "text": f"writer {w} row {i} " + "x" * (i % 40)})

        return go

    def reader():
        while not done.is_set():
            n = len(frame)
            assert n >= 1
            view = frame.view(0, n)
            assert view.row(n - 1) is not None
            cursor = frame.cursor()
            assert len(cursor.take(50)) <= 50
            assert frame.nbytes() > 0
            frame.column_values(2, 100)

    writers = [writer(w) for w in range(WRITERS)]

    # each writer thread is independent; the readers stop once every writer has finished
    finished = threading.Semaphore(0)

    def tracked(fn):
        def go():
            try:
                fn()
            finally:
                finished.release()

        return go

    def stopper():
        for _ in range(WRITERS):
            finished.acquire()
        done.set()

    run_threads([tracked(w) for w in writers] + [reader] * READERS + [stopper])

    assert len(frame) == 1 + WRITERS * PER_WRITER
    rows = frame.rows()
    per_writer = {}
    for writer_id, i, text in rows[1:]:
        assert text.startswith(f"writer {writer_id} row {i} ")
        per_writer.setdefault(writer_id, []).append(i)
    # every writer's rows arrive whole and in that writer's order
    assert sorted(per_writer) == list(range(WRITERS))
    assert all(v == list(range(PER_WRITER)) for v in per_writer.values())


def test_concurrent_dataframe_appends_and_arrow():
    df = orso.DataFrame(rows=[], schema=["a", "b"])

    def writer():
        for i in range(PER_WRITER):
            df.append({"a": i, "b": str(i)})

    def exporter():
        for _ in range(20):
            table = df.arrow()
            assert table.num_columns == 2

    run_threads([writer] * WRITERS + [exporter] * 2)
    assert df.rowcount == WRITERS * PER_WRITER
    assert sorted(df.arrow().column("a").to_pylist()) == sorted(list(range(PER_WRITER)) * WRITERS)


def test_concurrent_views_detach():
    # appending to a view copies it first; many threads doing so at once must each get
    # a consistent private copy
    plan = Plan(Row.create_class(["a"]), None, None, None)
    base = Frame(plan)
    for i in range(1000):
        base.append({"a": i})
    view = base.view(100, 100)

    def appender():
        for i in range(200):
            view.append({"a": -i})

    run_threads([appender] * 4)
    assert len(base) == 1000
    values = [r[0] for r in view.rows()]
    assert values[:100] == list(range(100, 200))
    assert len(values) == 100 + 4 * 200


def test_concurrent_display_widths():
    # display() caches East Asian widths in one table shared by every thread
    df = orso.DataFrame([{"a": "漢字かな" * 3, "b": "ｶﾀｶﾅ✓"} for _ in range(50)])

    def show():
        for _ in range(20):
            assert "漢字" in df.display(limit=50)

    run_threads([show] * 6)


def test_concurrent_schema_epoch():
    from orso import _native

    start = _native.schema_epoch()

    def bump():
        for _ in range(10000):
            _native.bump_schema_epoch()

    run_threads([bump] * 4)
    assert _native.schema_epoch() == start + 40000
