"""
The native store takes part in cyclic garbage collection (DESIGN.md §4.2).

A store holds a strong reference to every value it keeps as a Python object. A value
that refers back to its frame makes a cycle the collector must be able to see; and since
views and cursors share one store, those references must be counted exactly once or the
collector would free values that are still reachable.
"""

import gc
import os
import sys
import weakref

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import orso
from orso._native import Frame, Plan
from orso.row import Row


class Holder:
    pass


def frame_with(value):
    plan = Plan(Row.create_class(["a"]), None, None, None)
    frame = Frame(plan)
    frame.append({"a": value})
    return frame


def collected(build):
    ref = build()
    gc.collect()
    return ref() is None


def test_cycle_through_a_dataframe_is_collected():
    def build():
        h = Holder()
        df = orso.DataFrame(rows=[], schema=["a"])
        df.append({"a": h})
        h.df = df
        return weakref.ref(h)

    assert collected(build)


def test_cycle_through_a_view_is_collected():
    def build():
        h = Holder()
        frame = frame_with(h)
        h.view = frame.view(0, 1)
        h.cursor = frame.cursor()
        return weakref.ref(h)

    assert collected(build)


def test_cycle_through_the_plan_is_collected():
    def build():
        h = Holder()
        h.plan = Plan(Row.create_class(["a"]), None, None, None)
        h.frame = Frame(h.plan)
        h.frame.append({"a": h})
        return weakref.ref(h)

    assert collected(build)


def test_shared_store_values_survive_collection():
    # many owners of one store: none of its values may be collected while any is alive
    values = [Holder() for _ in range(50)]
    plan = Plan(Row.create_class(["a"]), None, None, None)
    frame = Frame(plan)
    for v in values:
        frame.append({"a": v})
    views = [frame.view(i, 10) for i in range(0, 50, 10)]
    cursors = [frame.cursor() for _ in range(3)]
    refs = [weakref.ref(v) for v in values]
    del values, v
    for _ in range(3):
        gc.collect()
    assert all(r() is not None for r in refs)
    assert [row[0] for row in frame.rows()] == [r() for r in refs]
    assert views[2].row(0)[0] is refs[20]()
    assert next(cursors[0])[0] is refs[0]()


def test_store_objects_are_released_with_the_frame():
    h = Holder()
    ref = weakref.ref(h)
    frame = frame_with(h)
    del h
    assert ref() is not None
    del frame
    assert ref() is None  # no cycle: plain reference counting frees it at once


def test_store_type_is_internal():
    # the store object cannot be made from Python; frames make their own
    import pytest

    from orso import _native

    with pytest.raises(TypeError):
        _native._Store()
