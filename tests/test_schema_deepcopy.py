"""
FlatColumn.__deepcopy__ and RelationSchema.__deepcopy__ (v2): the PyPI opteryx planner
deep-copies schemas on every query, so these copy the hidden slots directly instead of
going through copy's generic path. They must give exactly what copy.deepcopy of a v1
dataclass gave: equal, independent copies, with shared references inside one copied graph
still shared.
"""

import copy
import os
import pickle
import sys
from dataclasses import dataclass

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.schema import ColumnDisposition
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes


def make_column(**kw):
    base = dict(name="planet", type=OrsoTypes.VARCHAR, aliases=["p.name", "n"], description="d")
    base.update(kw)
    return FlatColumn(**base)


def test_column_copy_is_equal_and_independent():
    c = make_column(disposition=ColumnDisposition.NAME)
    d = copy.deepcopy(c)
    assert d == c and d is not c
    assert d.aliases == c.aliases and d.aliases is not c.aliases
    d.aliases.append("x")
    d.name = "renamed"
    assert c.aliases == ["p.name", "n"] and c.name == "planet"
    assert d.type is OrsoTypes.VARCHAR and d.disposition is ColumnDisposition.NAME


def test_nested_fields_are_copied_deeply():
    child = make_column(name="inner", aliases=[])
    c = FlatColumn(name="s", type=OrsoTypes.STRUCT, fields=[child])
    d = copy.deepcopy(c)
    assert d.fields[0] == child and d.fields[0] is not child
    d.fields[0].aliases.append("y")
    assert child.aliases == []


def test_mutable_items_inside_lists_are_copied():
    c = make_column(aliases=[["nested"]])  # not a list of strings: generic path
    d = copy.deepcopy(c)
    assert d.aliases == [["nested"]] and d.aliases[0] is not c.aliases[0]


def test_shared_list_stays_shared_within_one_copy():
    shared = ["a", "b"]
    s = RelationSchema(
        name="t",
        columns=[make_column(name="x", aliases=shared), make_column(name="y", aliases=shared)],
    )
    t = copy.deepcopy(s)
    assert t.columns[0].aliases is t.columns[1].aliases
    assert t.columns[0].aliases is not shared


def test_same_column_twice_is_one_copy():
    c = make_column()
    a, b = copy.deepcopy([c, c])
    assert a is b and a is not c


@dataclass(init=False)
class Expression(FlatColumn):  # how the PyPI opteryx planner subclasses FlatColumn
    expression: object = None
    tags: list = None


def test_dataclass_subclass_carries_its_dict():
    e = Expression(name="e", type=OrsoTypes.INTEGER)
    e.expression = {"op": "add", "args": [1, 2]}
    e.tags = ["t"]
    f = copy.deepcopy(e)
    assert type(f) is Expression and f.name == "e"
    assert f.expression == e.expression and f.expression is not e.expression
    assert f.tags == ["t"] and f.tags is not e.tags


def test_self_reference_through_a_subclass_field():
    e = Expression(name="loop", type=OrsoTypes.INTEGER)
    e.expression = e
    f = copy.deepcopy(e)
    assert f.expression is f


def test_schema_copy_lookups_and_cache():
    s = RelationSchema(
        name="planets", columns=[make_column(name=f"c{i}", aliases=[f"a{i}"]) for i in range(5)]
    )
    assert s.find_column("a3").name == "c3"  # builds the original's cache
    t = copy.deepcopy(s)
    assert t == s and t.columns[0] is not s.columns[0]
    t.columns[3].name = "renamed"
    assert t.find_column("renamed") is t.columns[3]
    assert s.find_column("c3") is s.columns[3] and s.find_column("renamed") is None


def test_unset_slot_reads_default_after_copy():
    c = FlatColumn.__new__(FlatColumn)  # to_flatcolumn() builds columns like this
    object.__setattr__(c, "_name", "bare")
    d = copy.deepcopy(c)
    assert d.name == "bare" and d.disposition == c.disposition


def test_pickle_and_copy_still_work():
    s = RelationSchema(name="p", columns=[make_column()])
    assert pickle.loads(pickle.dumps(s)) == s
    assert copy.copy(s) == s
