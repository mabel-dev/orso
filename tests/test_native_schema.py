"""
Phase 4 of the v2 rewrite (DESIGN.md §10): slotted schema classes, cached lookups that
stay correct under mutation, native validation and parsing, and the schema bugs found
in phase 3.
"""

import copy
import dataclasses
import datetime
import decimal
import os
import pickle
import random
import sys
import warnings
from typing import Any

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import orso
from orso import _native
from orso.exceptions import DataValidationError
from orso.exceptions import ExcessColumnsInDataError
from orso.schema import ConstantColumn
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.schema import convert_orso_schema_to_arrow_schema
from orso.tools import DecimalFactory
from orso.tools import random_string
from orso.types import OrsoTypes


def schema_of(*specs):
    return RelationSchema(name="t", columns=[FlatColumn(**s) for s in specs])


# ---------------------------------------------------------------------------------
# cached lookups follow every mutation the PyPI opteryx binder makes
# ---------------------------------------------------------------------------------


def test_find_column_follows_renames_and_aliases():
    schema = schema_of({"name": "a"}, {"name": "b", "aliases": ["bee"]})
    assert schema.find_column("bee").name == "b"
    schema.columns[1].name = "renamed"  # opteryx: column.name = column.identity
    assert schema.find_column("b") is None and schema.find_column("renamed") is schema.columns[1]
    schema.columns[1].aliases = []  # opteryx: schema_column.aliases = []
    assert schema.find_column("bee") is None
    schema.columns[0].aliases = ["alpha"]
    assert schema.find_column("ALPHA", case_insensitive=True) is schema.columns[0]
    assert schema.all_column_names() == ["alpha", "a", "renamed"]


def test_find_column_follows_list_changes():
    schema = schema_of({"name": "a"})
    assert schema.find_column("x") is None
    schema.columns.append(FlatColumn(name="x"))
    assert schema.find_column("x") is schema.columns[1]
    schema.pop_column("a")
    assert schema.find_column("a") is None and schema.column_names == ["x"]
    schema.columns = [FlatColumn(name="y")]  # opteryx: schema.columns = [...]
    assert schema.find_column("x") is None and schema.find_column("y") is not None


def test_first_match_wins_like_v1():
    schema = schema_of({"name": "a"}, {"name": "b", "aliases": ["a"]})
    assert schema.find_column("a") is schema.columns[0]
    schema.columns[0].name = "z"
    assert schema.find_column("a") is schema.columns[1]


def test_arrow_schema_follows_type_changes():
    pa = pytest.importorskip("pyarrow")
    schema = schema_of({"name": "a", "type": OrsoTypes.INTEGER})
    assert convert_orso_schema_to_arrow_schema(schema).field("a").type == pa.int64()
    schema.columns[0].type = OrsoTypes.TIMESTAMP  # opteryx retypes columns in place
    assert convert_orso_schema_to_arrow_schema(schema).field("a").type == pa.timestamp("us")
    assert schema.columns[0].arrow_field.type == pa.timestamp("us")


def test_validation_follows_nullability_changes():
    schema = schema_of({"name": "a", "type": OrsoTypes.INTEGER})
    assert schema.validate({"a": None})
    schema.columns[0].nullable = False
    with pytest.raises(DataValidationError):
        schema.validate({"a": None})


def test_unhashable_name_still_scans():
    schema = schema_of({"name": "a"})
    assert schema.find_column(["a"]) is None


# ---------------------------------------------------------------------------------
# native validation agrees with v1's
# ---------------------------------------------------------------------------------

TYPED = [
    {"name": "i", "type": OrsoTypes.INTEGER, "nullable": False},
    {"name": "s", "type": OrsoTypes.VARCHAR},
    {"name": "t", "type": OrsoTypes.TIMESTAMP},
    {"name": "d", "type": OrsoTypes.DATE},
    {"name": "dec", "type": OrsoTypes.DECIMAL},
    {"name": "j", "type": OrsoTypes.JSONB},
    {"name": "any"},
]
VALUES = [None, 1, True, "x", 1.5, b"b", datetime.datetime(2024, 1, 1), datetime.date(2024, 1, 1),
          decimal.Decimal("1.5"), [1], {"k": 1}]


def test_validate_matches_v1_on_random_records():
    schema = schema_of(*TYPED)
    rng = random.Random(4)
    names = [c["name"] for c in TYPED]
    for _ in range(3000):
        record = {n: rng.choice(VALUES) for n in names if rng.random() > 0.05}
        if rng.random() < 0.05:
            record["extra"] = 1
        try:
            expected = schema._validate_slow(record)
        except Exception as err:
            with pytest.raises(type(err)) as raised:
                schema.validate(record)
            assert str(raised.value) == str(err)
        else:
            assert schema.validate(record) is expected


def test_validate_null_typed_column_raises_like_v1():
    schema = schema_of({"name": "n", "type": OrsoTypes.NULL})
    with pytest.raises(TypeError):
        schema.validate({"n": 1})
    assert schema.validate({"n": None})


def test_validate_rejects_non_mappings_like_v1():
    schema = schema_of({"name": "a"})
    with pytest.raises(TypeError):
        schema.validate((1,))
    with pytest.raises(ExcessColumnsInDataError):
        schema.validate({"a": 1, "b": 2})


# ---------------------------------------------------------------------------------
# the dataclass contract consumers build on
# ---------------------------------------------------------------------------------


@dataclasses.dataclass(init=False)
class ExpressionColumn(FlatColumn):  # exactly as the PyPI opteryx planner declares it
    expression: Any = None


def test_dataclass_subclass_inherits_every_field():
    e = ExpressionColumn(name="", type=OrsoTypes.VARCHAR, expression="a > 1")
    assert [f.name for f in dataclasses.fields(e)][-1] == "expression"
    assert len(dataclasses.fields(e)) == len(dataclasses.fields(FlatColumn)) + 1
    assert e.type == OrsoTypes.VARCHAR and e.expression == "a > 1"
    same = copy.deepcopy(e)
    assert same == e and same is not e
    other = ExpressionColumn(name="x", identity=e.identity, expression="a > 1")
    assert other != e  # equality covers the inherited fields, as in v1
    assert repr(e).startswith("ExpressionColumn(name='', default=None, type=")


def test_asdict_and_json_round_trip():
    column = FlatColumn(name="a", type="DECIMAL(10,2)", aliases=["x"])
    as_dict = dataclasses.asdict(column)
    assert as_dict["precision"] == 10 and as_dict["aliases"] == ["x"]
    assert FlatColumn.from_json(column.to_json()) == column
    schema = RelationSchema(name="s", columns=[column])
    assert RelationSchema.from_dict(schema.to_dict()).columns[0].name == "a"


def test_copies_and_pickles_drop_caches():
    schema = schema_of({"name": "a", "aliases": ["x"]}, {"name": "b"})
    schema.find_column("x")
    schema.validate({"a": 1, "b": 2})
    for clone in (copy.deepcopy(schema), pickle.loads(pickle.dumps(schema)), copy.copy(schema)):
        assert clone == schema
        assert clone.find_column("x").name == "a"
    deep = copy.deepcopy(schema)
    deep.columns[0].name = "changed"
    assert schema.columns[0].name == "a" and deep.find_column("changed") is deep.columns[0]


def test_columns_have_no_instance_dict():
    with pytest.raises(AttributeError):
        FlatColumn(name="a").__dict__


def test_relation_schema_positional_signature():
    columns = [FlatColumn(name="a")]
    schema = RelationSchema("n", ["al"], columns, "a", 5)
    assert (schema.name, schema.aliases, schema.columns, schema.primary_key, schema.row_count_metric) == (
        "n", ["al"], columns, "a", 5,
    )
    assert RelationSchema("n", aliases=None).aliases is None  # an explicit None is kept


def test_unset_field_reads_default_or_names_the_field():
    column = object.__new__(FlatColumn)
    assert column.disposition is None  # v1 read class-level defaults
    with pytest.raises(AttributeError, match="'expectations'"):
        column.expectations


def test_to_flatcolumn_keeps_every_field():
    # v1's copy lost length and disposition and had no expectations attribute
    from orso.schema import ColumnDisposition

    column = FlatColumn(name="a", type="VARCHAR[10]", disposition=ColumnDisposition.NAME)
    copied = column.to_flatcolumn()
    assert dataclasses.asdict(copied) == dataclasses.asdict(column)


def test_constant_column_keeps_its_own_fields():
    c = ConstantColumn(name="c", value=7, length=4, type=OrsoTypes.INTEGER)
    assert (c.length, c.value, list(c.materialize())) == (4, 7, [7, 7, 7, 7])
    assert copy.deepcopy(c) == c


# ---------------------------------------------------------------------------------
# parsers and type names
# ---------------------------------------------------------------------------------


def test_from_name_cache_keeps_warnings_and_errors():
    for _ in range(3):
        with pytest.warns(UserWarning, match="LIST"):
            OrsoTypes.from_name("LIST")
        with pytest.raises(ValueError):
            OrsoTypes.from_name("NOT A TYPE")
    assert OrsoTypes.from_name("DECIMAL(10,2)") == (OrsoTypes.DECIMAL, None, 10, 2, None)
    # cache keys include the type, so False is not served 0's cached answer
    assert OrsoTypes.from_name(0)[0] == 0
    with pytest.raises(ValueError):
        OrsoTypes.from_name(False)


def test_parse_paths_agree():
    assert OrsoTypes.INTEGER.parse("12") == 12
    assert OrsoTypes.VARCHAR.parse(b"abcdef", length=3) == "abc"
    assert OrsoTypes.DECIMAL.parse("1.235", precision=10, scale=2) == decimal.Decimal("1.24")
    assert OrsoTypes.TIMESTAMP.parse("2024-01-01T00:00:00.25+01:00") == datetime.datetime(2023, 12, 31, 23, 0, 0, 250000)
    assert OrsoTypes.DATE.parse("2024-02-29") == datetime.date(2024, 2, 29)
    with pytest.raises(ValueError):
        OrsoTypes.DATE.parse("2023-02-29")


def test_decimal_factory_cache_is_transparent():
    a = DecimalFactory.new_factory(10, 2)
    assert a is DecimalFactory.new_factory(10, 2)
    assert a("3.14159") == decimal.Decimal("3.14")
    assert DecimalFactory.new_factory(10, 4)("3.14159") == decimal.Decimal("3.1416")


def test_random_string_is_always_full_width():
    assert {len(random_string()) for _ in range(20000)} == {16}
    assert {len(random_string(7)) for _ in range(2000)} == {7}


# ---------------------------------------------------------------------------------
# bugs found in phase 3
# ---------------------------------------------------------------------------------


def test_decimal_scale_zero_exports_as_scale_zero():
    pa = pytest.importorskip("pyarrow")
    column = FlatColumn(name="d", type="DECIMAL(10,0)")
    assert column.arrow_field.type == pa.decimal128(10, 0)  # v1: decimal128(10, 10)
    df = orso.DataFrame(rows=[(decimal.Decimal(7),)], schema=RelationSchema(name="t", columns=[column]))
    assert df.arrow().column(0).to_pylist() == [decimal.Decimal(7)]


def test_from_arrow_does_not_write_parameters_onto_the_shared_type():
    pa = pytest.importorskip("pyarrow")
    before = (OrsoTypes.DECIMAL._precision, OrsoTypes.DECIMAL._scale)
    FlatColumn.from_arrow(pa.field("x", pa.decimal128(7, 3)))
    assert (OrsoTypes.DECIMAL._precision, OrsoTypes.DECIMAL._scale) == before


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
