"""
Benchmark cases for orso.

Each case is registered with @case(group, name, ops). The decorated function is the
*setup*: it receives a scale factor, builds its input data, and returns a zero-argument
`run` callable that performs the work being timed. `ops` is the number of logical
items (rows, calls, columns) one `run()` processes at scale 1, used for the ns/op column.

Rules for writing a case:
  * everything expensive that is NOT the thing being measured goes in setup
  * `run()` must do the same amount of work every time it is called (fresh frame per
    call if the operation consumes a cursor or mutates the frame)
  * `run()` must force lazy results (from_arrow and several DataFrame methods are lazy)
  * data is generated from a fixed seed so runs are comparable

The groups are weighted towards what the consumers of orso actually do: mabel's
writers append dicts against a RelationSchema, size the frame with nbytes() and flush
it with arrow(); opteryx builds RelationSchemas from FlatColumns in its planner,
converts between Arrow and orso schemas, slices and displays frames.
"""

import datetime
import decimal
import random
from dataclasses import dataclass
from typing import Callable
from typing import List

import pyarrow

from orso import DataFrame
from orso import Row
from orso.converters import from_arrow
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.schema import convert_orso_schema_to_arrow_schema
from orso.tools import random_string
from orso.types import OrsoTypes


@dataclass
class Case:
    group: str
    name: str
    ops: int
    setup: Callable[[float], Callable[[], object]]

    @property
    def id(self) -> str:
        return f"{self.group}/{self.name}"


CASES: List[Case] = []


def case(group: str, name: str, ops: int):
    def register(setup):
        CASES.append(Case(group, name, ops, setup))
        return setup

    return register


# ---------------------------------------------------------------------------------
# shared data
# ---------------------------------------------------------------------------------

COLUMN_NAMES = ("id", "name", "score", "flag", "ts", "tag")
TAGS = ("alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta")
EPOCH = datetime.datetime(2024, 1, 1, 0, 0, 0)


def make_rows(n: int, seed: int = 42) -> list:
    """n rows of (int, str, float, bool, datetime, str|None) - the mixed workload."""
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        rows.append(
            (
                i,
                f"user_{i:07d}",
                rng.random() * 1000.0,
                i % 3 == 0,
                EPOCH + datetime.timedelta(seconds=i * 37),
                None if i % 11 == 0 else TAGS[i % len(TAGS)],
            )
        )
    return rows


def make_dicts(n: int, seed: int = 42) -> list:
    return [dict(zip(COLUMN_NAMES, row)) for row in make_rows(n, seed)]


def make_schema(name: str = "bench") -> RelationSchema:
    return RelationSchema(
        name=name,
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER, nullable=False),
            FlatColumn(name="name", type=OrsoTypes.VARCHAR, nullable=False),
            FlatColumn(name="score", type=OrsoTypes.DOUBLE),
            FlatColumn(name="flag", type=OrsoTypes.BOOLEAN),
            FlatColumn(name="ts", type=OrsoTypes.TIMESTAMP),
            FlatColumn(name="tag", type=OrsoTypes.VARCHAR),
        ],
    )


def make_wide_rows(n: int, width: int = 20) -> list:
    return [tuple(range(i, i + width)) for i in range(n)]


def make_arrow_table(n: int) -> pyarrow.Table:
    cols = list(zip(*make_rows(n)))
    return pyarrow.table(
        {
            "id": pyarrow.array(cols[0], pyarrow.int64()),
            "name": pyarrow.array(cols[1], pyarrow.string()),
            "score": pyarrow.array(cols[2], pyarrow.float64()),
            "flag": pyarrow.array(cols[3], pyarrow.bool_()),
            "ts": pyarrow.array(cols[4], pyarrow.timestamp("us")),
            "tag": pyarrow.array(cols[5], pyarrow.string()),
        }
    )


def consume(iterable) -> int:
    count = 0
    for _ in iterable:
        count += 1
    return count


def repeated(fn: Callable[[], object], times: int) -> Callable[[], object]:
    """Wrap a sub-millisecond operation so one run() is long enough to time reliably."""

    def run():
        result = None
        for _ in range(times):
            result = fn()
        return result

    return run


# ---------------------------------------------------------------------------------
# control: touches nothing in orso. If this case moves against the baseline, the
# machine moved (load, thermal state, power mode), and every other delta in the run
# carries the same bias. bench.py prints it first and warns past the threshold.
# ---------------------------------------------------------------------------------


@case("control", "pure_python_loop", 2_000_000)
def _(scale):
    n = int(2_000_000 * scale)
    words = ("alpha", "beta", "gamma", "delta")

    def run():
        total = 0
        d = {}
        for i in range(n):
            w = words[i & 3]
            total += len(w) + (i % 7)
            d[w] = i
        return total, d

    return run


# ---------------------------------------------------------------------------------
# construct: building frames and schemas
# ---------------------------------------------------------------------------------


@case("construct", "dataframe_from_dicts", 50_000)
def _(scale):
    dicts = make_dicts(int(50_000 * scale))
    return lambda: DataFrame(dicts)


@case("construct", "dataframe_from_rows_materialized", 100_000 * 20)
def _(scale):
    rows = make_rows(int(100_000 * scale))
    schema = make_schema()

    def run():
        df = DataFrame(rows=iter(rows), schema=schema)
        df.materialize()
        return df

    return repeated(run, 20)


@case("construct", "relation_schema_from_kwargs", 200 * 20)
def _(scale):
    n = int(200 * scale)
    specs = [
        {"name": f"col_{i}", "type": ("INTEGER", "VARCHAR", "DOUBLE", "TIMESTAMP")[i % 4]}
        for i in range(20)
    ]

    def run():
        out = []
        for i in range(n):
            out.append(RelationSchema(name=f"rel_{i}", columns=[FlatColumn(**s) for s in specs]))
        return out

    return run


@case("construct", "flatcolumn_init_typed", 20_000)
def _(scale):
    n = int(20_000 * scale)
    return lambda: [FlatColumn(name="c", type=OrsoTypes.VARCHAR) for _ in range(n)]


@case("construct", "flatcolumn_init_from_string_type", 20_000)
def _(scale):
    n = int(20_000 * scale)
    types = ("INTEGER", "VARCHAR[64]", "DECIMAL(10,2)", "ARRAY<VARCHAR>", "TIMESTAMP")
    return lambda: [FlatColumn(name="c", type=types[i % 5]) for i in range(n)]


@case("construct", "flatcolumn_from_arrow", 500 * 6)
def _(scale):
    n = int(500 * scale)
    fields = list(make_arrow_table(10).schema)

    def run():
        out = []
        for _ in range(n):
            out.extend(FlatColumn.from_arrow(f) for f in fields)
        return out

    return run


@case("construct", "orso_schema_to_arrow_schema", 2_000)
def _(scale):
    n = int(2_000 * scale)
    schema = make_schema()
    return lambda: [convert_orso_schema_to_arrow_schema(schema) for _ in range(n)]


@case("construct", "schema_to_dict_from_dict", 500)
def _(scale):
    n = int(500 * scale)
    schema = make_schema()
    return lambda: [RelationSchema.from_dict(schema.to_dict()) for _ in range(n)]


@case("construct", "schema_find_column", 50_000)
def _(scale):
    n = int(50_000 * scale)
    schema = RelationSchema(
        name="wide",
        columns=[FlatColumn(name=f"col_{i}", type=OrsoTypes.INTEGER) for i in range(20)],
    )
    find = schema.find_column
    return lambda: [find("col_19") for _ in range(n)]


@case("construct", "random_string", 100_000)
def _(scale):
    n = int(100_000 * scale)
    return lambda: [random_string() for _ in range(n)]


# ---------------------------------------------------------------------------------
# append: the writer path (mabel)
# ---------------------------------------------------------------------------------


@case("append", "dict_with_relation_schema", 50_000)
def _(scale):
    dicts = make_dicts(int(50_000 * scale))
    schema = make_schema()

    def run():
        df = DataFrame(rows=[], schema=schema)
        append = df.append
        for d in dicts:
            append(d)
        return df

    return run


@case("append", "dict_with_column_names", 50_000)
def _(scale):
    dicts = make_dicts(int(50_000 * scale))

    def run():
        df = DataFrame(rows=[], schema=list(COLUMN_NAMES))
        append = df.append
        for d in dicts:
            append(d)
        return df

    return run


@case("append", "tuple_with_column_names", 50_000)
def _(scale):
    rows = make_rows(int(50_000 * scale))

    def run():
        df = DataFrame(rows=[], schema=list(COLUMN_NAMES))
        append = df.append
        for r in rows:
            append(r)
        return df

    return run


@case("append", "dict_with_jsonb_column", 20_000)
def _(scale):
    n = int(20_000 * scale)
    schema = RelationSchema(
        name="events",
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER),
            FlatColumn(name="payload", type=OrsoTypes.JSONB),
        ],
    )
    dicts = [
        {"id": i, "payload": {"k": i, "tags": ["a", "b"], "nested": {"x": 1.5}}} for i in range(n)
    ]

    def run():
        df = DataFrame(rows=[], schema=schema)
        append = df.append
        for d in dicts:
            append(d)
        return df

    return run


@case("append", "schema_validate", 50_000)
def _(scale):
    dicts = make_dicts(int(50_000 * scale))
    validate = make_schema().validate
    return lambda: [validate(d) for d in dicts]


@case("append", "append_then_nbytes_then_arrow", 20_000)
def _(scale):
    """The blob-writer loop: append, size the frame every 1000 rows, flush to Arrow."""
    dicts = make_dicts(int(20_000 * scale))
    schema = make_schema()

    def run():
        df = DataFrame(rows=[], schema=schema)
        append = df.append
        for i, d in enumerate(dicts, 1):
            append(d)
            if i % 1000 == 0:
                df.nbytes()
        return df.arrow()

    return run


# ---------------------------------------------------------------------------------
# row: the Row tuple subclass
# ---------------------------------------------------------------------------------


@case("row", "create_from_dict", 100_000)
def _(scale):
    dicts = make_dicts(int(100_000 * scale))
    factory = Row.create_class(list(COLUMN_NAMES))
    return lambda: [factory(d) for d in dicts]


@case("row", "create_from_tuple", 100_000)
def _(scale):
    rows = make_rows(int(100_000 * scale))
    factory = Row.create_class(list(COLUMN_NAMES))
    return lambda: [factory(r) for r in rows]


@case("row", "create_from_tuple_tuples_only", 100_000)
def _(scale):
    rows = make_rows(int(100_000 * scale))
    factory = Row.create_class(list(COLUMN_NAMES), tuples_only=True)
    return lambda: [factory(r) for r in rows]


@case("row", "as_dict", 100_000)
def _(scale):
    factory = Row.create_class(list(COLUMN_NAMES))
    rows = [factory(r) for r in make_rows(int(100_000 * scale))]
    return lambda: [r.as_dict for r in rows]


@case("row", "get_by_name", 100_000)
def _(scale):
    factory = Row.create_class(list(COLUMN_NAMES))
    rows = [factory(r) for r in make_rows(int(100_000 * scale))]
    return lambda: [r.get("tag") for r in rows]


@case("row", "as_bytes", 50_000)
def _(scale):
    factory = Row.create_class(list(COLUMN_NAMES))
    rows = [factory(r) for r in make_rows(int(50_000 * scale))]
    return lambda: [r.as_bytes for r in rows]


@case("row", "from_bytes", 50_000)
def _(scale):
    factory = Row.create_class(list(COLUMN_NAMES))
    blobs = [factory(r).as_bytes for r in make_rows(int(50_000 * scale))]
    from_bytes = factory.from_bytes
    return lambda: [from_bytes(b) for b in blobs]


@case("row", "as_json", 50_000)
def _(scale):
    factory = Row.create_class(list(COLUMN_NAMES))
    rows = [factory(r) for r in make_rows(int(50_000 * scale))]
    return lambda: [r.as_json for r in rows]


@case("row", "nbytes_first_call", 50_000)
def _(scale):
    """nbytes caches per Row, so each run must build fresh rows; the factory cost is
    small next to the msgpack serialisation being measured."""
    rows = make_rows(int(50_000 * scale))
    factory = Row.create_class(list(COLUMN_NAMES))
    return lambda: [factory(r).nbytes() for r in rows]


# ---------------------------------------------------------------------------------
# arrow: conversion to and from pyarrow
# ---------------------------------------------------------------------------------


@case("arrow", "to_arrow_inferred_types", 100_000)
def _(scale):
    df = DataFrame(rows=make_rows(int(100_000 * scale)), schema=list(COLUMN_NAMES))
    return df.arrow


@case("arrow", "to_arrow_relation_schema", 100_000)
def _(scale):
    df = DataFrame(rows=make_rows(int(100_000 * scale)), schema=make_schema())
    return df.arrow


@case("arrow", "to_arrow_decimal_columns", 50_000)
def _(scale):
    n = int(50_000 * scale)
    schema = RelationSchema(
        name="prices",
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER),
            FlatColumn(name="price", type=OrsoTypes.DECIMAL, precision=10, scale=2),
            FlatColumn(name="qty", type=OrsoTypes.DECIMAL, precision=10, scale=2),
        ],
    )
    rows = [(i, decimal.Decimal(i) / 100, decimal.Decimal("99.99")) for i in range(n)]
    df = DataFrame(rows=rows, schema=schema)
    return df.arrow


@case("arrow", "to_arrow_wide_20_columns", 100_000)
def _(scale):
    df = DataFrame(rows=make_wide_rows(int(100_000 * scale)), schema=[f"c{i}" for i in range(20)])
    return df.arrow


@case("arrow", "to_arrow_limit_1000_of_100k", 1_000 * 50)
def _(scale):
    df = DataFrame(rows=make_rows(int(100_000 * scale)), schema=make_schema())
    return repeated(lambda: df.arrow(size=1_000), 50)


@case("arrow", "from_arrow_materialized", 100_000)
def _(scale):
    table = make_arrow_table(int(100_000 * scale))

    def run():
        df = DataFrame.from_arrow(table)
        df.materialize()
        return df

    return run


@case("arrow", "from_arrow_10_chunks_materialized", 100_000)
def _(scale):
    n = int(100_000 * scale)
    chunk = n // 10
    table = pyarrow.concat_tables([make_arrow_table(chunk) for _ in range(10)])

    def run():
        df = DataFrame.from_arrow(table)
        df.materialize()
        return df

    return run


@case("arrow", "from_arrow_list_of_tables_materialized", 100_000)
def _(scale):
    n = int(100_000 * scale)
    tables = [make_arrow_table(n // 10) for _ in range(10)]

    def run():
        df = DataFrame.from_arrow(tables)
        df.materialize()
        return df

    return run


@case("arrow", "from_arrow_then_read_every_row", 100_000)
def _(scale):
    """Import and then touch every row: the end-to-end cost when rows are actually used
    (v2 defers decoding, so from_arrow_materialized alone would flatter it)."""
    table = make_arrow_table(int(100_000 * scale))
    return lambda: consume(DataFrame.from_arrow(table))


@case("arrow", "from_arrow_first_1000_rows", 1_000)
def _(scale):
    table = make_arrow_table(int(100_000 * scale))

    def run():
        rows, schema = from_arrow(table, size=1_000)
        return list(rows)

    return run


@case("arrow", "roundtrip_arrow_orso_arrow", 100_000)
def _(scale):
    table = make_arrow_table(int(100_000 * scale))
    return lambda: DataFrame.from_arrow(table).arrow()


# ---------------------------------------------------------------------------------
# iterate: reading rows back out
# ---------------------------------------------------------------------------------


@case("iterate", "for_row_in_materialized", 100_000 * 20)
def _(scale):
    df = DataFrame(rows=make_rows(int(100_000 * scale)), schema=make_schema())
    return repeated(lambda: consume(df), 20)


@case("iterate", "for_row_in_generator_backed", 100_000 * 10)
def _(scale):
    rows = make_rows(int(100_000 * scale))
    schema = make_schema()
    return repeated(lambda: consume(DataFrame(rows=(r for r in rows), schema=schema)), 10)


@case("iterate", "fetchall", 100_000 * 20)
def _(scale):
    rows = make_rows(int(100_000 * scale))
    schema = make_schema()
    return repeated(lambda: DataFrame(rows=rows, schema=schema).fetchall(), 20)


@case("iterate", "fetchmany_1000_loop", 100_000 * 10)
def _(scale):
    rows = make_rows(int(100_000 * scale))
    schema = make_schema()

    def run():
        df = DataFrame(rows=rows, schema=schema)
        count = 0
        while True:
            batch = df.fetchmany(1_000)
            if not batch:
                return count
            count += len(batch)

    return repeated(run, 10)


@case("iterate", "fetchone_loop", 100_000 * 10)
def _(scale):
    rows = make_rows(int(100_000 * scale))
    schema = make_schema()

    def run():
        df = DataFrame(rows=rows, schema=schema)
        count = 0
        while df.fetchone() is not None:
            count += 1
        return count

    return repeated(run, 10)


@case("iterate", "collect_one_column", 100_000 * 20)
def _(scale):
    df = DataFrame(rows=make_rows(int(100_000 * scale)), schema=make_schema())
    return repeated(lambda: df.collect("tag"), 20)


@case("iterate", "collect_three_columns", 100_000 * 20)
def _(scale):
    df = DataFrame(rows=make_rows(int(100_000 * scale)), schema=make_schema())
    return repeated(lambda: df.collect(["id", "score", "tag"]), 20)


@case("iterate", "to_batches_1000", 100_000 * 20)
def _(scale):
    df = DataFrame(rows=make_rows(int(100_000 * scale)), schema=make_schema())
    return repeated(lambda: consume(df.to_batches(1_000)), 20)


@case("iterate", "head_5_repeated", 20_000)
def _(scale):
    n = int(20_000 * scale)
    df = DataFrame(rows=make_rows(1_000), schema=make_schema())
    return lambda: [df.head(5) for _ in range(n)]


@case("iterate", "slice_10k_window", 2_000)
def _(scale):
    n = int(2_000 * scale)
    df = DataFrame(rows=make_rows(100_000), schema=make_schema())
    return lambda: [df.slice(offset=i * 40, length=10_000) for i in range(n)]


@case("iterate", "query_predicate", 100_000 * 10)
def _(scale):
    df = DataFrame(rows=make_rows(int(100_000 * scale)), schema=make_schema())
    return repeated(lambda: df.query(lambda r: r[3]).materialize(), 10)


@case("iterate", "distinct", 100_000 * 5)
def _(scale):
    n = int(100_000 * scale)
    factory = Row.create_class(list(COLUMN_NAMES))
    # one in ten rows is unique, the rest are repeats of the first n//10
    base = make_rows(n // 10)
    rows = [factory(base[i % len(base)]) for i in range(n)]
    df = DataFrame(rows=rows, schema=make_schema())
    return repeated(lambda: df.distinct().materialize(), 5)


@case("iterate", "group_by_count", 100_000)
def _(scale):
    df = DataFrame(rows=make_rows(int(100_000 * scale)), schema=make_schema())
    return lambda: df.group_by("tag").count().materialize()


# ---------------------------------------------------------------------------------
# display: rendering
# ---------------------------------------------------------------------------------


@case("display", "ascii_100_rows_with_types", 100 * 5)
def _(scale):
    df = DataFrame(rows=make_rows(int(1_000 * scale)), schema=make_schema())
    return repeated(lambda: df.display(limit=100, show_types=True), 5)


@case("display", "ascii_wide_50_columns", 20 * 10)
def _(scale):
    df = DataFrame(
        rows=[tuple(range(i, i + 50)) for i in range(int(100 * scale))],
        schema=[f"col_{i}" for i in range(50)],
    )
    return repeated(lambda: df.display(limit=20), 10)


@case("display", "str_of_100k_row_frame", 10 * 20)
def _(scale):
    """str() shows 10 rows top-and-tail; this checks nothing scans the whole frame."""
    df = DataFrame(rows=make_rows(int(100_000 * scale)), schema=make_schema())
    return repeated(lambda: str(df), 20)


@case("display", "markdown_50_rows", 50 * 50)
def _(scale):
    df = DataFrame(rows=make_rows(int(1_000 * scale)), schema=make_schema())
    return repeated(lambda: df.markdown(limit=50), 50)


# ---------------------------------------------------------------------------------
# types: OrsoTypes parsing and casting
# ---------------------------------------------------------------------------------


@case("types", "from_name", 50_000)
def _(scale):
    n = int(50_000 * scale)
    names = (
        "INTEGER",
        "VARCHAR",
        "VARCHAR[64]",
        "DECIMAL(10,2)",
        "ARRAY<VARCHAR>",
        "TIMESTAMP",
        "double",
    )
    from_name = OrsoTypes.from_name
    return lambda: [from_name(names[i % len(names)]) for i in range(n)]


@case("types", "parse_scalars", 100_000)
def _(scale):
    n = int(100_000 * scale)
    values = [
        (OrsoTypes.INTEGER, "12345"),
        (OrsoTypes.DOUBLE, "3.25"),
        (OrsoTypes.VARCHAR, 42),
        (OrsoTypes.BOOLEAN, "true"),
    ]
    return lambda: [values[i % 4][0].parse(values[i % 4][1]) for i in range(n)]


@case("types", "parse_timestamps", 50_000)
def _(scale):
    n = int(50_000 * scale)
    stamps = [(EPOCH + datetime.timedelta(seconds=i * 37)).isoformat() for i in range(n)]
    parse = OrsoTypes.TIMESTAMP.parse
    return lambda: [parse(s) for s in stamps]


@case("types", "parse_dates", 50_000)
def _(scale):
    n = int(50_000 * scale)
    dates = [(EPOCH + datetime.timedelta(days=i % 3000)).date().isoformat() for i in range(n)]
    parse = OrsoTypes.DATE.parse
    return lambda: [parse(s) for s in dates]


@case("types", "parse_decimals", 50_000)
def _(scale):
    n = int(50_000 * scale)
    values = [f"{i}.{i % 100:02d}" for i in range(n)]
    parse = OrsoTypes.DECIMAL.parse
    return lambda: [parse(v, precision=12, scale=2) for v in values]
