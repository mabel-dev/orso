# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ---------------------------------------------------------------------------------
# v2 (DESIGN.md §4.8): Arrow in and out through the C Data Interface.
#
#   to_arrow     every column is built natively from the record store when the values
#                are ones pyarrow would convert to the target type exactly; any other
#                column is handed to pyarrow as v1's Python list, so it converts (or
#                raises) exactly as it did in v1.
#   from_arrow   record batches are imported natively; columns with no faithful native
#                reader come through pyarrow's own to_pylist().
#
# Behaviour changes (DESIGN.md §8): DATE exports as date32 (fix 10); a column with no
# declared type, or a STRUCT with no fields, is inferred on its own instead of making the
# whole table fall back to inference (fixes 1 and 13); from_arrow(size=n) converts n rows,
# not the whole table (fix 3).
# ---------------------------------------------------------------------------------

import itertools
import typing

from orso._native import Frame
from orso._native import Plan
from orso.compute.compiled import extract_columns_to_lists
from orso.compute.compiled import process_table
from orso.exceptions import MissingDependencyError
from orso.row import Row
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.schema import convert_orso_schema_to_arrow_schema
from orso.types import OrsoTypes

# Cache for optional dependencies
_pyarrow = None
_polars = None
_native_arrow = None  # pyarrow speaks the PyCapsule interface (pyarrow >= 14)


def _get_pyarrow():
    global _pyarrow, _native_arrow
    if _pyarrow is None:
        try:
            import pyarrow
        except ImportError as import_error:
            raise MissingDependencyError(import_error.name) from import_error
        _pyarrow = pyarrow
        _native_arrow = hasattr(pyarrow.RecordBatch, "__arrow_c_array__")
    return _pyarrow


class _Capsules:
    """Hands an exported (schema, array) capsule pair to pyarrow.array()."""

    __slots__ = ("pair",)

    def __init__(self, pair):
        self.pair = pair

    def __arrow_c_array__(self, requested_schema=None):
        return self.pair


class _RowsIterator:
    """
    Iterator class for processing PyArrow tables lazily (the v1 path, used when pyarrow
    predates the PyCapsule interface or a table cannot go into a fixed-width store).

    Parameters:
        tables: Iterable of PyArrow tables to process.
        row_factory: Factory method to create Row instances.
        batch_size: Number of rows to process at a time.
        max_size: Maximum number of rows to return.
    """

    def __init__(
        self, tables: typing.Iterable, row_factory: typing.Callable, batch_size: int, max_size: int
    ):
        self.tables = itertools.chain(tables)
        self.row_factory = row_factory
        self.batch_size = batch_size
        self.max_size = max_size
        self.rows_processed = 0
        self.current_table = None
        self.current_rows = iter([])

    def __iter__(self):
        return self

    def __next__(self):
        if self.rows_processed >= self.max_size:
            raise StopIteration()

        row = next(self.current_rows, None)
        if row is not None:
            self.rows_processed += 1
            return row
        else:
            # Fetch the next table and process it
            self.current_table = next(self.tables, None)
            if self.current_table is None:
                raise StopIteration()

            self.current_rows = iter(
                process_table(self.current_table, self.row_factory, self.batch_size)
            )

            # Check if the new table has rows to process
            row = next(self.current_rows, None)
            if row is not None:
                self.rows_processed += 1
                return row
            else:
                raise StopIteration()


# ---------------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------------


def _spec_for_type(arrow_type):
    """
    The native export spec for a declared pyarrow type, or None when pyarrow should
    convert the column itself. Specs are derived from the declared Arrow type (not the
    orso type) so a natively built array always has exactly the declared type.
    """
    pa = _pyarrow
    t = pa.types
    if t.is_int64(arrow_type):
        return ("int64",)
    if t.is_float64(arrow_type):
        return ("float64",)
    if t.is_boolean(arrow_type):
        return ("bool",)
    if arrow_type == pa.string():
        return ("string",)
    if arrow_type == pa.binary():
        return ("binary",)
    if arrow_type == pa.timestamp("us"):
        return ("timestamp_us",)
    if arrow_type == pa.date32():
        return ("date32",)
    if arrow_type == pa.time32("ms"):
        return ("time32_ms",)
    if arrow_type == pa.time64("us"):
        return ("time64_us",)
    if arrow_type == pa.duration("us"):
        return ("duration_us",)
    if arrow_type == pa.month_day_nano_interval():
        return ("month_day_nano",)
    if t.is_null(arrow_type):
        return ("null",)
    if t.is_decimal128(arrow_type) and 0 <= arrow_type.scale <= arrow_type.precision:
        return ("decimal128", arrow_type.precision, arrow_type.scale)
    if t.is_list(arrow_type):
        field = arrow_type.value_field
        child = _spec_for_type(field.type)
        if child is None or field.name != "item" or not field.nullable:
            return None
        return ("list", child)
    if t.is_struct(arrow_type):
        children = []
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            child = _spec_for_type(field.type)
            if child is None:
                return None
            children.append((field.name, child, field.nullable))
        return ("struct", tuple(children))
    return None


def _export_plan(dataset):
    """
    Per column: (name, declared pyarrow field or None to infer, native spec or None).
    """
    names = dataset.column_names
    schema = getattr(dataset, "schema", None)
    plan = []
    if isinstance(schema, RelationSchema):
        for name, column in zip(names, schema.columns):
            if column.type == OrsoTypes._MISSING_TYPE or (
                column.type == OrsoTypes.STRUCT and not getattr(column, "fields", None)
            ):
                # no declared Arrow type to honour: infer this column, keep the others
                plan.append((name, None, ("infer",)))
                continue
            field = column.arrow_field
            plan.append((name, field, _spec_for_type(field.type)))
    else:
        for name in names:
            plan.append((name, None, ("infer",)))
    return plan


def _native_frame_for(dataset, limit):
    """The record store to export from: the frame's own, or a temporary one built from
    its Python rows. None when the rows cannot go into a fixed-width store."""
    frame = getattr(dataset, "_frame", None)
    if frame is not None:
        return frame
    rows = dataset._rows
    temp = Frame(Plan(dataset._row_factory, None, None, None))
    try:
        temp.extend(rows, limit)
    except (ValueError, TypeError):
        return None
    return temp


def to_arrow(dataset, size=None):
    pa = _get_pyarrow()

    dataset.materialize()
    total_rows = len(dataset)
    limit = min(size, total_rows) if size is not None and size >= 0 else total_rows

    frame = _native_frame_for(dataset, limit) if _native_arrow else None
    if frame is None:
        return _to_arrow_v1(dataset, limit)

    plan = _export_plan(dataset)
    exported = frame.export_columns(
        [spec if spec is not None else ("skip",) for _, _, spec in plan], limit
    )
    arrays = []
    fields = []
    declared = isinstance(getattr(dataset, "schema", None), RelationSchema)
    for j, ((name, field, spec), native) in enumerate(zip(plan, exported)):
        if native is not None:
            array = pa.array(_Capsules(native))
        else:
            values = frame.column_values(j, limit)
            array = pa.array(values) if field is None else pa.array(values, type=field.type)
        arrays.append(array)
        fields.append(field if field is not None else pa.field(name, array.type))
    if declared:
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    return pa.Table.from_arrays(arrays, list(dataset.column_names))


def _to_arrow_v1(dataset, limit):
    """v1's export, for rows that are not all the frame's width or old pyarrow."""
    pa = _pyarrow
    rows = dataset._rows
    column_names = dataset.column_names

    if limit == 0:
        arrays = [list() for _ in column_names]
    else:
        # Use Cython for faster column extraction and respect the requested limit
        arrays = extract_columns_to_lists(rows, limit if limit >= 0 else -1)

    arrow_schema = None
    dataset_schema = getattr(dataset, "schema", None)
    if isinstance(dataset_schema, RelationSchema):
        try:
            arrow_schema = convert_orso_schema_to_arrow_schema(dataset_schema)
        except MissingDependencyError:
            # Fall back to letting PyArrow infer types if optional dependency missing
            arrow_schema = None

        if arrow_schema is not None and any(
            col.type == OrsoTypes.STRUCT and not getattr(col, "fields", None)
            for col in dataset_schema.columns
        ):
            arrow_schema = None

    if arrow_schema is not None:
        return pa.Table.from_arrays(arrays, schema=arrow_schema)
    return pa.Table.from_arrays(arrays, column_names)


# ---------------------------------------------------------------------------------
# import
# ---------------------------------------------------------------------------------


def schema_from_arrow(table) -> RelationSchema:
    """The orso schema for a pyarrow Table or RecordBatch."""
    return RelationSchema(
        name="arrow",
        columns=[FlatColumn.from_arrow(field) for field in table.schema],
    )


def eager_tables(tables):
    """A list of tables when `tables` is a Table, a RecordBatch, or a list or tuple of
    them; None for anything else (generators stay lazy, as in v1)."""
    pa = _get_pyarrow()
    if not _native_arrow:
        return None
    if isinstance(tables, (pa.Table, pa.RecordBatch)):
        return [tables]
    if isinstance(tables, (list, tuple)) and all(
        isinstance(t, (pa.Table, pa.RecordBatch)) for t in tables
    ):
        return list(tables)
    return None


def import_tables(frame, tables, limit: int = -1) -> int:
    """Import tables into `frame` (until `limit` rows when non-negative)."""
    imported = 0
    for table in tables:
        if 0 <= limit <= imported:
            break
        imported += frame.import_arrow(table, -1 if limit < 0 else limit - imported)
    return imported


def _lazy_native_rows(tables, plan, row_factory, limit):
    """Rows from a generator of tables, one table at a time, as v1 iterated them."""
    remaining = limit
    for table in tables:
        if remaining == 0:
            return
        frame = Frame(plan)
        try:
            n = frame.import_arrow(table, remaining)
        except ValueError:
            # a table of a different width cannot share the store: v1's conversion
            rows = process_table(table, row_factory, 10_000)
            if remaining > 0:
                rows = rows[:remaining]
            n = len(rows)
            yield from rows
        else:
            yield from frame.cursor()
        if remaining > 0:
            remaining -= n


def from_arrow(tables, size=None):
    """
    Convert an Arrow table or an iterable of Arrow tables to an iterator of rows and the
    orso schema of the first table. Tables are converted one at a time as rows are read.
    """
    pa = _get_pyarrow()
    single = not isinstance(tables, (typing.Generator, list, tuple))
    if single:
        tables = [tables]

    if isinstance(tables, (list, tuple)):
        tables = iter(tables)

    batch_size: int = 10_000
    if size:
        batch_size = min(size, batch_size)
    else:
        size = float("inf")

    # Extract schema information from the first table
    first_table = next(tables, None)
    if first_table is None:
        return iter([]), {}

    orso_schema = schema_from_arrow(first_table)
    row_factory = Row.create_class(orso_schema, tuples_only=True)

    if not _native_arrow or not isinstance(first_table, (pa.Table, pa.RecordBatch)):
        rows_iterator = _RowsIterator(
            tables=itertools.chain([first_table], tables),
            row_factory=row_factory,
            batch_size=batch_size,
            max_size=size,
        )
        return rows_iterator, orso_schema

    plan = Plan(row_factory, None, None, None)
    limit = -1 if size == float("inf") else int(size)
    if single:
        frame = Frame(plan)
        frame.import_arrow(first_table, limit)
        return frame.cursor(), orso_schema
    return _lazy_native_rows(itertools.chain([first_table], tables), plan, row_factory, limit), orso_schema


def to_pandas(dataset, size=None):
    """wrap the arrow function to convert to pandas"""
    return dataset.arrow(size).to_pandas()


def from_pandas(pandas):
    raise NotImplementedError()


def to_polars(dataset, size=None):
    global _polars
    if _polars is None:
        try:
            import polars

            _polars = polars
        except ImportError as import_error:
            raise MissingDependencyError(import_error.name) from import_error
    return _polars.DataFrame(r.as_dict for r in dataset.slice(0, size))


def from_polars(polars):
    raise NotImplementedError()


def to_csv(dataset):
    raise NotImplementedError()


def from_csv(values):
    raise NotImplementedError()


def to_parquet(dataset):
    raise NotImplementedError()


def from_parquet(values):
    raise NotImplementedError()


def to_jsonl(dataset):
    raise NotImplementedError()


def from_jsonl(values):
    raise NotImplementedError()
