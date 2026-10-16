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
# v2 (DESIGN.md §4.3): a DataFrame's rows live in one of two places, and stay in the
# form they arrived in until an operation needs the other form.
#
#   list frame    rows handed to the constructor (a list, a tuple, a generator) or built
#                 by DataFrame(dictionaries) are kept as Python objects in `_py_rows`,
#                 exactly as v1 kept them. Nothing is encoded, so passing rows through
#                 costs what it always did.
#   store frame   rows appended to an empty frame go into a native record store
#                 (`_frame`, orso._native.Frame): validated and encoded in C, sized in
#                 O(1), sliced as views, decoded to Row objects only when read.
#
# Reading `_rows` on a store frame decodes it once and turns it into a list frame, so the
# v1 code paths that still want a Python list (arrow, display, collect, group_by...) see
# one. Phase 3 and 5 give those paths native routes and the conversion goes away.
# ---------------------------------------------------------------------------------

import typing
from collections.abc import MutableMapping
from itertools import islice
from typing import Generator
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from orso import Row
from orso._json import dumps_bytes as json_dumps_bytes
from orso._native import Frame
from orso._native import collect_rows_into
from orso._native import xxh64 as _xxh64
from orso._native import FrameCursor
from orso._native import Plan
from orso._native import rows_from_dicts
from orso._native import rows_nbytes
from orso.exceptions import MissingDependencyError
from orso.group_by import GroupBy
from orso.schema import RelationSchema
from orso.types import ORSO_TO_PYTHON_MAP
from orso.types import OrsoTypes


def _numpy():
    """numpy, which collect() returns arrays of; no longer a hard requirement (v2)."""
    try:
        import numpy
    except ImportError as import_error:  # pragma: no cover
        raise MissingDependencyError(import_error.name) from import_error
    return numpy


class DataFrame:
    """
    Orso DataFrames are a lightweight container for tabular data.
    """

    __slots__ = (
        "_schema",
        "_py_rows",
        "_frame",
        "_decoded",
        "_plan",
        "_cursor",
        "_rf",
        "_colnames",
        "arraysize",
        "_nbytes",
        "_nbytes_upto",
    )

    _LIST_ITERATOR_TYPE = type(iter([]))
    _TUPLE_ITERATOR_TYPE = type(iter(()))

    def __init__(
        self,
        dictionaries: Optional[Iterable[dict]] = None,
        *,
        rows: Union[List[tuple], Generator[tuple, None, None], None] = None,
        schema: Union[RelationSchema, List[str], Tuple[str], None] = None,
    ):
        """
        Create an orso DataFrame. DataFrames are a representation of a table with
        rows of records which have the same fields of the same data types.

        Parameters:
            dictionaries: iterable of dicts (optional)
                An iterable of dictionaries. The schema for the frame is determined
                from the first dictionary in the iterable. Schemas for dictionary
                defined DataFrames are forgiving, missing fields are substituted with
                None and common types are not enforced.
            rows: iterable of tuples (optional, keyworded)
                An iterable of tuples representing a row in the DataFrame. Rows should
                conform to the schema, this is not enforced at creation. Must be used
                with 'schema' and cannot be used with 'dictionaries'.
            schema: RelationSchema or list of column names (optional, keyworded)
                A RelationSchema describing the schema of the DataFrame or a list of
                the column names. Must be used with 'rows' and cannot be used with
                'dictionaries'

        """
        # `_nbytes` is the summed size of rows[:_nbytes_upto] for list frames; nbytes()
        # tops it up with rows appended since. Store frames keep their own watermark.
        self._nbytes = 0
        self._nbytes_upto = 0
        self._frame = None
        self._decoded = None
        self._plan = None
        self._rf = None
        self._colnames = None
        if dictionaries is not None:
            if schema is not None or rows is not None:
                raise ValueError(
                    "Can't implicitly and explicitly define a DataFrame at the same time"
                )

            # make the list of dicts iterable
            dicts = iter(dictionaries)
            # extract the first of the list, and get the column names from it
            first_dict = next(dicts)

            self._schema = [str(k) for k in first_dict]
            row_factory = Row.create_class(self._schema)
            self._rf = row_factory
            # one native pass: every dict read by the first dict's keys, missing -> None
            self._py_rows = rows_from_dicts(
                row_factory, tuple(first_dict.keys()), first_dict, dicts
            )
        else:
            if not isinstance(schema, (RelationSchema, list, tuple)):
                raise ValueError("Row requires either a list of field names or a RelationSchema")
            self._schema = schema  # type:ignore
            self._py_rows = rows or []  # type:ignore
        self.arraysize = 100
        self._cursor = iter(self._py_rows or [])

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    @staticmethod
    def _from_frame(frame, parent: "DataFrame") -> "DataFrame":
        """A store frame over `frame` (a view) that shares `parent`'s schema and plan."""
        df = DataFrame.__new__(DataFrame)
        df._schema = parent._schema
        df._py_rows = None
        df._frame = frame
        df._decoded = None
        df._plan = parent._plan
        df._rf = parent._rf
        df._colnames = parent._colnames
        df.arraysize = 100
        df._nbytes = 0
        df._nbytes_upto = 0
        df._cursor = frame.cursor()
        return df

    # ------------------------------------------------------------------------------
    # storage plumbing
    # ------------------------------------------------------------------------------

    @property
    def _rows(self):
        """
        The rows as a Python sequence (list, tuple or iterator), as v1 exposed them.
        On a store frame this decodes every row once and the frame becomes a list frame.
        """
        if self._frame is not None:
            self._to_list_frame()
        return self._py_rows

    @_rows.setter
    def _rows(self, value):
        self._frame = None
        self._decoded = None
        self._py_rows = value

    def _to_list_frame(self):
        frame = self._frame
        rows = self._decoded if self._decoded is not None else frame.rows()
        cursor = self._cursor
        position = cursor.position if type(cursor) is FrameCursor else None
        self._frame = None
        self._decoded = None
        self._py_rows = rows
        self._nbytes = 0
        self._nbytes_upto = 0
        if position is not None:
            cursor = iter(rows)
            if position:
                next(islice(cursor, position, position), None)  # skip what was fetched
            self._cursor = cursor
        elif cursor is not None:
            self._cursor = None  # only store frames reach here, and theirs is a FrameCursor

    @property
    def _is_lazy(self) -> bool:
        """True when rows are an unconsumed iterator rather than a materialised list."""
        return self._frame is None and not isinstance(self._py_rows, list)

    @property
    def _row_factory(self):
        row_factory = self._rf
        if row_factory is None:
            row_factory = self._rf = Row.create_class(self._schema)
        return row_factory

    def _get_plan(self):
        """The native validation plan, built on first append (v1 rebuilt it per frame)."""
        plan = self._plan
        if plan is None:
            schema = self._schema
            if isinstance(schema, RelationSchema):
                columns = [
                    (
                        str(col.name),
                        col.nullable,
                        None
                        if col.type == OrsoTypes._MISSING_TYPE
                        else ORSO_TO_PYTHON_MAP[col.type],
                        col.type == OrsoTypes.JSONB,
                    )
                    for col in schema.columns
                ]
                plan = Plan(self._row_factory, columns, schema.validate, json_dumps_bytes)
            else:
                plan = Plan(self._row_factory, None, None, None)
            self._plan = plan
        return plan

    # ------------------------------------------------------------------------------

    def group_by(self, columns: List[str]) -> GroupBy:
        return GroupBy(self, columns)

    @classmethod
    def from_arrow(cls, tables):
        """
        Convert a PyArrow Table, or list of Tables, to an Orso DataFrame.

        A Table, RecordBatch, or list of them is imported natively into a store frame:
        no Python objects are made until rows are read. A generator of tables stays a
        lazy frame, read one table at a time, as in v1.
        """
        from orso import converters

        eager = converters.eager_tables(tables)
        if eager:
            schema = converters.schema_from_arrow(eager[0])
            df = cls(rows=[], schema=schema)
            frame = Frame(df._get_plan())
            try:
                converters.import_tables(frame, eager)
            except ValueError:
                eager = None  # tables of different widths: they cannot share a store
            else:
                df._frame = frame
                df._py_rows = None
                df._cursor = frame.cursor()
                return df
        rows, schema = converters.from_arrow(tables)
        return cls(rows=rows, schema=schema)

    def arrow(self, size=None):
        """
        Convert an Orso DataFrame to a PyArrow Table, optionally limit the number
        of records.
        """
        from orso.converters import to_arrow

        return to_arrow(self, size=size)

    def pandas(self, size=None):
        from orso.converters import to_pandas

        return to_pandas(self, size)

    def polars(self, size=None):
        from orso.converters import to_polars

        return to_polars(self, size)

    def nbytes(self) -> int:
        """Approximate the number of bytes used by the DataFrame"""
        frame = self._frame
        if frame is not None:
            return frame.nbytes()
        self.materialize()
        rows = self._py_rows
        total_rows = len(rows)
        counted = self._nbytes_upto
        if counted != total_rows:
            if counted > total_rows:
                # rows were replaced with a shorter sequence - recount from scratch
                self._nbytes = rows_nbytes(rows)
            else:
                # only size the rows appended since the last call, so repeatedly
                # polling nbytes() while building a frame stays linear overall
                self._nbytes += rows_nbytes(rows, counted, total_rows)
            self._nbytes_upto = total_rows
        return self._nbytes

    def append(self, entry):
        frame = self._frame
        if frame is None:
            rows = self._py_rows
            if not isinstance(rows, list):
                self.materialize()
                rows = self._py_rows
            if rows:
                # the frame already holds Python rows: keep them that way, validated
                # natively when there is a RelationSchema
                if isinstance(self._schema, RelationSchema):
                    rows.append(self._get_plan().row(entry))
                else:
                    rows.append(self._row_factory(entry))
                self._cursor = None
                return
            # an empty frame: rows from here on go into the native record store. The
            # first record is written before the switch so a rejected record leaves the
            # frame exactly as it was.
            frame = Frame(self._get_plan())
            frame.append(entry)
            self._frame = frame
            self._py_rows = None
            self._decoded = None
            self._cursor = None
            return
        frame.append(entry)
        self._decoded = None
        # Appending invalidates the cursor (v1). Rows are not sized here; nbytes() picks
        # them up from the store when it is next called.
        self._cursor = None

    def head(self, size: int = 5) -> "DataFrame":
        return self.slice(0, size)

    def tail(self, size: int = 5) -> "DataFrame":
        return self.slice(offset=0 - size, length=size)

    def query(self, predicate) -> "DataFrame":
        """
        Apply a Selection operation to a Relation, this filters the data in the
        Relation to just the entries which match the predicate.

        Parameters:
            predicate (callable):
                A function which can be applied to a tuple to determine if it
                should be returned in the target Relation.

        Returns:
            DataFrame
        """
        return DataFrame(rows=[r for r in self if predicate(r)], schema=self._schema)

    def select(self, attributes) -> "DataFrame":
        """
        Create a new Orso DataFrame with a subset of columns of an existing DataFrame
        """
        if not isinstance(attributes, (list, tuple)):
            attributes = [attributes]
        attribute_indices = []
        new_header = attributes
        for index, attribute in enumerate(self._schema):
            if attribute in attributes:
                attribute_indices.append(index)

        def _inner_projection():
            for tup in self:
                yield tuple([tup[indice] for indice in attribute_indices])

        return DataFrame(rows=_inner_projection(), schema=new_header)

    def materialize(self):
        """
        Convert a Lazy DataFrame to an Eager DataFrame
        """
        # store frames are always materialised; list frames only when not a list already
        if self._frame is None and not isinstance(self._py_rows, list):
            self._py_rows = list(self._py_rows or [])

    def distinct(self) -> "DataFrame":
        seen = set()
        unique_rows = [
            x for x in self if hash(x) not in seen and not seen.add(hash(x))  # type:ignore
        ]
        return DataFrame(rows=unique_rows, schema=self._schema)

    def collect(
        self, columns: Union[int, str, List[Union[int, str]]], limit: int = None
    ) -> Union[List, Tuple]:
        """
        Collects specified columns from the internal row storage into a tuple of lists.

        Parameters:
            columns: Union[int, str, List[Union[int, str]]]
                The column(s) to collect. Could be an integer index, string name, or list thereof.
            limit: int (optional)
                The number of rows to return, defaults to all rows

        Returns:
            Union[list, tuple]:
                A tuple containing lists of the column data, or a single list if only one column is specified.
        """
        numpy = _numpy()

        self.materialize()

        if limit is None or limit < 0:
            limit = -1

        single = False
        if not isinstance(columns, (list, set, tuple)):
            single = True
            columns = [columns]
        else:
            columns = list(columns)

        column_indicies = columns
        for i, c in enumerate(columns):
            if not isinstance(c, int):
                column_indicies[i] = self.column_names.index(c)

        # the values are written straight into the object array's buffer, natively (v1
        # did the same through a Cython memoryview); store frames decode only these fields
        frame = self._frame
        if frame is not None:
            total = len(frame)
        else:
            rows = self._py_rows
            total = len(rows)
        n = total if limit < 0 else min(limit, total)
        collected = numpy.empty((len(column_indicies), n), dtype=object)
        if n and column_indicies:
            address = collected.__array_interface__["data"][0]
            if frame is not None:
                frame.collect_into(address, column_indicies, n)
            else:
                collect_rows_into(address, rows, column_indicies, n)
        if single:
            return collected[0]
        return collected

    def __getitem__(self, items):
        return self.collect(columns=items, limit=None)

    def slice(self, offset: int = 0, length: int = None) -> "DataFrame":
        frame = self._frame
        if frame is not None:
            n = len(frame)
            if offset < 0:
                offset = n + offset
            if length == 0:
                return DataFrame._from_frame(frame.view(0, 0), self)
            # the same window list slicing gives, negative bounds included
            window = range(n)[offset:] if length is None else range(n)[offset : offset + length]
            return DataFrame._from_frame(frame.view(window.start, len(window)), self)
        self.materialize()
        rows = self._py_rows
        if offset < 0:
            offset = len(rows) + offset
        if length is None:
            return DataFrame(schema=self._schema, rows=rows[offset:])
        if length == 0:
            return DataFrame(schema=self._schema, rows=[])
        return DataFrame(schema=self._schema, rows=rows[offset : offset + length])

    def filter(self, mask) -> "DataFrame":
        """
        Select rows from the DataFRame. The DataFrame is filtered based on boolean array.
        """
        return DataFrame(schema=self._schema, rows=(t for t, m in zip(self, mask) if m))

    def take(self, indexes) -> "DataFrame":
        """
        Select rows from the DataFrame. Rows are selected based on their appearance in the indexes
        list
        """
        return DataFrame(schema=self._schema, rows=(m for i, m in enumerate(self) if i in indexes))

    def row(self, i) -> Row:
        frame = self._frame
        if frame is not None:
            decoded = self._decoded
            return decoded[i] if decoded is not None else frame.row(i)
        self.materialize()
        return self._py_rows[i]

    # ------------------------------------------------------------------------------
    # DB-API cursor. List frames keep v1's iterator; store frames have a FrameCursor,
    # which iterates the same way, so fetchone() is `next(cursor)` for both.
    # ------------------------------------------------------------------------------

    def fetchone(self) -> Row:
        if self._cursor is None:
            raise Exception("Cannot use fetchone and append on the same DataFrame")
        try:
            return next(self._cursor)
        except StopIteration:
            return None

    def fetchmany(self, size=None) -> List[Row]:
        cursor = self._cursor
        if cursor is None:
            raise Exception("Cannot use fetchmany and append on the same DataFrame")
        fetch_size = self.arraysize if size is None else size
        if type(cursor) is FrameCursor:
            return cursor.take(fetch_size)
        entries = []
        for i in range(fetch_size):
            try:
                entry = next(cursor)
                entries.append(entry)
            except StopIteration:
                break
        return entries

    def fetchall(self) -> List[Row]:
        cursor = self._cursor
        if cursor is None:
            raise Exception("Cannot use fetchall and append on the same DataFrame")
        rows = self._py_rows

        # Fast-path for materialized rows by slicing instead of iterating element-by-element.
        if isinstance(rows, list) and isinstance(cursor, self._LIST_ITERATOR_TYPE):
            remaining = cursor.__length_hint__()
            total = len(rows)
            consumed = total - remaining
            if consumed <= 0:
                result = rows.copy()
            elif consumed >= total:
                result = []
            else:
                result = rows[consumed:].copy()
            self._cursor = iter(())
            return result

        if isinstance(rows, tuple) and isinstance(cursor, self._TUPLE_ITERATOR_TYPE):
            remaining = cursor.__length_hint__()
            total = len(rows)
            consumed = total - remaining
            if consumed <= 0:
                result = list(rows)
            elif consumed >= total:
                result = []
            else:
                result = list(rows[consumed:])
            self._cursor = iter(())
            return result

        if type(cursor) is FrameCursor:
            return cursor.rest()

        result = list(cursor)
        self._cursor = iter(())
        return result

    def display(
        self,
        limit: int = 5,
        display_width: Union[bool, int] = True,
        max_column_width: int = 32,
        colorize: bool = True,
        show_types: bool = True,
    ) -> str:
        from .display import ascii_table

        table_output, displayed_row_count = ascii_table(
            self,
            limit=limit,
            display_width=display_width,
            max_column_width=max_column_width,
            colorize=colorize,
            show_types=show_types,
            return_row_count=True,
        )
        return table_output + f"\n[ {displayed_row_count} rows x {self.columncount} columns ]"

    def markdown(self, limit: int = 5, max_column_width: int = 30) -> str:
        from .display import markdown

        return "\n".join(markdown(self, limit=limit, max_column_width=max_column_width))

    def to_batches(self, batch_size: int = 1000) -> typing.Generator["DataFrame", None, None]:
        """
        Batch a DataFrame into batches of `size` records.

        Args:
            size (int): The size of each batch.

        Yields:
            DataFrames
        """
        frame = self._frame
        if frame is not None:
            for i in range(0, len(frame), batch_size):
                yield DataFrame._from_frame(frame.view(i, batch_size), self)
            return
        self.materialize()
        rows = self._py_rows
        for i in range(0, len(rows), batch_size):
            yield DataFrame(rows=rows[i : i + batch_size], schema=self._schema)

    @property
    def description(
        self,
    ) -> List[
        Tuple[
            str,
            Optional[str],
            None,
            None,
            Optional[int],
            Optional[int],
            Optional[bool],
        ]
    ]:
        """
        name
        type_code
        display_size
        internal_size
        precision
        scale
        null_ok
        """
        result = []
        for column in self.column_names:
            data_type = None
            data_precision = None
            data_scale = None
            nullable = None
            if isinstance(self._schema, RelationSchema):
                column_data = self._schema.find_column(column)
                column_type = column_data.type
                if column_type is not None:
                    data_type = str(column_data.type.value)
                if column_type.value == "DECIMAL":
                    data_precision = column_data.precision
                    data_scale = column_data.scale
                    data_type = f"DECIMAL({data_precision},{data_scale})"
                if column_type.value == "ARRAY" and column_data.element_type is not None:
                    data_type = f"ARRAY<{column_data.element_type.value}>"
                nullable = column_data.nullable
            result.append(
                (
                    column,
                    data_type,
                    None,
                    None,
                    data_precision,
                    data_scale,
                    nullable,
                )
            )
        return result  # type:ignore

    @property
    def column_names(self) -> Tuple[str]:
        names = self._colnames
        if names is None:
            schema = self._schema
            if isinstance(schema, (tuple, list)):
                names = tuple(str(c) for c in schema)  # type:ignore
            else:
                names = tuple(str(col.name) for col in schema.columns)
            self._colnames = names
        return names

    @property
    def columncount(self) -> int:
        schema = self._schema
        if isinstance(schema, (tuple, list)):
            return len(schema)
        return len(schema.columns)

    @property
    def shape(self) -> typing.Tuple[int, int]:
        return (self.rowcount, self.columncount)  # type: ignore

    @property
    def rowcount(self) -> int:
        frame = self._frame
        if frame is not None:
            return len(frame)
        self.materialize()
        return len(self._py_rows)

    @property
    def schema(self) -> RelationSchema:
        return self._schema

    def __hash__(self):
        _hash = 0
        for i, row in enumerate(self):
            row_hash = _xxh64(str(row).encode())
            _hash = i ^ _hash ^ row_hash
        return _hash

    def __iter__(self):
        frame = self._frame
        if frame is not None:
            decoded = self._decoded
            if decoded is None:
                decoded = self._decoded = frame.rows()
            return decoded.__iter__()
        rows = self._py_rows
        if isinstance(rows, list):
            return rows.__iter__()
        return iter(rows)

    def __len__(self) -> int:
        frame = self._frame
        if frame is not None:
            return len(frame)
        self.materialize()
        return len(self._py_rows)

    def __repr__(self) -> str:
        """
        If we're in a notebook, return a table, otherwise return a tag
        """
        try:
            from IPython import get_ipython

            i_am_in_a_notebook = get_ipython() is not None
        except ImportError:
            return f"<orso.dataframe>"
        return str(self)

    def __str__(self) -> str:
        size: int = 10
        try:
            from IPython import get_ipython

            i_am_in_a_notebook = get_ipython() is not None
        except ImportError:
            i_am_in_a_notebook = False

        if i_am_in_a_notebook:
            from IPython.display import HTML
            from IPython.display import display

            from .display import html_table

            self.materialize()
            html = html_table(self, size)
            display(HTML(html))
            return ""
        else:
            from .display import ascii_table

            table_output, displayed_row_count = ascii_table(
                self, limit=size, top_and_tail=True, return_row_count=True
            )
            return table_output + f"\n[ {displayed_row_count} rows x {self.columncount} columns ]"

    def __add__(self, the_other):
        if self._schema != the_other._schema:
            raise ValueError("Schemas must be identical to add DataFrames")
        return DataFrame(rows=self._rows + the_other._rows, schema=self._schema)
