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

import typing
from typing import Generator
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import numpy

from orso import Row
from orso.group_by import GroupBy
from orso.schema import RelationSchema
from orso.tools import single_item_cache


class DataFrame:
    """
    Orso DataFrames are a lightweight container for tabular data.
    """

    __slots__ = ("_schema", "_rows", "_cursor", "_row_factory", "arraysize", "_nbytes")

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
        self._nbytes = None
        if dictionaries is not None:
            if schema is not None or rows is not None:
                raise ValueError(
                    "Can't implicitly and explicitly define a DataFrame at the same time"
                )

            from itertools import chain

            # make the list of dicts iterable
            dicts = iter(dictionaries)
            # extract the first of the list, and get the types from it
            first_dict = next(dicts)

            # if we have an explicit schema, use that, otherwise guess from the first entry
            self._schema = [str(k) for k in first_dict]
            self._row_factory = Row.create_class(self._schema)
            keys = list(first_dict.keys())

            # create a list of tuples
            self._rows = [
                self._row_factory([row.get(k, None) for k in keys])
                for row in chain([first_dict], dicts)
            ]
        else:
            self._schema = schema  # type:ignore
            self._rows = rows or []  # type:ignore
            self._row_factory = Row.create_class(self._schema)
            self._nbytes = 0
        self.arraysize = 100
        self._cursor = iter(self._rows or [])

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    def group_by(self, columns: List[str]) -> GroupBy:
        return GroupBy(self, columns)

    @classmethod
    def from_arrow(cls, tables):
        """Convert a PyArrow Table, or list of Tables, to an Orso DataFrame."""
        from orso.converters import from_arrow

        rows, schema = from_arrow(tables)
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
        self.materialize()
        if self._nbytes is None:
            self._nbytes = sum(row.nbytes() for row in self._rows)
        return self._nbytes

    def append(self, entry):
        if isinstance(self._schema, RelationSchema):
            self._schema.validate(entry)
        new_row = self._row_factory(entry)
        self._rows.append(new_row)
        self._nbytes += new_row.nbytes()
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
        return DataFrame(rows=[r for r in self._rows if predicate(r)], schema=self._schema)

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
            for tup in self._rows:
                yield tuple([tup[indice] for indice in attribute_indices])

        return DataFrame(rows=_inner_projection(), schema=new_header)

    def materialize(self):
        """
        Convert a Lazy DataFrame to an Eager DataFrame
        """
        if not isinstance(self._rows, list):
            self._rows = list(self._rows or [])

    def distinct(self) -> "DataFrame":
        seen = set()
        unique_rows = [
            x
            for x in self._rows
            if hash(x) not in seen and not seen.add(hash(x))  # type:ignore
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
        from orso.compute.compiled import collect_cython

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

        collected = collect_cython(
            self._rows, numpy.array(column_indicies, dtype=numpy.int32), limit
        )
        if single:
            return collected[0]
        return collected

    def __getitem__(self, items):
        return self.collect(columns=items, limit=None)

    def slice(self, offset: int = 0, length: int = None) -> "DataFrame":
        self.materialize()
        if offset < 0:
            offset = len(self._rows) + offset
        if length is None:
            return DataFrame(schema=self._schema, rows=self._rows[offset:])
        if length == 0:
            return DataFrame(schema=self._schema, rows=[])
        return DataFrame(schema=self._schema, rows=self._rows[offset : offset + length])

    def filter(self, mask) -> "DataFrame":
        """
        Select rows from the DataFRame. The DataFrame is filtered based on boolean array.
        """
        return DataFrame(schema=self._schema, rows=(t for t, m in zip(self._rows, mask) if m))

    def take(self, indexes) -> "DataFrame":
        """
        Select rows from the DataFrame. Rows are selected based on their appearance in the indexes
        list
        """
        return DataFrame(
            schema=self._schema, rows=(m for i, m in enumerate(self._rows) if i in indexes)
        )

    def row(self, i) -> Row:
        self.materialize()
        return self._rows[i]

    def fetchone(self) -> Row:
        if self._cursor is None:
            raise Exception("Cannot use fetchone and append on the same DataFrame")
        try:
            return next(self._cursor)
        except StopIteration:
            return None

    def fetchmany(self, size=None) -> List[Row]:
        if self._cursor is None:
            raise Exception("Cannot use fetchmany and append on the same DataFrame")
        fetch_size = self.arraysize if size is None else size
        entries = []
        for i in range(fetch_size):
            try:
                entry = next(self._cursor)
                entries.append(entry)
            except StopIteration:
                break
        return entries

    def fetchall(self) -> List[Row]:
        if self._cursor is None:
            raise Exception("Cannot use fetchall and append on the same DataFrame")
        return list(self._cursor)

    def display(
        self,
        limit: int = 5,
        display_width: Union[bool, int] = True,
        max_column_width: int = 32,
        colorize: bool = True,
        show_types: bool = True,
    ) -> str:
        from .display import ascii_table

        return ascii_table(
            self,
            limit=limit,
            display_width=display_width,
            max_column_width=max_column_width,
            colorize=colorize,
            show_types=show_types,
        )

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
        self.materialize()
        for i in range(0, self.rowcount, batch_size):
            yield DataFrame(rows=self._rows[i : i + batch_size], schema=self._schema)

    @property
    def profile(self) -> "TableProfile":
        from orso.profiler import TableProfile

        return TableProfile.from_dataframe(self)

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
    @single_item_cache
    def column_names(self) -> Tuple[str]:
        if isinstance(self._schema, (tuple, list)):
            return tuple(str(c) for c in self._schema)  # type:ignore
        return tuple(str(col.name) for col in self._schema.columns)

    @property
    @single_item_cache
    def columncount(self) -> int:
        if isinstance(self._schema, (tuple, list)):
            return len(self._schema)
        return len(self._schema.columns)

    @property
    def shape(self) -> typing.Tuple[int, int]:
        return (self.rowcount, self.columncount)  # type: ignore

    @property
    def rowcount(self) -> int:
        self.materialize()
        return len(self._rows)

    @property
    def schema(self) -> RelationSchema:
        return self._schema

    def __hash__(self):
        from xxhash import xxh64

        _hash = 0
        for i, row in enumerate(self._rows):
            row_hash = xxh64(str(row).encode()).intdigest()
            _hash = i ^ _hash ^ row_hash
        return _hash

    def __iter__(self):
        return iter(self._rows)

    def __len__(self) -> int:
        self.materialize()
        return len(self._rows)

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

            return (
                ascii_table(self, limit=size, top_and_tail=True)
                + f"\n[ {self.rowcount} rows x {self.columncount} columns ]"
            )

    def __add__(self, the_other):
        if self._schema != the_other._schema:
            raise ValueError("Schemas must be identical to add DataFrames")
        return DataFrame(rows=self._rows + the_other._rows, schema=self._schema)
