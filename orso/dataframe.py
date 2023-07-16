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

from orso import Row
from orso.schema import RelationSchema

try:
    # added 3.9
    from functools import cache
except ImportError:
    from functools import lru_cache

    cache = lru_cache(1)


class DataFrame:
    """
    Orso DataFrames are a lightweight container for tabular data.
    """

    __slots__ = ("_schema", "_rows", "_cursor", "arraysize", "_row_factory")

    def __init__(
        self,
        dictionaries: typing.Optional[typing.Iterable[dict]] = None,
        *,
        rows: typing.Union[typing.List[tuple], typing.Generator[tuple, None, None], None] = None,
        schema: typing.Union[RelationSchema, typing.List[str], typing.Tuple[str], None] = None,
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
        if dictionaries is not None:
            if schema is not None or rows is not None:
                raise ValueError(
                    "Can't implicitly and explicitly define a DataFrame at the same time"
                )

            from itertools import chain

            # make the list of dicts iterable
            dicts = iter(dictionaries)

            # extract the first of the list, and get the types from it
            first_dict = {}
            first_dict = next(dicts)

            # if we have an explicit schema, use that, otherwise guess from the first entry
            self._schema = list(map(str, first_dict.keys()))  # type: ignore

            self._row_factory = Row.create_class(self._schema)
            # create a list of tuples
            self._rows: list = [  # type:ignore
                self._row_factory([row.get(k) for k in first_dict.keys()])
                for row in chain([first_dict], dicts)
            ]
        else:
            self._schema = schema  # type:ignore
            self._rows = rows or []  # type:ignore
            self._row_factory = Row.create_class(self._schema)
        self.arraysize = 100
        self._cursor = iter(self._rows or [])

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

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

    def nbytes(self):
        """Approximate the number of bytes used by the DataFrame"""
        self.materialize()
        size = 0
        for row in self._rows:
            size += len(row.to_bytes())
        return size

    def append(self, entry):
        if isinstance(self._schema, RelationSchema):
            self._schema.validate(entry)
        self._rows.append(self._row_factory(entry))
        self._cursor = None

    def head(self, size: int = 5):
        return self.slice(0, size)

    def tail(self, size: int = 5):
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
        return DataFrame(rows=(r for r in filter(predicate, self._rows)), schema=self._schema)

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
        hash_list = {}

        def do_dedupe(data):
            for item in data:
                hashed_item = hash(item)
                if hashed_item not in hash_list:
                    yield item
                    hash_list[hashed_item] = True

        return DataFrame(rows=do_dedupe(self._rows), schema=self._schema)

    def collect(self, columns) -> typing.Union[list, tuple]:
        single = False
        if not isinstance(columns, typing.Iterable) or isinstance(columns, str):
            single = True
            columns = [columns]

        # get the index of a column name
        columns = [c if isinstance(c, int) else self.column_names.index(c) for c in columns]

        # Initialize empty lists for each column
        result: tuple = tuple([] for _ in columns)

        # Extract the specified columns into the result lists
        for row in self._rows:
            for j, column in enumerate(columns):
                result[j].append(row[column])

        if single:
            return result[0]
        return result

    def slice(self, offset: int = 0, length: int = None) -> "DataFrame":
        self.materialize()
        if offset < 0:
            offset = len(self._rows) + offset
        if length is None:
            return DataFrame(schema=self._schema, rows=self._rows[offset:])
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

    def fetchmany(self, size=None) -> typing.List[Row]:
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

    def fetchall(self) -> typing.List[Row]:
        if self._cursor is None:
            raise Exception("Cannot use fetchall and append on the same DataFrame")
        return list(self._cursor)

    def display(
        self,
        limit: int = 5,
        display_width: typing.Union[bool, int] = True,
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
    def profile(self) -> "DataFrame":
        from orso.profiler import DataProfile

        return DataProfile.from_dataset(self)

    @property
    def description(
        self,
    ) -> typing.List[
        typing.Tuple[
            str,
            typing.Optional[str],
            None,
            None,
            typing.Optional[int],
            typing.Optional[int],
            typing.Optional[bool],
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
        from orso.types import PYTHON_TO_ORSO_MAP

        result = []
        for column in self.column_names:
            nullable = None
            data_type = None
            data_precision = None
            data_scale = None
            if isinstance(self._schema, RelationSchema):
                column_data = self._schema.find_column(column)
                column_type = column_data.type
                if column_type is not None:
                    data_type = str(column_data.type.value)
                if column_type.value == "DECIMAL":
                    data_precision = column_data.precision
                    data_scale = column_data.scale
                    data_type = f"DECIMAL({data_precision},{data_scale})"
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
        return result

    @property
    @cache
    def column_names(self) -> tuple:
        if isinstance(self._schema, (tuple, list)):
            return tuple(map(str, self._schema))
        return tuple([col.name for col in self._schema.columns])

    @property
    @cache
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

    def __hash__(self):
        from orso.cityhash import CityHash32

        _hash = 0
        for row in self._rows:
            row_hash = CityHash32(str(row).encode())
            _hash = _hash ^ row_hash
        return _hash

    def __iter__(self):
        return iter(self._rows)

    def __len__(self) -> int:
        self.materialize()
        return len(self._rows)

    def __str__(self) -> str:
        from .display import ascii_table

        return (
            ascii_table(self, show_types=True)
            + f"\n[ {self.rowcount} rows x {self.columncount} columns ]"
        )

    def __repr__(self) -> str:
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

            html = html_table([r.as_dict for i, r in enumerate(iter(self)) if i < size], size)
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
