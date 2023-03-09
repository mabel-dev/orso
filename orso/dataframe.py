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
import itertools
import typing

from orso.exceptions import MissingDependencyError
from orso.row import Row


class DataFrame:
    __slots__ = ("_schema", "_rows", "_cursor")

    def __init__(self, dictionaries=None, *, rows: typing.List[tuple] = None, schema: dict = None):
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
            self._schema = {name: {"type": type(value)} for name, value in first_dict.items()}

            # create a list of tuples
            self._rows = (
                tuple([row.get(k) for k in first_dict.keys()]) for row in chain([first_dict], dicts)
            )
        else:
            self._schema = schema
            self._rows = rows
        self._cursor = 0

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    @classmethod
    def from_arrow(cls, tables) -> typing.Any:
        try:
            import pyarrow.lib as lib
        except ImportError as import_error:
            raise MissingDependencyError(import_error.name) from import_error

        def _type_convert(field_type):
            if field_type.id == lib.Type_BOOL:
                return bool
            if field_type.id == lib.Type_STRING:
                return str
            if field_type.id in {
                lib.Type_INT8,
                lib.Type_INT16,
                lib.Type_INT32,
                lib.Type_INT64,
                lib.Type_UINT8,
                lib.Type_UINT16,
                lib.Type_UINT32,
                lib.Type_UINT64,
            }:
                return int
            if field_type.id in {lib.Type_HALF_FLOAT, lib.Type_FLOAT, lib.Type_DOUBLE}:
                return float

        def _peek(iterable):
            iter1, iter2 = itertools.tee(iterable)
            try:
                first = next(iter1)
            except StopIteration:
                return None, iter([])
            else:
                return first, iter2

        if not isinstance(tables, (typing.Generator, list, tuple)):
            tables = [tables]

        if isinstance(tables, (list, tuple)):
            tables = iter(tables)

        first_table, all_tables = _peek(tables)
        schema = first_table.schema
        fields = {
            str(field.name): {"type": _type_convert(field.type), "nullable": field.nullable}
            for field in schema
        }

        # Create a list of tuples from the columns
        row_factory = Row.create_class(fields)
        rows = (
            (row_factory(col[i].as_py() for col in [table.column(j) for j in schema.names]))
            for table in all_tables
            for i in range(table.num_rows)
        )

        return cls(rows=rows, schema=fields)

    def arrow(self):
        try:
            import pyarrow
        except ImportError as import_error:
            raise MissingDependencyError(import_error.name) from import_error
        # Create a list of PyArrow arrays from the rows
        arrays = [pyarrow.array(col) for col in zip(*self._rows)]

        # Create a PyArrow table from the arrays and schema
        table = pyarrow.Table.from_arrays(arrays, self.column_names)

        return table

    def pandas(self):
        raise NotImplementedError()

    def polars(self):
        raise NotImplementedError()

    def head(self, size: int):
        raise NotImplementedError()

    def tail(self, size: int):
        raise NotImplementedError()

    def query(self, predicate):
        """
        Apply a Selection operation to a Relation, this filters the data in the
        Relation to just the entries which match the predicate.

        Parameters:
            predicate (callable):
                A function which can be applied to a tuple to determine if it
                should be returned in the target Relation.

        Returns:

        """
        # selection invalidates what we thought we knew about counts etc
        new_schema = {k: {"type": v.get("type")} for k, v in self._schema.items()}
        return DataFrame(rows=filter(predicate, self._rows), schema=new_schema)

    def select(self, attributes):
        if not isinstance(attributes, (list, tuple)):
            attributes = [attributes]
        attribute_indices = []
        new_header = {k: v for k, v in self._schema.items() if k in attributes}
        for index, attribute in enumerate(self._schema.keys()):
            if attribute in attributes:
                attribute_indices.append(index)

        def _inner_projection():
            for tup in self._rows:
                yield tuple([tup[indice] for indice in attribute_indices])

        return DataFrame(rows=_inner_projection(), schema=new_header)

    def materialize(self):
        if not isinstance(self._rows, list):
            self._rows = list(self._rows)

    def distinct(self):
        hash_list = {}

        def do_dedupe(data):
            for item in data:
                hashed_item = hash(item)
                if hashed_item not in hash_list:
                    yield item
                    hash_list[hashed_item] = True

        return DataFrame(rows=do_dedupe(self._rows), schema=self._schema)

    def collect(self, columns):
        single = False
        if not isinstance(columns, typing.Iterable) or isinstance(columns, str):
            single = True
            columns = [columns]

        # get the index of a column name
        columns = [c if isinstance(c, int) else self.column_names.index(c) for c in columns]

        # Initialize empty lists for each column
        result = tuple([] for _ in columns)

        # Extract the specified columns into the result lists
        for row in self._rows:
            for j, column in enumerate(columns):
                result[j].append(row[column])

        if single:
            return result[0]
        return result

    def slice(self, offset: int = 0, length: int = None):
        self.materialize()
        if offset < 0:
            offset = len(self._rows) + offset
        if length is None:
            return DataFrame(schema=self._schema, rows=self._rows[offset:])
        return DataFrame(schema=self._schema, rows=self._rows[offset : offset + length])

    def row(self, i):
        self.materialize()
        return self._rows[i]

    def fetchone(self):
        row = self.row(self._cursor)
        self._cursor += 1
        return row

    def fetchmany(self, size=None):
        fetch_size = self.arraysize if size is None else size
        rows = self.slice(offset=self._cursor, length=fetch_size)
        self._cursor += fetch_size
        return rows

    def fetchall(self):
        self._cursor = 0
        return list(self)

    @property
    def column_names(self):
        return tuple(self._schema.keys())

    @property
    def columncount(self):
        return len(self._schema.keys())

    @property
    def shape(self):
        return (self.rowcount, self.columncount)

    @property
    def rowcount(self):
        self.materialize()
        return len(self._rows)

    def __iter__(self):
        return iter(self._rows)

    def __len__(self) -> int:
        self.materialize()
        return len(self._rows)

    def __str__(self) -> str:
        from .display import ascii_table

        return ascii_table(self)
