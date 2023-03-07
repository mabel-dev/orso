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

from orso.exceptions import MissingDependencyError
from orso.row import Row


class DataFrame:
    __slots__ = ("_schema", "_rows")

    def __init__(self, schema, rows):
        self._schema = schema
        self._rows = rows

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    @classmethod
    def from_arrow(cls, table) -> tuple:
        schema = table.schema
        fields = {
            str(field.name): {"type": field.type.to_pandas_dtype(), "nullable": field.nullable}
            for field in schema
        }
        columns = [table.column(i) for i in schema.names]

        # Create a list of tuples from the columns
        row_factory = Row.create_class(fields)
        rows = (row_factory(col[i].as_py() for col in columns) for i in range(table.num_rows))

        return cls(fields, rows)

    def to_arrow(self):
        try:
            import pyarrow
        except ImportError as import_error:
            raise MissingDependencyError(import_error.name) from import_error
        # Create a list of PyArrow arrays from the rows
        arrays = [pyarrow.array(col) for col in zip(*self._rows)]

        # Create a PyArrow table from the arrays and schema
        table = pyarrow.Table.from_arrays(arrays, self.column_names)

        return table

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
        new_header = {k: {"type": v.get("type")} for k, v in self._schema.items()}
        return DataFrame(filter(predicate, self._rows), new_header)

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

        return DataFrame(_inner_projection(), new_header)

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

        return DataFrame(do_dedupe(self._rows), self._schema)

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
            return DataFrame(self._schema, self._rows[offset:])
        return DataFrame(self._schema, self._rows[offset : offset + length])

    def row(self, i):
        self.materialize()
        return self._rows[i]

    @property
    def column_names(self):
        return tuple(self._schema.keys())

    @property
    def num_columns(self):
        return len(self._schema.keys())

    @property
    def num_rows(self):
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
