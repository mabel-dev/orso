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
        self._cursor = iter(self._rows)

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    @classmethod
    def from_arrow(cls, tables):
        from orso.converters import from_arrow

        return from_arrow(tables)

    def arrow(self, size=None):
        from orso.converters import to_arrow

        return to_arrow(self, size=size)

    def pandas(self):
        from orso.converters import to_pandas

        return to_pandas(self)

    def polars(self):
        from orso.converters import to_polars

        return to_polars(self)

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

    def filter(self, mask):
        """
        Select rows from the DataFRame. The DataFrame is filtered based on boolean array.
        """
        return DataFrame(schema=self._schema, rows=(t for t, m in zip(self._rows, mask) if m))

    def take(self, indexes):
        """
        Select rows from the DataFrame. Rows are selected based on their appearance in the indexes
        list
        """
        return DataFrame(
            schema=self._schema, rows=(m for i, m in enumerate(self._rows) if i in indexes)
        )

    def row(self, i):
        self.materialize()
        return self._rows[i]

    def fetchone(self):
        try:
            return next(self._cursor)
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        fetch_size = self.arraysize if size is None else size
        entries = []
        for i in range(fetch_size):
            try:
                entry = next(self._cursor)
                entries.append(entry)
            except StopIteration:
                break
        return entries

    def fetchall(self):
        return list(self._rows)

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

    def __hash__(self):
        from cityhash import CityHash32

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

        return ascii_table(self)
