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
# Our record when converted to bytes looks like this
#
#       ┌───────────┬─────────────┬──────────────┐
#       │   flags   │   row_size  │  row values  │
#       └───────────┴─────────────┴──────────────┘
#
# 'flags' currently has a a version string and a single flag of deleted.
#
# Row object looks and acts like a Tuple where possible, but has additional features
# such as as_dict() to render as a dictionary.

import typing

import orjson
from ormsgpack import packb

from orso.exceptions import DataError
from orso.row_cython import from_bytes_cython
from orso.schema import RelationSchema

HEADER_SIZE: int = 6
HEADER_PREFIX: bytes = b"\x10\x00"
MAXIMUM_RECORD_SIZE: int = 8 * 1024 * 1024


def extract_columns(table, columns):
    # Pre-allocate lists for each column with known sizes
    n_rows = len(table)
    result = [None] * len(columns)
    for i in range(len(columns)):
        result[i] = [None] * n_rows

    # Fill in the lists with actual values
    for i, row in enumerate(table):
        for j, column in enumerate(columns):
            result[j][i] = row[column]

    return tuple(result)


class Row(tuple):
    __slots__ = ()
    _fields: tuple = None

    def __new__(cls, data):
        if isinstance(data, dict):
            data = data.values()
        return super().__new__(cls, data)

    @property
    def as_dict(self):
        return dict(zip(self._fields, self))

    @property
    def values(self):
        return tuple(self)

    def keys(self):
        return self._fields

    def __repr__(self):
        return f"Row{super().__repr__()}"

    def __str__(self):
        return str(self.as_dict)

    @classmethod
    def from_bytes(cls, data: bytes) -> "Row":
        return cls(from_bytes_cython(data))

    def to_bytes(self) -> bytes:
        # initial attempts to cythonize this function were slower
        record_bytes = packb(tuple(self))
        record_size = len(record_bytes)

        if record_size > MAXIMUM_RECORD_SIZE:
            raise DataError("Record length cannot exceed 8Mb")

        return HEADER_PREFIX + record_size.to_bytes(4, "big") + record_bytes

    def to_json(self) -> bytes:
        return orjson.dumps(self.as_dict, default=str)

    @classmethod
    def create_class(cls, schema: typing.Union[RelationSchema, tuple, list, set]):
        if isinstance(schema, RelationSchema):
            fields = tuple(c.name for c in schema.columns)
        elif isinstance(schema, (list, tuple)):
            fields = tuple(schema)
        else:
            raise ValueError("Row requires either a list of field names of a RelationSchema")
        return type("RowFactory", (Row,), {"_fields": fields})
