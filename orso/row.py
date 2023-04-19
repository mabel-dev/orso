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
# Our record when saved looks like this
#
#   ┌───────┬───────────┬────────────┐
#   │ flags | row_size  │ row values │
#   └───────┴───────────┴────────────┘
#
# flags currently has a single flag of deleted.
#
# Row object looks and acts like a Tuple where possible, but has additional features
# such as as_dict() to render as a dictionary.

from ormsgpack import packb
from ormsgpack import unpackb


def extract_columns(table, columns):
    # Initialize empty lists for each column
    result: tuple = tuple([] for _ in columns)

    # Extract the specified columns into the result lists
    for row in table:
        for j, column in enumerate(columns):
            result[j].append(row[column])

    return result


class Row(tuple):
    __slots__ = ()
    _fields: list = []

    def __new__(cls, data):
        return super().__new__(cls, data)

    @property
    def as_dict(self):
        return {k: v for k, v in zip(self._fields, self)}

    @property
    def values(self):
        return tuple(self)

    def keys(self):
        return self._fields

    def __repr__(self):
        return f"Row{super().__repr__()}"

    def __str__(self):
        return str(self.as_dict)

    def __setattr__(self, name, value):
        raise AttributeError("can't set attribute")

    def __delattr__(self, name):
        raise AttributeError("can't delete attribute")

    @classmethod
    def from_bytes(cls, data: bytes) -> tuple:
        unpacked_values = unpackb(data)
        return cls(unpacked_values)

    def to_bytes(self) -> bytes:
        record_bytes = packb(tuple(self))
        record_size = len(record_bytes)
        return b"\x00" + record_size.to_bytes(4, "big") + record_bytes

    def to_json(self) -> bytes:
        import orjson

        return orjson.dumps(self.as_dict, default=str)

    @classmethod
    def create_class(cls, schema):
        row_class = type(
            "RowClass", (Row,), {"_fields": [field for field in schema], "_schema": schema}
        )
        return row_class
