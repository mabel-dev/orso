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

import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

import numpy
import orjson
from ormsgpack import OPT_SERIALIZE_NUMPY
from ormsgpack import packb

from orso.compiled import from_bytes_cython
from orso.exceptions import DataError
from orso.schema import RelationSchema

HEADER_SIZE: int = 6
HEADER_PREFIX: bytes = b"\x10\x00"
MAXIMUM_RECORD_SIZE: int = 8 * 1024 * 1024


def extract_columns(table: List[Dict[str, Any]], columns: List[str]) -> Tuple[List[Any], ...]:
    """
    Extracts specified columns from a table.

    Parameters:
        table: List of dictionaries representing rows in a table.
        columns: List of column names to extract.
    Returns:
        A tuple of lists, each containing values of a particular column.
    """
    n_rows = len(table)
    result = [None] * len(columns)
    for i in range(len(columns)):
        result[i] = [None] * n_rows  # type:ignore

    for i, row in enumerate(table):
        for j, column in enumerate(columns):
            result[j][i] = row[column]  # type:ignore

    return tuple(result)


class Row(tuple):
    __slots__ = ()
    _fields: Tuple[str, ...] = None
    _cached_map: Tuple[Tuple[str, Any]] = None

    def __new__(cls, data: Union[Dict[str, Any], Tuple[Any, ...]]):
        """
        Creates a new Row instance.

        Parameters:
            data: Either a dictionary or a tuple.
        Returns:
            A new Row instance.
        """
        if isinstance(data, dict):
            data = data.values()  # type:ignore
        instance = super().__new__(cls, data)  # type:ignore
        return instance

    @property
    def as_map(self) -> Tuple[Tuple[str, Any], ...]:
        """
        Returns the Row as a tuple of key-value pair tuples (a 'map').

        Returns:
            A tuple of key-value pair tuples.
        """
        if self._cached_map is None:
            self._cached_map = tuple(zip(self._fields, self))  # type:ignore
        return self._cached_map

    @property
    def as_dict(self) -> Dict[str, Any]:
        """
        Returns the Row as a dictionary.

        Returns:
            A dictionary representation of the Row.
        """
        return dict(self.as_map)

    @property
    def values(self) -> Tuple[Any, ...]:
        return tuple(self)

    def keys(self) -> Tuple[str, ...]:
        return self._fields

    @classmethod
    def from_bytes(cls, data: bytes) -> "Row":
        """
        Creates a Row instance from bytes.

        Parameters:
            data: The byte representation of a Row.
        Returns:
            A new Row instance.
        """
        return cls(from_bytes_cython(data))

    @property
    def as_bytes(self) -> bytes:
        """
        Converts the Row to bytes.

        Returns:
            The byte representation of the Row.
        """

        def serialize(value):
            if isinstance(value, numpy.datetime64):
                if numpy.isnat(value):
                    return None
                return ("__datetime__", value.astype("datetime64[s]").astype("int"))
            if isinstance(value, datetime.datetime):
                return ("__datetime__", value.timestamp())
            if isinstance(value, numpy.ndarray):
                return list(value)
            return str(value)

        record_bytes = packb(tuple(self), option=OPT_SERIALIZE_NUMPY, default=serialize)
        record_size = len(record_bytes)
        if record_size > MAXIMUM_RECORD_SIZE:
            raise DataError("Record length cannot exceed 8Mb")

        return HEADER_PREFIX + record_size.to_bytes(4, "big") + record_bytes

    @property
    def as_json(self) -> bytes:
        """
        Converts the Row to JSON bytes.

        Returns:
            The JSON byte representation of the Row.
        """
        return orjson.dumps(self.as_dict, default=str)

    @classmethod
    def create_class(cls, schema: Union[RelationSchema, Tuple[str, ...], List[str]]) -> type:
        """
        Creates a subclass of Row based on the schema provided.

        Parameters:
            schema: Either a RelationSchema or a list/tuple of field names.
        Returns:
            A new Row subclass.
        """
        if isinstance(schema, RelationSchema):
            fields = tuple(c.name for c in schema.columns)
        elif isinstance(schema, (list, tuple)):
            fields = tuple(schema)
        else:
            raise ValueError("Row requires either a list of field names or a RelationSchema")

        return type("RowFactory", (Row,), {"_fields": fields})
