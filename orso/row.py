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
import time
from functools import cached_property
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

import numpy
import orjson
from ormsgpack import OPT_SERIALIZE_NUMPY
from ormsgpack import packb

from orso.compute.compiled import extract_dict_columns
from orso.compute.compiled import from_bytes_cython
from orso.exceptions import DataError
from orso.schema import RelationSchema

HEADER_SIZE: int = 14
HEADER_PREFIX: bytes = b"\x10\x00"
MAXIMUM_RECORD_SIZE: int = 16 * 1024 * 1024


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
    _cached_byte_size: int = None
    _key: Tuple[int, int] = None

    def __new__(cls, data: Union[Dict[str, Any], Tuple[Any, ...]]):
        """
        Creates a new Row instance.

        Parameters:
            data: Either a dictionary or a tuple.
        Returns:
            A new Row instance.
        """
        if isinstance(data, dict):
            # data = tuple([data.get(field) for field in cls._fields])
            # previous comments on the below line suggested it had a bug, but didn't
            # say what the bug was - this is about 25% faster than the pure Python version
            # There is a lot of testing on this function and it hasn't found any bugs.
            data = extract_dict_columns(data, cls._fields)  # type:ignore
        instance = super().__new__(cls, data)  # type:ignore
        return instance

    def get(self, item, default=None):
        index = self._fields.index(item)
        if index == -1:
            return default
        return self[index]

    @cached_property
    def as_map(self) -> Tuple[Tuple[str, Any], ...]:
        """
        Returns the Row as a tuple of key-value pair tuples (a 'map').

        Returns:
            A tuple of key-value pair tuples.
        """
        return tuple(zip(self._fields, self))

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

    def nbytes(self) -> int:
        if self._cached_byte_size is None:
            self._cached_byte_size = len(self.as_bytes)
        return self._cached_byte_size

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
                return value.tolist()
            return str(value)

        record_bytes = packb(tuple(self), option=OPT_SERIALIZE_NUMPY, default=serialize)
        record_size = len(record_bytes)
        timestamp = time.time_ns()

        if record_size > MAXIMUM_RECORD_SIZE:
            raise DataError("Record length cannot exceed 16Mb")

        return (
            HEADER_PREFIX
            + record_size.to_bytes(4, "big")
            + timestamp.to_bytes(8, "big")
            + record_bytes
        )

    @property
    def as_json(self) -> bytes:
        """
        Converts the Row to JSON bytes.

        Returns:
            The JSON byte representation of the Row.
        """
        return orjson.dumps(self.as_dict, default=str)

    @classmethod
    def create_class(
        cls, schema: Union[RelationSchema, Tuple[str, ...], List[str]], tuples_only: bool = False
    ) -> type:
        """
        Creates a subclass of Row based on the schema provided.

        Parameters:
            schema: Either a RelationSchema or a list/tuple of field names.
        Returns:
            A new Row subclass.
        """
        if isinstance(schema, RelationSchema):
            fields = tuple(c.name for c in schema.columns)
        elif not isinstance(schema, (list, tuple)):
            raise ValueError("Row requires either a list of field names or a RelationSchema")

        fields = tuple(str(s) for s in schema)
        if tuples_only:
            # if we're only handling tuples, we can delegate to super
            return type("RowFactory", (Row,), {"_fields": fields, "__new__": super().__new__})
        return type("RowFactory", (Row,), {"_fields": fields})
