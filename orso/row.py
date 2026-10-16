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
#
# v2: the per-row work (construction from a dict or a sequence, as_dict, as_map, values,
# get, keys, nbytes) lives in the C base type orso._native.RowBase. Row subclasses have
# no __dict__, so a row is exactly a tuple in memory.

import datetime
import json
import sys
import time
from threading import RLock
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union
from weakref import WeakValueDictionary

import numpy
from ormsgpack import OPT_SERIALIZE_NUMPY
from ormsgpack import packb

from orso._native import RowBase
from orso._native import set_value_sizer
from orso.compute.compiled import from_bytes_cython
from orso.exceptions import DataError
from orso.schema import RelationSchema

HEADER_SIZE: int = 14
HEADER_PREFIX: bytes = b"\x10\x00"
MAXIMUM_RECORD_SIZE: int = 16 * 1024 * 1024
# HEADER_PREFIX + 4 byte length + 8 byte timestamp
RECORD_HEADER_SIZE: int = len(HEADER_PREFIX) + 4 + 8

# Cache Row subclasses so we reuse the lightweight tuple wrappers across identical schemas.
_ROW_CLASS_CACHE: "WeakValueDictionary[Tuple[Tuple[str, ...], bool], type]" = WeakValueDictionary()
_ROW_CLASS_CACHE_LOCK = RLock()


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


def make_as_dict(fields):
    """Kept for compatibility: RowBase provides `as_dict` for every Row subclass."""

    def _as_dict(self):
        return {field: self[i] for i, field in enumerate(fields)}

    return property(_as_dict)


def _serialize(value):
    """ormsgpack `default` for values it cannot encode natively (v1 behaviour)."""
    if isinstance(value, numpy.datetime64):
        if numpy.isnat(value):
            return None
        return ("__datetime__", value.astype("datetime64[s]").astype("int"))
    if isinstance(value, datetime.datetime):
        return ("__datetime__", value.timestamp())
    if isinstance(value, numpy.ndarray):
        return value.tolist()
    return str(value)


def _value_size(value) -> int:
    """
    The msgpack size of one value as v1's serialiser writes it. The native size estimate
    (orso/native/width.h) handles the common types itself and calls this for the rest,
    so Row.nbytes() and DataFrame.nbytes() give v1's numbers without serialising rows.
    """
    return len(packb(value, option=OPT_SERIALIZE_NUMPY, default=_serialize))


set_value_sizer(_value_size)


class Row(RowBase):
    """
    A tuple with named fields. Use `Row.create_class(fields)` to get a Row type for a
    schema; instances are built from a dict (by field name, missing fields are None) or
    from any iterable (positionally).

    Provided by the native base: `as_dict`, `as_map`, `values` (properties), `get`,
    `keys`, `nbytes` (methods).
    """

    __slots__ = ()  # must be empty because we're inheriting from tuple
    _fields: Tuple[str, ...] = None
    _key: Tuple[int, int] = None

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

    def _packed(self) -> bytes:
        """msgpack payload for this Row, without the record header."""
        return packb(tuple(self), option=OPT_SERIALIZE_NUMPY, default=_serialize)

    @property
    def as_bytes(self) -> bytes:
        """
        Converts the Row to bytes.

        Returns:
            The byte representation of the Row.
        """
        record_bytes = self._packed()
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
        # `separators` keeps this byte-identical to the orjson output this method
        # produced before 0.0.233; stdlib json defaults to ", "/": " padding.
        return json.dumps(self.as_dict, default=str, separators=(",", ":")).encode()

    @classmethod
    def create_class(
        cls, schema: Union[RelationSchema, Tuple[str, ...], List[str]], tuples_only: bool = False
    ) -> type:
        """
        Creates a subclass of Row based on the schema provided.

        Parameters:
            schema: Either a RelationSchema or a list/tuple of field names.
            tuples_only: Retained for compatibility. v2 rows are built natively from a
                dict or a sequence either way; the flag only keeps a separate cache entry.
        Returns:
            A new Row subclass.
        """
        if isinstance(schema, RelationSchema):
            fields = tuple(c.name for c in schema.columns)
        elif not isinstance(schema, (list, tuple)):
            raise ValueError("Row requires either a list of field names or a RelationSchema")
        else:
            fields = tuple(str(s) for s in schema)
        fields = tuple(sys.intern(f) if type(f) is str else f for f in fields)

        cache_key = (fields, tuples_only)
        cached_factory = _ROW_CLASS_CACHE.get(cache_key)
        if cached_factory is not None:
            return cached_factory

        with _ROW_CLASS_CACHE_LOCK:
            cached_factory = _ROW_CLASS_CACHE.get(cache_key)
            if cached_factory is not None:
                return cached_factory

            # Field map for O(1) lookups in .get()
            field_map = {field: i for i, field in enumerate(fields)}

            row_factory = type(
                "RowFactory",
                (Row,),
                {
                    "__slots__": (),
                    "_fields": fields,
                    "_field_map": field_map,
                    "_tuples_only": tuples_only,
                },
            )

            _ROW_CLASS_CACHE[cache_key] = row_factory
            return row_factory
