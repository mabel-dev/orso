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

import datetime
import decimal
import re
from enum import Enum
from typing import Any
from typing import Iterable
from typing import Tuple
from typing import Type
from typing import Union
from warnings import warn

from orso.tools import parse_iso


def _parse_type(type_str: str) -> Union[str, Tuple[str, Tuple[int, ...]]]:
    """
    Parses a SQL type string into its base type and optional parameters.

    Parameters:
        type_str (str): The type definition string (e.g., 'DECIMAL(10,2)', 'VARCHAR[255]', 'ARRAY<VARCHAR>').

    Returns:
        Union[str, Tuple[str, Tuple[int, ...]]]:
            - Just the base type (e.g., "INTEGER", "TEXT").
            - A tuple with the base type and a tuple of integer parameters if applicable (e.g., ("DECIMAL", (10, 2))).
    """

    # Match ARRAY<TYPE>
    array_match = re.match(r"ARRAY<([\w\s\[\]\(\)]+)>", type_str)
    if array_match:
        return "ARRAY", (array_match.group(1),)

    # Match DECIMAL(p,s)
    decimal_match = re.match(r"DECIMAL\((\d+),\s*(\d+)\)", type_str)
    if decimal_match:
        precision, scale = map(int, decimal_match.groups())
        return "DECIMAL", (precision, scale)

    # Match VARCHAR[n]
    varchar_match = re.match(r"VARCHAR\[(\d+)\]", type_str)
    if varchar_match:
        length = int(varchar_match.group(1))
        return "VARCHAR", (length,)

    # Match BLOB[n]
    blob_match = re.match(r"BLOB\[(\d+)\]", type_str)
    if blob_match:
        size = int(blob_match.group(1))
        return "BLOB", (size,)

    # If no parameters, return base type as a string
    return type_str.upper()


class OrsoTypes(str, Enum):
    """
    The names of the types supported by Orso
    """

    ARRAY = "ARRAY"
    BLOB = "BLOB"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DECIMAL = "DECIMAL"
    DOUBLE = "DOUBLE"
    INTEGER = "INTEGER"
    INTERVAL = "INTERVAL"
    STRUCT = "STRUCT"
    TIMESTAMP = "TIMESTAMP"
    TIME = "TIME"
    VARCHAR = "VARCHAR"
    NULL = "NULL"
    JSONB = "JSONB"
    _MISSING_TYPE = 0

    def __init__(self, *args, **kwargs):
        self._precision: int = None
        self._scale: int = None
        self._element_type: "OrsoTypes" = None
        self._length: int = None

        str.__init__(self)
        Enum.__init__(self)

    def is_numeric(self):
        """is the typle number-based"""
        return self in (self.INTEGER, self.DOUBLE, self.DECIMAL, self.BOOLEAN)

    def is_temporal(self):
        """is the type time-based"""
        return self in (self.DATE, self.TIME, self.TIMESTAMP)

    def is_large_object(self):
        """is the type arbitrary length string"""
        return self in (self.VARCHAR, self.BLOB)

    def is_complex(self):
        return self in (self.ARRAY, self.STRUCT, self.JSONB, self.INTERVAL)

    def __str__(self):
        return self.value

    def parse(self, value: Any) -> Any:
        return ORSO_TO_PYTHON_PARSER[self.value](value)

    @property
    def python_type(self) -> Type:
        return ORSO_TO_PYTHON_MAP.get(self)

    @property
    def numpy_dtype(self):
        import numpy

        MAP = {
            OrsoTypes.ARRAY: numpy.dtype("O"),
            OrsoTypes.BLOB: numpy.dtype("S"),
            OrsoTypes.BOOLEAN: numpy.dtype("?"),
            OrsoTypes.DATE: numpy.dtype("datetime64[D]"),  # [2.5e16 BC, 2.5e16 AD]
            OrsoTypes.DECIMAL: numpy.dtype("O"),
            OrsoTypes.DOUBLE: numpy.dtype("float64"),
            OrsoTypes.INTEGER: numpy.dtype("int64"),
            OrsoTypes.INTERVAL: numpy.dtype("m"),
            OrsoTypes.STRUCT: numpy.dtype("O"),
            OrsoTypes.TIMESTAMP: numpy.dtype("datetime64[us]"),  # [290301 BC, 294241 AD]
            OrsoTypes.TIME: numpy.dtype("O"),
            OrsoTypes.VARCHAR: numpy.dtype("U"),
            OrsoTypes.NULL: numpy.dtype("O"),
        }
        return MAP.get(self)

    @staticmethod
    def from_name(name: str) -> tuple:
        _length = None
        _precision = None
        _scale = None
        _element_type = None

        if name is None:
            return (OrsoTypes._MISSING_TYPE, _length, _precision, _scale, _element_type)

        type_name = str(name).upper()
        parsed_types = _parse_type(type_name)
        if isinstance(parsed_types, str):
            if parsed_types == "ARRAY":
                warn("Column type ARRAY without element_type, defaulting to VARCHAR.")
                _type = OrsoTypes.ARRAY
                _element_type = OrsoTypes.VARCHAR
            elif parsed_types in OrsoTypes.__members__:
                _type = OrsoTypes[parsed_types]
            elif parsed_types == "LIST":
                warn("Column type LIST will be deprecated in a future version, use ARRAY instead.")
                _type = OrsoTypes.ARRAY
            elif parsed_types == "NUMERIC":
                warn(
                    "Column type NUMERIC will be deprecated in a future version, use DECIMAL, DOUBLE or INTEGER instead. Mapped to DOUBLE, this may not be compatible with all values NUMERIC was compatible with."
                )
                _type = OrsoTypes.DOUBLE
            elif parsed_types == "BSON":
                warn("Column type BSON will be deprecated in a future version, use JSONB instead.")
                _type = OrsoTypes.JSONB
            elif parsed_types == "STRING":
                raise ValueError(f"Unknown type '{_type}'. Did you mean 'VARCHAR'?")
            elif (
                type_name == "0"
                or type_name == 0
                or type_name == "VARIANT"
                or type_name == "MISSING"
            ):
                _type = 0
            else:
                raise ValueError(f"Unknown column type '{name}''.")
        elif parsed_types[0] == "ARRAY":
            _type = OrsoTypes.ARRAY
            _element_type = parsed_types[1][0]
            if _element_type.startswith(("ARRAY", "LIST", "NUMERIC", "BSON", "STRING", "DECIMAL")):
                raise ValueError(f"Invalid element type '{_element_type}' for ARRAY type.")
            if _element_type in OrsoTypes.__members__:
                _type = OrsoTypes.ARRAY
                _element_type = OrsoTypes[_element_type]
            else:
                raise ValueError(f"Unknown column type '{_element_type}'.")
        elif parsed_types[0] == "DECIMAL":
            _type = OrsoTypes.DECIMAL
            _precision, _scale = parsed_types[1]
        elif parsed_types[0] == "VARCHAR":
            _type = OrsoTypes.VARCHAR
            _length = parsed_types[1][0]
        elif parsed_types[0] == "BLOB":
            _type = OrsoTypes.BLOB
            _length = parsed_types[1][0]
        else:
            raise ValueError(f"Unknown column type '{_type}'.")

        return (_type, _length, _precision, _scale, _element_type)


ORSO_TO_PYTHON_MAP: dict = {
    OrsoTypes.BOOLEAN: bool,
    OrsoTypes.BLOB: bytes,
    OrsoTypes.DATE: datetime.date,
    OrsoTypes.TIMESTAMP: datetime.datetime,
    OrsoTypes.TIME: datetime.time,
    OrsoTypes.INTERVAL: datetime.timedelta,
    OrsoTypes.STRUCT: dict,
    OrsoTypes.DECIMAL: decimal.Decimal,
    OrsoTypes.DOUBLE: float,
    OrsoTypes.INTEGER: int,
    OrsoTypes.ARRAY: list,
    OrsoTypes.VARCHAR: str,
    OrsoTypes.JSONB: bytes,
    OrsoTypes.NULL: None,
}

PYTHON_TO_ORSO_MAP: dict = {
    value: key for key, value in ORSO_TO_PYTHON_MAP.items() if key != OrsoTypes.JSONB
}
PYTHON_TO_ORSO_MAP.update({tuple: OrsoTypes.ARRAY, set: OrsoTypes.ARRAY})  # map other python types

ORSO_TO_PYTHON_PARSER: dict = {
    OrsoTypes.BOOLEAN: bool,
    OrsoTypes.BLOB: lambda x: x.encode("utf-8") if isinstance(x, str) else bytes(x),
    OrsoTypes.DATE: lambda x: parse_iso(x).date(),
    OrsoTypes.TIMESTAMP: parse_iso,
    OrsoTypes.TIME: lambda x: parse_iso(x).time(),
    OrsoTypes.INTERVAL: datetime.timedelta,
    OrsoTypes.STRUCT: dict,
    OrsoTypes.DECIMAL: decimal.Decimal,
    OrsoTypes.DOUBLE: float,
    OrsoTypes.INTEGER: int,
    OrsoTypes.ARRAY: list,
    OrsoTypes.VARCHAR: str,
    OrsoTypes.JSONB: bytes,
    OrsoTypes.NULL: lambda x: None,
}


def find_compatible_type(types: Iterable[OrsoTypes], default=OrsoTypes.VARCHAR) -> OrsoTypes:
    """
    Find the most compatible type that can represent all input types.

    Parameters:
        types (list): List of OrsoTypes to find a compatible type for

    Returns:
        OrsoTypes: The most compatible type that can represent all input types

    Examples:
        >>> OrsoTypes.find_compatible_type([OrsoTypes.INTEGER, OrsoTypes.DOUBLE])
        OrsoTypes.DOUBLE
        >>> OrsoTypes.find_compatible_type([OrsoTypes.BLOB, OrsoTypes.VARCHAR])
        OrsoTypes.VARCHAR
    """
    if not types:
        return OrsoTypes.NULL

    # Handle single type case
    if len(set(types)) == 1:
        return types[0]

    # Define type promotion hierarchy
    type_hierarchy = {
        # Numeric promotion
        OrsoTypes.BOOLEAN: 1,
        OrsoTypes.INTEGER: 2,
        OrsoTypes.DOUBLE: 3,
        OrsoTypes.DECIMAL: 4,
        # Temporal promotion
        OrsoTypes.DATE: 1,
        OrsoTypes.TIMESTAMP: 2,
        # String/binary promotion
        OrsoTypes.BLOB: 1,
        OrsoTypes.VARCHAR: 2,
    }

    # First check if all types are in the same category
    if all(t.is_numeric() for t in types):
        return max(types, key=lambda t: type_hierarchy.get(t, 0))
    if all(t.is_temporal() for t in types):
        return max(types, key=lambda t: type_hierarchy.get(t, 0))
    if all(t.is_large_object() for t in types):
        return max(types, key=lambda t: type_hierarchy.get(t, 0))
    if all(
        t in (OrsoTypes.BLOB, OrsoTypes.STRUCT, OrsoTypes.JSONB, OrsoTypes.VARCHAR) for t in types
    ):
        return OrsoTypes.BLOB

    # For heterogeneous types, default to the most flexible type
    if any(t == OrsoTypes.BLOB for t in types):
        return OrsoTypes.BLOB
    return default
