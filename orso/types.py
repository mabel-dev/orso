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

from orso._json import dumps_bytes as json_dumps_bytes
from orso._json import loads as json_loads
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


def get_orso_type(type_str: str) -> "OrsoTypes":
    """
    Convert a type string to an OrsoType enum value with full type information.

    This function parses a type string and returns an OrsoType enum value with
    all relevant attributes set (precision, scale, length, element types).

    Parameters:
        type_str (str): The type definition string (e.g., 'INTEGER', 'ARRAY<INTEGER>', 'DECIMAL(10,2)').

    Returns:
        OrsoTypes: The corresponding OrsoType enum value with all attributes properly set.

    Raises:
        ValueError: If the type string is not recognized.

    Examples:
        >>> t = get_orso_type("INTEGER")
        >>> t == OrsoTypes.INTEGER
        True

        >>> t = get_orso_type("DECIMAL(10,2)")
        >>> t._precision
        10
        >>> t._scale
        2

        >>> t = get_orso_type("VARCHAR[255]")
        >>> t._length
        255

        >>> t = get_orso_type("ARRAY<INTEGER>")
        >>> t._element_type == OrsoTypes.INTEGER
        True
    """
    if not type_str:
        raise ValueError("Type string cannot be empty")

    # Use the existing from_name method which handles all type attributes
    _type, _length, _precision, _scale, _element_type = OrsoTypes.from_name(type_str)

    if _type == 0 or _type is None:
        raise ValueError(f"Unknown type '{type_str}'")

    # Attach all the metadata to the returned type instance
    # The __init__ method initializes these as None, so we just update them
    object.__setattr__(_type, "_length", _length)
    object.__setattr__(_type, "_precision", _precision)
    object.__setattr__(_type, "_scale", _scale)
    object.__setattr__(_type, "_element_type", _element_type)

    return _type


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
    VECTOR = "VECTOR"
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
        return self in (self.ARRAY, self.STRUCT, self.JSONB, self.INTERVAL, self.VECTOR)

    def __str__(self):
        if self.value == self.ARRAY and self._element_type is not None:
            return f"ARRAY<{self._element_type}>"
        if self.value == self.DECIMAL and self._precision is not None and self._scale is not None:
            return f"DECIMAL({self._precision}, {self._scale})"
        if self.value == self.VARCHAR and self._length is not None:
            return f"VARCHAR[{self._length}]"
        if self.value == self.BLOB and self._length is not None:
            return f"BLOB[{self._length}]"
        if self.value == self.VECTOR and self._length is not None:
            return f"VECTOR[{self._length}]"
        return self.value

    def parse(self, value: Any, **kwargs) -> Any:
        kwargs["length"] = kwargs.get("length", self._length)
        kwargs["precision"] = kwargs.get("precision", self._precision)
        kwargs["scale"] = kwargs.get("scale", self._scale)
        kwargs["element_type"] = kwargs.get("element_type", self._element_type)

        if value is None:
            return None
        return ORSO_TO_PYTHON_PARSER[self.value](value, **kwargs)

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
            OrsoTypes.JSONB: numpy.dtype("O"),
            OrsoTypes.VECTOR: numpy.dtype("O"),
        }
        return MAP.get(self)

    @property
    def metadata(self) -> dict:
        """Return metadata used for generating documentation.

        Metadata keys:
            - description: short text explaining the type
            - min: minimum representable value (or constraint)
            - max: maximum representable value (or constraint)
            - example: example literal for the type
            - notes: additional documentation notes (Markdown)
        """

        # Base metadata for each type (static per type)
        base = _ORSO_TYPE_METADATA.get(self, {}).copy()

        # Add parameterized details when available
        if self == OrsoTypes.DECIMAL:
            precision = self._precision or 38
            scale = self._scale or 21
            place = max(precision - scale, 0)
            max_int_part = 10**place - 1 if place > 0 else 0
            max_fraction = 10**scale - 1
            base["min"] = f"-{max_int_part}.{str(max_fraction).zfill(scale)}"
            base["max"] = f"{max_int_part}.{str(max_fraction).zfill(scale)}"
            base.setdefault("example", "123.45")
            base.setdefault(
                "notes",
                "Precision and scale are configurable. If not specified, defaults to precision=38, scale=21.",
            )
        elif self in (OrsoTypes.VARCHAR, OrsoTypes.BLOB, OrsoTypes.VECTOR):
            length = self._length
            if length is not None:
                base["min"] = 0
                base["max"] = length
                base["notes"] = f"Maximum length is {length} when specified."
            else:
                base.setdefault("notes", "Length is unbounded unless specified.")
        elif self == OrsoTypes.ARRAY:
            element = self._element_type
            base.setdefault(
                "notes",
                f"Array containing elements of type {element.value if element is not None else 'UNKNOWN'}.",
            )

        return base

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
            elif parsed_types == "VECTOR":
                _type = OrsoTypes.VECTOR
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
            if _element_type.startswith(
                ("ARRAY", "LIST", "NUMERIC", "BSON", "STRING", "DECIMAL", "VECTOR")
            ):
                raise ValueError(f"Invalid element type '{_element_type}' for ARRAY type.")
            if _element_type in OrsoTypes.__members__:
                _type = OrsoTypes.ARRAY
                _element_type = OrsoTypes[_element_type]
            else:
                raise ValueError(f"Unknown column type '{_element_type}'.")
        elif parsed_types[0] == "DECIMAL":
            _type = OrsoTypes.DECIMAL
            _precision, _scale = parsed_types[1]
            if _precision < 0 or _precision > 38:
                raise ValueError(f"Invalid precision '{_precision}' for DECIMAL type.")
            if _scale < 0 or _scale > 38:
                raise ValueError(f"Invalid scale '{_scale}' for DECIMAL type.")
            if _precision < _scale:
                raise ValueError(
                    "Precision must be equal to or greater than scale for DECIMAL type."
                )
        elif parsed_types[0] == "VARCHAR":
            _type = OrsoTypes.VARCHAR
            _length = parsed_types[1][0]
        elif parsed_types[0] in ("BLOB", "VARBINARY"):
            _type = OrsoTypes.BLOB
            _length = parsed_types[1][0]
        elif parsed_types[0] == "VECTOR":
            _type = OrsoTypes.VECTOR
            _length = parsed_types[1][0]
        else:
            raise ValueError(f"Unknown column type '{_type}'.")

        return (_type, _length, _precision, _scale, _element_type)


BOOLEAN_STRINGS = (
    "TRUE",
    "ON",
    "YES",
    "1",
    "1.0",
    "T",
    "Y",
    b"TRUE",
    b"ON",
    b"YES",
    b"1",
    b"1.0",
    b"T",
    b"Y",
)


def parse_decimal(value, *, precision=None, scale=None, **kwargs):
    from orso.tools import DecimalFactory

    if value is None:
        return None

    scale = 21 if scale is None else int(scale)
    precision = 38 if precision is None else int(precision)
    value = (
        value.as_py()
        if hasattr(value, "as_py")
        else (
            value.item()
            if hasattr(value, "item") and not isinstance(value, (list, dict, tuple))
            else value
        )
    )
    if isinstance(value, float):
        value = format(value, ".99g")
    elif isinstance(value, int):
        value = str(value)
    elif isinstance(value, bytes):
        value = value.decode("utf-8")
    value = value.strip()
    factory = DecimalFactory.new_factory(precision, scale)
    return factory(value)


_ORSO_TYPE_METADATA = {
    OrsoTypes.BOOLEAN: {
        "description": "Boolean value representing true or false.",
        "min": False,
        "max": True,
        "example": "TRUE",
    },
    OrsoTypes.INTEGER: {
        "description": "Signed 64-bit integer.",
        "min": -9223372036854775808,
        "max": 9223372036854775807,
        "example": "42",
    },
    OrsoTypes.DOUBLE: {
        "description": "Double-precision floating point number.",
        "min": -1.7976931348623157e308,
        "max": 1.7976931348623157e308,
        "example": "123.45",
    },
    OrsoTypes.DECIMAL: {
        "description": "Fixed-point decimal number with configurable precision and scale.",
        "example": "123.45",
        "notes": "If precision/scale are not defined, defaults to precision=38 and scale=21.",
    },
    OrsoTypes.VARCHAR: {
        "description": "Variable-length string.",
        "min": 0,
        "max": None,
        "example": "'hello'",
        "notes": "By default, length is unbounded unless specified (e.g. VARCHAR[255]).",
    },
    OrsoTypes.BLOB: {
        "description": "Binary large object (bytes).",
        "min": 0,
        "max": None,
        "example": "b'\\x01\\x02'",
        "notes": "By default, length is unbounded unless specified (e.g. BLOB[1024]).",
    },
    OrsoTypes.DATE: {
        "description": "Calendar date (YYYY-MM-DD).",
        "min": str(datetime.date.min),
        "max": str(datetime.date.max),
        "example": "'2023-04-18'",
    },
    OrsoTypes.TIMESTAMP: {
        "description": "Timestamp including date and time.",
        "min": str(datetime.datetime.min),
        "max": str(datetime.datetime.max),
        "example": "'2023-04-18T12:34:56'",
    },
    OrsoTypes.TIME: {
        "description": "Time of day (HH:MM:SS).",
        "min": str(datetime.time.min),
        "max": str(datetime.time.max),
        "example": "'12:34:56'",
    },
    OrsoTypes.INTERVAL: {
        "description": "Time interval/duration.",
        "example": "'1 day 02:03:04'",
    },
    OrsoTypes.STRUCT: {
        "description": "Structured record (mapping of field names to values).",
        "example": '{"id": 1, "name": "Alice"}',
    },
    OrsoTypes.JSONB: {
        "description": "JSON binary data.",
        "example": '{"key": "value"}',
    },
    OrsoTypes.ARRAY: {
        "description": "Array of values of a single type.",
        "example": "[1, 2, 3]",
        "notes": "Element type is specified as ARRAY<INTEGER>, ARRAY<VARCHAR>, etc.",
    },
    OrsoTypes.VECTOR: {
        "description": "Fixed-length numeric vector.",
        "example": "[0.1, 0.2, 0.3]",
        "notes": "Length can be specified as VECTOR[<size>].",
    },
    OrsoTypes.NULL: {
        "description": "Null value.",
        "example": "NULL",
        "notes": "Represents absence of a value.",
    },
}

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
    OrsoTypes.VECTOR: list,
}

PYTHON_TO_ORSO_MAP: dict = {
    value: key for key, value in ORSO_TO_PYTHON_MAP.items() if key != OrsoTypes.JSONB
}
PYTHON_TO_ORSO_MAP.update({tuple: OrsoTypes.ARRAY, set: OrsoTypes.ARRAY})  # map other python types


def parse_boolean(x, **kwargs):
    return (x if isinstance(x, (bytes, str)) else str(x)).upper() in BOOLEAN_STRINGS


def parse_bytes(x, **kwargs):
    length = kwargs.get("length")
    if isinstance(x, (dict, list, tuple, set)):
        value = json_dumps_bytes(x)
    else:
        value = str(x).encode("utf-8") if not isinstance(x, bytes) else x
    if length:
        value = value[:length]
    return value


def parse_date(x, **kwargs):
    result = parse_iso(x)
    if result is None:
        raise ValueError("Invalid date.")
    return result.date()


def parse_time(x, **kwargs):
    result = parse_iso(x)
    if result is None:
        raise ValueError("Invalid date.")
    return result.time()


def parse_varchar(x, **kwargs):
    byte_version = parse_bytes(x, **kwargs)
    if isinstance(byte_version, bytes):
        return byte_version.decode("utf-8")
    return str(byte_version)


def parse_array(x, **kwargs):
    element_type = kwargs.get("element_type")
    if not isinstance(x, (list, tuple, set)):
        x = json_loads(x)
    if element_type is None:
        return x
    parser = element_type.parse
    return [parser(v) for v in x]


def parse_vector(x, **kwargs):
    if not isinstance(x, (list, tuple, set)):
        x = json_loads(x)
    return [float(v) for v in x]


def parse_double(x, **kwargs):
    return float(x)


def parse_integer(x, **kwargs):
    return int(x)


def parse_null(x, **kwargs):
    return None


def parse_timestamp(x, **kwargs):
    result = parse_iso(x)
    if result is None:
        raise ValueError(f"Invalid timestamp.")
    return result


def parse_interval(x, **kwargs):
    return datetime.timedelta(x)


ORSO_TO_PYTHON_PARSER: dict = {
    OrsoTypes.BOOLEAN: parse_boolean,
    OrsoTypes.BLOB: parse_bytes,
    OrsoTypes.DATE: parse_date,
    OrsoTypes.TIMESTAMP: parse_timestamp,
    OrsoTypes.TIME: parse_time,
    OrsoTypes.INTERVAL: parse_interval,
    OrsoTypes.STRUCT: parse_bytes,
    OrsoTypes.DECIMAL: parse_decimal,
    OrsoTypes.DOUBLE: parse_double,
    OrsoTypes.INTEGER: parse_integer,
    OrsoTypes.ARRAY: parse_array,
    OrsoTypes.VARCHAR: parse_varchar,
    OrsoTypes.JSONB: parse_bytes,
    OrsoTypes.NULL: parse_null,
    OrsoTypes.VECTOR: parse_vector,
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
    # Types move towards greater numbers
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
        # Vectors are a special array
        OrsoTypes.VECTOR: 1,
        OrsoTypes.ARRAY: 2,
    }

    # First check if all types are in the same category
    if all(t.is_numeric() for t in types):
        return max(types, key=lambda t: type_hierarchy.get(t, 0))
    if all(t.is_temporal() for t in types):
        return max(types, key=lambda t: type_hierarchy.get(t, 0))
    if all(t.is_large_object() for t in types):
        return max(types, key=lambda t: type_hierarchy.get(t, 0))
    if all(t.is_complex() for t in types):
        return max(types, key=lambda t: type_hierarchy.get(t, 0))
    if all(
        t in (OrsoTypes.BLOB, OrsoTypes.STRUCT, OrsoTypes.JSONB, OrsoTypes.VARCHAR) for t in types
    ):
        return OrsoTypes.BLOB

    # For heterogeneous types, default to the most flexible type
    if any(t == OrsoTypes.BLOB for t in types):
        return OrsoTypes.BLOB
    return default
