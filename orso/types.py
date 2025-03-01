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
from enum import Enum
from typing import Any
from typing import Type

from orso.tools import parse_iso


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

    def is_numeric(self):
        """is the typle number-based"""
        return self in (self.INTEGER, self.DOUBLE, self.DECIMAL)

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
