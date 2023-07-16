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

    def is_numeric(self):
        """is the typle number-based"""
        return self in (self.INTEGER, self.DOUBLE, self.DECIMAL)

    def is_temporal(self):
        """is the type time-based"""
        return self in (self.DATE, self.TIME, self.TIMESTAMP)


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
    OrsoTypes.NULL: None,
}

PYTHON_TO_ORSO_MAP: dict = {value: key for key, value in ORSO_TO_PYTHON_MAP.items()}
PYTHON_TO_ORSO_MAP.update({tuple: OrsoTypes.ARRAY, set: OrsoTypes.ARRAY})  # map other python types
