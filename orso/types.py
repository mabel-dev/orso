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
from enum import Enum


class OrsoTypes(str, Enum):
    """
    The names of the types supported by Orso
    """

    ARRAY = "ARRAY"
    BLOB = "BLOB"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DOUBLE = "DOUBLE"
    INTEGER = "INTEGER"
    INTERVAL = "INTERVAL"
    STRUCT = "STRUCT"
    TIMESTAMP = "TIMESTAMP"
    TIME = "TIME"
    VARCHAR = "VARCHAR"


PYTHON_TO_ORSO_MAP: dict = {
    bool: OrsoTypes.BOOLEAN,
    bytes: OrsoTypes.BLOB,
    datetime.date: OrsoTypes.DATE,
    datetime.datetime: OrsoTypes.TIMESTAMP,
    datetime.time: OrsoTypes.TIME,
    datetime.timedelta: OrsoTypes.INTERVAL,
    dict: OrsoTypes.STRUCT,
    float: OrsoTypes.DOUBLE,
    int: OrsoTypes.INTEGER,
    list: OrsoTypes.ARRAY,
    str: OrsoTypes.VARCHAR,
}

ORSO_TO_PYTHON_MAP: dict = {value: key for key, value in PYTHON_TO_ORSO_MAP.items()}
