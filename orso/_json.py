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

import json
from typing import Any

_json_loads = json.loads
_json_encoder = json.JSONEncoder()


def _stdlib_dumps_bytes(value: Any) -> bytes:
    return _json_encoder.encode(value).encode("utf-8")


_json_dumps_bytes = _stdlib_dumps_bytes

try:
    import simdjson as _simdjson  # type:ignore

    if hasattr(_simdjson, "loads"):
        _json_loads = _simdjson.loads
    if hasattr(_simdjson, "dumps"):
        _simdjson_dumps = _simdjson.dumps

        def _simdjson_dumps_bytes(value: Any) -> bytes:
            serialized = _simdjson_dumps(value)
            if isinstance(serialized, (bytes, bytearray)):
                return bytes(serialized)
            return str(serialized).encode("utf-8")

        _json_dumps_bytes = _simdjson_dumps_bytes
except ImportError:
    pass


def dumps_bytes(value: Any) -> bytes:
    return _json_dumps_bytes(value)


def loads(value):
    return _json_loads(value)
