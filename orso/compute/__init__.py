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

"""
Orso Compute Module

High-performance compiled implementations for column operations.
"""

# Import compiled functions
from orso.compute.compiled import (
    from_bytes_cython,
    extract_dict_columns,
    collect_cython,
    calculate_data_width,
    process_table,
)

# Import column encoding functions
from orso.compute.column_encodings import (
    rle_encode,
    rle_decode,
    dict_encode,
    dict_decode,
)

__all__ = [
    # From compiled module
    "from_bytes_cython",
    "extract_dict_columns",
    "collect_cython",
    "calculate_data_width",
    "process_table",
    # From column_encodings module
    "rle_encode",
    "rle_decode",
    "dict_encode",
    "dict_decode",
]
