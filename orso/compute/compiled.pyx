#cython: infer_types=True
#cython: embedsignature=True
#cython: binding=False
#cython: language_level=3

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

from cpython.bytes cimport PyBytes_AsString, PyBytes_GET_SIZE
from cython cimport int
from datetime import datetime
from ormsgpack import unpackb
from orso.exceptions import DataError
from typing import Dict, Any, Tuple
from libc.stdlib cimport malloc, free

cimport cython

HEADER_PREFIX = b"\x10\x00"
MAXIMUM_RECORD_SIZE = 8 * 1024 * 1024

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing
cpdef from_bytes_cython(bytes data):
    cdef const char* data_ptr = PyBytes_AsString(data)
    cdef Py_ssize_t length = PyBytes_GET_SIZE(data)

    HEADER_SIZE = 14
    # Validate header and size, now using pointer arithmetic
    if length < HEADER_SIZE or (data_ptr[0] & 0xF0 != 0x10):
        raise DataError("Data malformed")
    
    # Deserialize record bytes
    cdef Py_ssize_t record_size = (
        (<unsigned char>data_ptr[2]) << 24 |
        (<unsigned char>data_ptr[3]) << 16 |
        (<unsigned char>data_ptr[4]) << 8  |
        (<unsigned char>data_ptr[5])
    )

    if record_size != length - HEADER_SIZE:
        raise DataError("Data malformed - incorrect length")

    # Deserialize and post-process
    cdef list raw_tuple = unpackb(data[HEADER_SIZE:])
    cdef list processed_list = []
    cdef object item


    for item in raw_tuple:
        if isinstance(item, list) and len(item) == 2 and item[0] == "__datetime__":
            processed_list.append(datetime.fromtimestamp(item[1]))
        else:
            processed_list.append(item)

    return tuple(processed_list)





cpdef tuple extract_dict_columns(dict data, tuple fields):
    cdef int i
    cdef str field
    cdef list sorted_data = [None] * len(fields)  # Preallocate list size
    for i in range(len(fields)):
        field = fields[i]
        sorted_data[i] = data[field]
    return tuple(sorted_data)  # Convert list to tuple




from libc.stdlib cimport malloc, free
import numpy as np
cimport cython
cimport numpy as cnp

@cython.boundscheck(False)
@cython.wraparound(False)
def collect_cython(list rows, cnp.ndarray[cnp.int32_t, ndim=1] columns, int limit=-1, int single=False) -> list:
    cdef int i, j
    cdef int num_rows = len(rows)
    cdef int num_cols = columns.shape[0]

    if limit >= 0 and limit < num_rows:
        num_rows = limit

    # Initialize result list
    cdef list result = [[None] * num_rows for _ in range(num_cols)]

    # Iterate over rows and columns, collecting data
    for i in range(num_rows):
        row = rows[i]
        for j in range(num_cols):
            result[j][i] = row[columns[j]]

    return result[0] if single else result

from cpython.object cimport PyObject_Str

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef int calculate_data_width(list column_values):
    cdef int width, max_width
    cdef object value
    
    max_width = 4  # Default width
    for value in column_values:
        if value:
            width = PyBytes_GET_SIZE(PyObject_Str(value))
            if width > max_width:
                max_width = width

    return max_width