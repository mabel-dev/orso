# cython: infer_types=True
# cython: embedsignature=True
# cython: binding=False
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False

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
from cpython.object cimport PyObject_Str
from cython cimport int
from datetime import datetime
from ormsgpack import unpackb
from orso.exceptions import DataError
from typing import Dict, Any, Tuple
from libc.stdlib cimport malloc, free
import numpy as np
cimport cython
cimport numpy as cnp
from numpy cimport ndarray
from libc.stdint cimport int32_t

cnp.import_array()

HEADER_PREFIX = b"\x10\x00"
MAXIMUM_RECORD_SIZE = 8 * 1024 * 1024


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
    cdef list field_data = [None] * len(fields)  # Preallocate list size

    for i, field in enumerate(fields):
        if field in data:
            field_data[i] = data[field]
    return tuple(field_data)  # Convert list to tuple


cpdef cnp.ndarray collect_cython(list rows, cnp.ndarray[cnp.int32_t, ndim=1] columns, int limit=-1):
    """
    Collects columns from a list of tuples (rows).
    """
    cdef int32_t i, j, col_idx
    cdef int32_t num_rows = len(rows)
    cdef int32_t num_cols = columns.shape[0]
    cdef cnp.ndarray row

    if limit >= 0 and limit < num_rows:
        num_rows = limit

    # Initialize result memory view with pre-allocated numpy arrays for each column
    cdef cnp.ndarray result = np.empty((num_cols, num_rows), dtype=object)

    # Populate each column one at a time
    for j in range(num_cols):
        col_idx = columns[j]
        for i in range(num_rows):
            result[j, i] = rows[i][col_idx]
    
    # Convert each column back to a list and return the list of lists
    return result


cpdef int calculate_data_width(cnp.ndarray column_values):
    cdef int width, max_width
    cdef object value
    
    max_width = 4  # Default width
    for value in column_values:
        if value is not None:
            width = PyBytes_GET_SIZE(PyObject_Str(value))
            if width > max_width:
                max_width = width

    return max_width


def process_table(table, row_factory, int max_chunksize) -> list:
    cdef list rows = []

    for batch in table.to_batches(max_chunksize):
        df = batch.to_pandas().replace({np.nan: None})
        for row in df.itertuples(index=False, name=None):
            rows.append(row_factory(row))
    return rows
