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
from cpython.unicode cimport PyUnicode_GET_LENGTH
from cpython.object cimport PyObject_Str
from cython cimport int
from datetime import datetime
from ormsgpack import unpackb
from orso.exceptions import DataError
import numpy as np
cimport numpy as cnp
from numpy cimport ndarray
from libc.stdint cimport int32_t, int64_t
from cpython.dict cimport PyDict_GetItem
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.object cimport PyObject

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
        (<unsigned char>data_ptr[4]) << 8 |
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
    """
    Extracts the given fields from a dictionary and returns them as a tuple.

    Parameters:
        data: dict
            The dictionary to extract fields from.
        fields: tuple
            The field names to extract.

    Returns:
        A tuple containing values from the dictionary for the requested fields.
        Missing fields will have None.
    """
    cdef Py_ssize_t i, num_fields = len(fields)
    cdef PyObject* value_ptr
    cdef list field_data = [None] * num_fields

    for i in range(num_fields):
        value_ptr = PyDict_GetItem(data, fields[i])
        (<list>field_data)[i] = <object>value_ptr if value_ptr is not NULL else None

    return tuple(field_data)


cpdef cnp.ndarray collect_cython(list rows, int32_t[:] columns, int limit=-1):
    """
    Collects columns from a list of tuples (rows).
    """
    cdef Py_ssize_t i, j
    cdef Py_ssize_t num_rows = len(rows)
    cdef Py_ssize_t num_cols = columns.shape[0]
    cdef int32_t col_idx
    cdef object row
    cdef tuple tuple_row
    
    # Early exit if no rows or columns
    if num_rows == 0 or num_cols == 0:
        return np.empty((num_cols, 0), dtype=object)
    
    cdef Py_ssize_t row_width = len(<tuple>rows[0])
    
    # Check if limit is set and within bounds
    if limit >= 0 and limit < num_rows:
        num_rows = limit
    
    # Check if columns are within bounds (only need to check once)
    for j in range(num_cols):
        col_idx = columns[j]
        if col_idx < 0 or col_idx >= row_width:
            raise IndexError(f"Column index out of bounds (0 <= {col_idx} < {row_width})")
    
    # Create result array directly
    cdef cnp.ndarray result_arr = np.empty((num_cols, num_rows), dtype=object)
    cdef object[:, :] result = result_arr
    
    # Specialized fast paths for common column counts
    if num_cols == 1:
        # Single column case (very common)
        col_idx = columns[0]
        for i in range(num_rows):
            tuple_row = <tuple>rows[i]
            result[0, i] = tuple_row[col_idx]
        return result_arr
    elif num_cols == 2:
        # Two column case (also common)
        col_idx0 = columns[0]
        col_idx1 = columns[1]
        for i in range(num_rows):
            tuple_row = <tuple>rows[i]
            result[0, i] = tuple_row[col_idx0]
            result[1, i] = tuple_row[col_idx1]
        return result_arr

    # General case for any number of columns
    for i in range(num_rows):
        tuple_row = <tuple>rows[i]
        for j in range(num_cols):
            result[j, i] = tuple_row[columns[j]]
    return result_arr


cpdef int calculate_data_width(cnp.ndarray column_values):
    """
    Estimate the maximum display width of a column based on string conversion.
    """
    cdef Py_ssize_t i, n = column_values.shape[0]
    cdef int width, max_width = 4
    cdef object value, string_value

    for i in range(n):
        value = column_values[i]
        if value is not None:
            string_value = PyObject_Str(value)  # returns a unicode object
            width = PyUnicode_GET_LENGTH(string_value)
            if width > max_width:
                max_width = width

    return max_width


from cpython.list cimport PyList_New, PyList_SET_ITEM

def process_table(table, row_factory, int max_chunksize) -> list:
    """
    Processes a PyArrow table and applies a row factory function to each row.

    Parameters:
        table: PyArrow Table
            The input table to process.
        row_factory: function
            A function applied to each row.
        max_chunksize: int
            The batch size to process at a time.

    Returns:
        A list of transformed rows.
    """
    cdef list rows = [None] * table.num_rows
    cdef int64_t i = 0

    for batch in table.to_batches(max_chunksize):
        df = batch.to_pandas().replace({np.nan: None})
        for row in df.itertuples(index=False, name=None):
            rows[i] = row_factory(row)
            i += 1
    return rows
