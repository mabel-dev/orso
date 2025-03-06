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
import numpy as np
cimport numpy as cnp
from numpy cimport ndarray
from libc.stdint cimport int32_t, int64_t
from cpython.dict cimport PyDict_GetItem
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM

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
    cdef int64_t i, num_fields = len(fields)
    cdef void* value_ptr
    cdef list field_data = [None] * num_fields

    for i in range(num_fields):
        value_ptr = PyDict_GetItem(data, fields[i])
        if value_ptr != NULL:
            field_data[i] = <object>value_ptr
        else:
            field_data[i] = None

    return tuple(field_data)  # Convert list to tuple


cpdef cnp.ndarray collect_cython(list rows, int32_t[:] columns, int limit=-1):
    """
    Collects columns from a list of tuples (rows).
    """
    cdef int64_t i, j
    cdef int64_t num_rows = len(rows)
    cdef int64_t num_cols = columns.shape[0]
    cdef int32_t col_idx
    cdef object row
    cdef tuple tuple_row
    
    # Early exit if no rows or columns
    if num_rows == 0 or num_cols == 0:
        return np.empty((num_cols, num_rows), dtype=object)
    
    cdef int64_t row_width = len(rows[0])
    
    # Check if limit is set and within bounds
    if limit >= 0 and limit < num_rows:
        num_rows = limit
    
    # Check if columns are within bounds (only need to check once)
    for j in range(num_cols):
        col_idx = columns[j]
        if col_idx < 0 or col_idx >= row_width:
            raise IndexError(f"Column index out of bounds (0 <= {col_idx} < {row_width})")
    
    # Create result array directly
    cdef cnp.ndarray[object, ndim=2] result = np.empty((num_cols, num_rows), dtype=object)
    
    # Specialized fast paths for common column counts
    if num_cols == 1:
        # Single column case (very common)
        col_idx = columns[0]
        for i in range(num_rows):
            tuple_row = <tuple>rows[i]
            result[0, i] = tuple_row[col_idx]
    elif num_cols == 2:
        # Two column case (also common)
        col_idx0 = columns[0]
        col_idx1 = columns[1]
        for i in range(num_rows):
            tuple_row = <tuple>rows[i]
            result[0, i] = tuple_row[col_idx0]
            result[1, i] = tuple_row[col_idx1]
    else:
        # General case for any number of columns
        for i in range(num_rows):
            tuple_row = <tuple>rows[i]
            for j in range(num_cols):
                result[j, i] = tuple_row[columns[j]]
    
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



# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True

import pyarrow
cimport cython
import struct

from libc.stdint cimport int64_t, uint8_t, int32_t
from libc.stdint cimport int32_t, int64_t, uint8_t, uint64_t, uintptr_t
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
cpdef list _process_table(table, object row_factory, int max_chunksize):
    """
    Converts a PyArrow table into a list of tuples efficiently.

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
    cdef list result = []
    cdef Py_ssize_t num_cols = table.num_columns
    cdef Py_ssize_t row_idx, col_idx
    cdef object chunk, buffers
    cdef const uint8_t* validity
    cdef const int32_t* int_offsets
    cdef const char* data
    cdef Py_ssize_t str_start, str_end
    cdef bytes value
    cdef tuple row_tuple
    cdef uint8_t null_mask
    cdef Py_ssize_t byte_offset, bit_index
    cdef const uint8_t* raw_data

    for batch in table.to_batches(max_chunksize):
        batch_cols = batch.columns
        batch_num_rows = batch.num_rows

        # Preallocate row storage
        batch_result = [None] * batch_num_rows

        for row_idx in range(batch_num_rows):
            # Create Python list for row data first
            row_data = [None] * num_cols

            for col_idx in range(num_cols):
                chunk = batch_cols[col_idx]
                buffers = chunk.buffers()

                # Extract validity bitmap
                validity = <const uint8_t*><uintptr_t>buffers[0].address if buffers[0] else NULL

                # Compute null mask offsets
                if validity:
                    byte_offset = row_idx // 8
                    bit_index = row_idx % 8
                    null_mask = (validity[byte_offset] >> bit_index) & 1
                else:
                    null_mask = 1  # If no validity buffer, assume all valid

                if null_mask == 0:
                    # NULL value case
                    continue

                # Process based on type
                if pyarrow.types.is_string(chunk.type) or pyarrow.types.is_binary(chunk.type):
                    int_offsets = <const int32_t*><uintptr_t>buffers[1].address
                    data = <const char*><uintptr_t>buffers[2].address if len(buffers) > 2 else NULL

                    str_start = int_offsets[row_idx]
                    str_end = int_offsets[row_idx + 1]
                    
                    if str_start < str_end and data:
                        value = data[str_start:str_end]
                        row_data[col_idx] = value.decode('utf-8', errors='replace')
                    else:
                        row_data[col_idx] = ""

                # Integer handling section - replace with this safer version
                elif pyarrow.types.is_integer(chunk.type):
                    # Get raw pointer to numeric data and safely handle buffer sizes
                    if buffers[1]:
                        raw_data = <const uint8_t*><uintptr_t>buffers[1].address
                        item_size = chunk.type.bit_width // 8
                        buffer_size = buffers[1].size
                        
                        # Calculate offset and make sure we have enough bytes
                        offset = row_idx * item_size
                        if offset + item_size <= buffer_size:
                            if item_size == 8:  # int64
                                row_data[col_idx] = struct.unpack_from("<q", raw_data, offset)[0]
                            elif item_size == 4:  # int32
                                row_data[col_idx] = struct.unpack_from("<i", raw_data, offset)[0]
                            elif item_size == 2:  # int16
                                row_data[col_idx] = struct.unpack_from("<h", raw_data, offset)[0]
                            elif item_size == 1:  # int8
                                row_data[col_idx] = struct.unpack_from("<b", raw_data, offset)[0]
                        else:
                            # Not enough bytes in buffer
                            row_data[col_idx] = None
                    else:
                        row_data[col_idx] = None
                elif pyarrow.types.is_floating(chunk.type):
                    if buffers[1]:
                        raw_data = <const uint8_t*><uintptr_t>buffers[1].address
                        item_size = chunk.type.bit_width // 8
                        buffer_size = buffers[1].size
                        
                        # Calculate offset and make sure we have enough bytes
                        offset = row_idx * item_size
                        if offset + item_size <= buffer_size:
                            if item_size == 8:  # float64
                                row_data[col_idx] = struct.unpack_from("<d", raw_data, offset)[0]
                            elif item_size == 4:  # float32
                                row_data[col_idx] = struct.unpack_from("<f", raw_data, offset)[0]
                        else:
                            row_data[col_idx] = None
                    else:
                        row_data[col_idx] = None
                elif pyarrow.types.is_boolean(chunk.type):
                    # Booleans are bit-packed
                    bool_data = <const uint8_t*><uintptr_t>buffers[1].address if buffers[1] else NULL
                    if bool_data:
                        bool_value = (bool_data[byte_offset] >> bit_index) & 1
                        row_data[col_idx] = bool(bool_value)
                    else:
                        row_data[col_idx] = False

                else:
                    # Fallback for unsupported types
                    try:
                        row_data[col_idx] = chunk[row_idx].as_py()
                    except:
                        row_data[col_idx] = None

            # Convert list to tuple and apply row factory
            row_tuple = tuple(row_data)
            batch_result[row_idx] = row_factory(row_tuple)

        result.extend(batch_result)

    return result