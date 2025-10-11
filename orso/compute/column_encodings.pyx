# cython: infer_types=True
# cython: embedsignature=True
# cython: binding=False
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False

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
High-performance column encoding implementations using C++.
Supports RLE (Run-Length Encoding) and Dictionary encoding for various data types.
"""

import numpy as np
cimport numpy as cnp
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t

cnp.import_array()

# RLE encoding for 8-bit integers
def rle_encode_int8(cnp.ndarray[int8_t, ndim=1] data):
    """
    Encode an array of 8-bit integers using Run-Length Encoding.
    
    Parameters:
        data: numpy array of int8
    
    Returns:
        tuple: (values, lengths) where values are unique run values and lengths are run lengths
    """
    cdef Py_ssize_t i, size = data.shape[0]
    cdef list values = []
    cdef list lengths = []
    cdef int8_t current_value, prev_value
    cdef uint32_t current_length
    
    if size == 0:
        return (np.array([], dtype=np.int8), np.array([], dtype=np.uint32))
    
    prev_value = data[0]
    current_length = 1
    
    for i in range(1, size):
        current_value = data[i]
        if current_value == prev_value:
            current_length += 1
        else:
            values.append(prev_value)
            lengths.append(current_length)
            prev_value = current_value
            current_length = 1
    
    # Add the last run
    values.append(prev_value)
    lengths.append(current_length)
    
    return (np.array(values, dtype=np.int8), np.array(lengths, dtype=np.uint32))

# RLE decoding for 8-bit integers
def rle_decode_int8(cnp.ndarray[int8_t, ndim=1] values, cnp.ndarray[uint32_t, ndim=1] lengths):
    """
    Decode RLE encoded 8-bit integers.
    
    Parameters:
        values: numpy array of unique values
        lengths: numpy array of run lengths
    
    Returns:
        numpy array of decoded values
    """
    cdef Py_ssize_t i, j
    cdef Py_ssize_t num_runs = values.shape[0]
    cdef uint32_t total_size = 0
    cdef int8_t value
    cdef uint32_t length
    
    # Calculate total size
    for i in range(num_runs):
        total_size += lengths[i]
    
    cdef cnp.ndarray[int8_t, ndim=1] result = np.empty(total_size, dtype=np.int8)
    cdef Py_ssize_t pos = 0
    
    for i in range(num_runs):
        value = values[i]
        length = lengths[i]
        for j in range(length):
            result[pos] = value
            pos += 1
    
    return result

# RLE encoding for 16-bit integers
def rle_encode_int16(cnp.ndarray[int16_t, ndim=1] data):
    """Encode 16-bit integers using RLE."""
    cdef Py_ssize_t i, size = data.shape[0]
    cdef list values = []
    cdef list lengths = []
    cdef int16_t current_value, prev_value
    cdef uint32_t current_length
    
    if size == 0:
        return (np.array([], dtype=np.int16), np.array([], dtype=np.uint32))
    
    prev_value = data[0]
    current_length = 1
    
    for i in range(1, size):
        current_value = data[i]
        if current_value == prev_value:
            current_length += 1
        else:
            values.append(prev_value)
            lengths.append(current_length)
            prev_value = current_value
            current_length = 1
    
    values.append(prev_value)
    lengths.append(current_length)
    
    return (np.array(values, dtype=np.int16), np.array(lengths, dtype=np.uint32))

def rle_decode_int16(cnp.ndarray[int16_t, ndim=1] values, cnp.ndarray[uint32_t, ndim=1] lengths):
    """Decode RLE encoded 16-bit integers."""
    cdef Py_ssize_t i, j, num_runs = values.shape[0]
    cdef uint32_t total_size = 0
    
    for i in range(num_runs):
        total_size += lengths[i]
    
    cdef cnp.ndarray[int16_t, ndim=1] result = np.empty(total_size, dtype=np.int16)
    cdef Py_ssize_t pos = 0
    
    for i in range(num_runs):
        for j in range(lengths[i]):
            result[pos] = values[i]
            pos += 1
    
    return result

# RLE encoding for 32-bit integers
def rle_encode_int32(cnp.ndarray[int32_t, ndim=1] data):
    """Encode 32-bit integers using RLE."""
    cdef Py_ssize_t i, size = data.shape[0]
    cdef list values = []
    cdef list lengths = []
    cdef int32_t current_value, prev_value
    cdef uint32_t current_length
    
    if size == 0:
        return (np.array([], dtype=np.int32), np.array([], dtype=np.uint32))
    
    prev_value = data[0]
    current_length = 1
    
    for i in range(1, size):
        current_value = data[i]
        if current_value == prev_value:
            current_length += 1
        else:
            values.append(prev_value)
            lengths.append(current_length)
            prev_value = current_value
            current_length = 1
    
    values.append(prev_value)
    lengths.append(current_length)
    
    return (np.array(values, dtype=np.int32), np.array(lengths, dtype=np.uint32))

def rle_decode_int32(cnp.ndarray[int32_t, ndim=1] values, cnp.ndarray[uint32_t, ndim=1] lengths):
    """Decode RLE encoded 32-bit integers."""
    cdef Py_ssize_t i, j, num_runs = values.shape[0]
    cdef uint32_t total_size = 0
    
    for i in range(num_runs):
        total_size += lengths[i]
    
    cdef cnp.ndarray[int32_t, ndim=1] result = np.empty(total_size, dtype=np.int32)
    cdef Py_ssize_t pos = 0
    
    for i in range(num_runs):
        for j in range(lengths[i]):
            result[pos] = values[i]
            pos += 1
    
    return result

# RLE encoding for 64-bit integers
def rle_encode_int64(cnp.ndarray[int64_t, ndim=1] data):
    """Encode 64-bit integers using RLE."""
    cdef Py_ssize_t i, size = data.shape[0]
    cdef list values = []
    cdef list lengths = []
    cdef int64_t current_value, prev_value
    cdef uint32_t current_length
    
    if size == 0:
        return (np.array([], dtype=np.int64), np.array([], dtype=np.uint32))
    
    prev_value = data[0]
    current_length = 1
    
    for i in range(1, size):
        current_value = data[i]
        if current_value == prev_value:
            current_length += 1
        else:
            values.append(prev_value)
            lengths.append(current_length)
            prev_value = current_value
            current_length = 1
    
    values.append(prev_value)
    lengths.append(current_length)
    
    return (np.array(values, dtype=np.int64), np.array(lengths, dtype=np.uint32))

def rle_decode_int64(cnp.ndarray[int64_t, ndim=1] values, cnp.ndarray[uint32_t, ndim=1] lengths):
    """Decode RLE encoded 64-bit integers."""
    cdef Py_ssize_t i, j, num_runs = values.shape[0]
    cdef uint32_t total_size = 0
    
    for i in range(num_runs):
        total_size += lengths[i]
    
    cdef cnp.ndarray[int64_t, ndim=1] result = np.empty(total_size, dtype=np.int64)
    cdef Py_ssize_t pos = 0
    
    for i in range(num_runs):
        for j in range(lengths[i]):
            result[pos] = values[i]
            pos += 1
    
    return result

# Dictionary encoding for integers
def dict_encode_int32(cnp.ndarray[int32_t, ndim=1] data):
    """
    Encode 32-bit integers using dictionary encoding.
    
    Parameters:
        data: numpy array of int32
    
    Returns:
        tuple: (dictionary, indices) where dictionary contains unique values and indices map to them
    """
    cdef Py_ssize_t i, size = data.shape[0]
    cdef dict value_to_index = {}
    cdef list dictionary = []
    cdef list indices = []
    cdef int32_t value
    cdef uint32_t idx
    
    for i in range(size):
        value = data[i]
        if value not in value_to_index:
            idx = len(dictionary)
            value_to_index[value] = idx
            dictionary.append(value)
            indices.append(idx)
        else:
            indices.append(value_to_index[value])
    
    return (np.array(dictionary, dtype=np.int32), np.array(indices, dtype=np.uint32))

def dict_decode_int32(cnp.ndarray[int32_t, ndim=1] dictionary, cnp.ndarray[uint32_t, ndim=1] indices):
    """
    Decode dictionary encoded 32-bit integers.
    
    Parameters:
        dictionary: numpy array of unique values
        indices: numpy array of indices into dictionary
    
    Returns:
        numpy array of decoded values
    """
    cdef Py_ssize_t i, size = indices.shape[0]
    cdef cnp.ndarray[int32_t, ndim=1] result = np.empty(size, dtype=np.int32)
    cdef uint32_t idx
    
    for i in range(size):
        idx = indices[i]
        if idx >= dictionary.shape[0]:
            raise IndexError(f"Dictionary index {idx} out of range")
        result[i] = dictionary[idx]
    
    return result

def dict_encode_int64(cnp.ndarray[int64_t, ndim=1] data):
    """Encode 64-bit integers using dictionary encoding."""
    cdef Py_ssize_t i, size = data.shape[0]
    cdef dict value_to_index = {}
    cdef list dictionary = []
    cdef list indices = []
    cdef int64_t value
    cdef uint32_t idx
    
    for i in range(size):
        value = data[i]
        if value not in value_to_index:
            idx = len(dictionary)
            value_to_index[value] = idx
            dictionary.append(value)
            indices.append(idx)
        else:
            indices.append(value_to_index[value])
    
    return (np.array(dictionary, dtype=np.int64), np.array(indices, dtype=np.uint32))

def dict_decode_int64(cnp.ndarray[int64_t, ndim=1] dictionary, cnp.ndarray[uint32_t, ndim=1] indices):
    """Decode dictionary encoded 64-bit integers."""
    cdef Py_ssize_t i, size = indices.shape[0]
    cdef cnp.ndarray[int64_t, ndim=1] result = np.empty(size, dtype=np.int64)
    cdef uint32_t idx
    
    for i in range(size):
        idx = indices[i]
        if idx >= dictionary.shape[0]:
            raise IndexError(f"Dictionary index {idx} out of range")
        result[i] = dictionary[idx]
    
    return result

# Generic RLE encode/decode that dispatches based on dtype
def rle_encode(cnp.ndarray data):
    """
    Encode an array using Run-Length Encoding.
    Automatically dispatches to the appropriate typed implementation based on dtype.
    
    Parameters:
        data: numpy array of supported dtype (int8, int16, int32, int64)
    
    Returns:
        tuple: (values, lengths)
    """
    if data.dtype == np.int8:
        return rle_encode_int8(data)
    elif data.dtype == np.int16:
        return rle_encode_int16(data)
    elif data.dtype == np.int32:
        return rle_encode_int32(data)
    elif data.dtype == np.int64:
        return rle_encode_int64(data)
    else:
        raise TypeError(f"Unsupported dtype for RLE encoding: {data.dtype}")

def rle_decode(cnp.ndarray values, cnp.ndarray lengths):
    """
    Decode RLE encoded data.
    Automatically dispatches to the appropriate typed implementation based on values dtype.
    
    Parameters:
        values: numpy array of unique values
        lengths: numpy array of run lengths (uint32)
    
    Returns:
        numpy array of decoded values
    """
    if values.dtype == np.int8:
        return rle_decode_int8(values, lengths)
    elif values.dtype == np.int16:
        return rle_decode_int16(values, lengths)
    elif values.dtype == np.int32:
        return rle_decode_int32(values, lengths)
    elif values.dtype == np.int64:
        return rle_decode_int64(values, lengths)
    else:
        raise TypeError(f"Unsupported dtype for RLE decoding: {values.dtype}")

def dict_encode(cnp.ndarray data):
    """
    Encode an array using dictionary encoding.
    Automatically dispatches to the appropriate typed implementation based on dtype.
    
    Parameters:
        data: numpy array of supported dtype (int32, int64)
    
    Returns:
        tuple: (dictionary, indices)
    """
    if data.dtype == np.int32:
        return dict_encode_int32(data)
    elif data.dtype == np.int64:
        return dict_encode_int64(data)
    else:
        raise TypeError(f"Unsupported dtype for dictionary encoding: {data.dtype}")

def dict_decode(cnp.ndarray dictionary, cnp.ndarray indices):
    """
    Decode dictionary encoded data.
    Automatically dispatches to the appropriate typed implementation based on dictionary dtype.
    
    Parameters:
        dictionary: numpy array of unique values
        indices: numpy array of indices (uint32)
    
    Returns:
        numpy array of decoded values
    """
    if dictionary.dtype == np.int32:
        return dict_decode_int32(dictionary, indices)
    elif dictionary.dtype == np.int64:
        return dict_decode_int64(dictionary, indices)
    else:
        raise TypeError(f"Unsupported dtype for dictionary decoding: {dictionary.dtype}")
