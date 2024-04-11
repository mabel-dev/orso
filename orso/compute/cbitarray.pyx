# cython: language_level=3

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
This module was written with assistance from ChatGPT
"""

cdef extern from "Python.h":
    void *PyMem_Malloc(int size) nogil
    void PyMem_Free(void *ptr) nogil
    void memset(void *s, int c, int n)

cdef class BitArray:
    cdef public int size
    cdef long long *bits

    def __init__(self, int size):
        assert size > 0, "bitarray size must be a positive integer"
        self.size = size
        cdef int n_longs = (size + 63) >> 6
        self.bits = <long long *> PyMem_Malloc(n_longs * sizeof(long long))
        memset(self.bits, 0, n_longs * sizeof(long long))

    def __dealloc__(self):
        with nogil:
            PyMem_Free(self.bits)

    def get(self, int index):
        if 0 > index or index >= self.size:
            raise IndexError("Index out of range")
        return (self.bits[index >> 6] & (1 << (index & 63))) != 0

    def set(self, int index, bint value):
        if 0 > index or index >= self.size:
            raise IndexError("Index out of range")
        if value:
            self.bits[index >> 6] |= (1 << (index & 63))
        else:
            self.bits[index >> 6] &= ~(1 << (index & 63))

    @property
    def array(self):
        ba = bytearray((self.size + 7) >> 3)
        for i in range((self.size + 63) >> 6):
            for j in range(64):
                if i * 64 + j < self.size:
                    ba[i*8 + (j>>3)] |= ((self.bits[i] >> j) & 1) << (j & 7)
        return ba

    @classmethod
    def from_array(cls, array, int length):
        bit_array = cls(length)
        for index in range(length):
            bit = (array[(index >> 3)] & (1 << (index & 7))) != 0
            if index < length and bit:
                bit_array.set(index, 1)
        return bit_array
