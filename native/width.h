#pragma once
// orso/native/width.h — the msgpack size estimate behind nbytes() (DESIGN.md §4.6).
//
// v1 sized a row as `len(ormsgpack.packb(tuple(row), option=OPT_SERIALIZE_NUMPY,
// default=serialize)) + 14`, serialising every row to throw the bytes away. mabel uses
// that number to decide when a blob is full, so v2 keeps the number and drops the
// serialisation: msgpack is length-prefixed and concatenative, so the encoded size of a
// tuple is its array header plus the encoded size of each element, and for the types
// below the encoded size is a function of the value alone.
//
// Widths here were measured against ormsgpack (tests/test_native_rows.py pins them):
//   None, bool                  1
//   int  0..127 / -32..-1       1      uint8 / int8      2
//        uint16 / int16         3      uint32 / int32    5      uint64 / int64   9
//   float                       9      (always float64)
//   str  (utf-8 n bytes)        n + 1 / 2 / 3 / 5   (fixstr <32, str8, str16, str32)
//   bytes                       n + 2 / 3 / 5       (bin8, bin16, bin32)
//   naive datetime              20, or 27 with microseconds   (ormsgpack writes RFC 3339 text)
//   date                        11
//   naive time                  9, or 16 with microseconds
//   tuple / list / dict         header 1 / 3 / 5 (<16, <65536, else) + elements
//
// Anything else (aware datetimes, Decimal, timedelta, numpy, subclasses, ints outside
// 64 bits, dicts with non-str keys) is sized by calling back into Python with the v1
// expression for that one value, so the answer — including the TypeError v1 raised for
// ints wider than 64 bits — is v1's by construction.
//
// Requires <datetime.h> with PyDateTime_IMPORT done (native.cpp does it at init).

#include <Python.h>
#include <datetime.h>

#include <cstdint>

namespace orso {

constexpr int64_t RECORD_HEADER_SIZE = 14;  // orso.row.RECORD_HEADER_SIZE

// Python callable `value -> int` set by orso.row at import (set_value_sizer).
inline PyObject* g_value_sizer = nullptr;

inline int64_t mp_int_width(int64_t v) noexcept {
    if (v >= 0) {
        if (v <= 127) return 1;
        if (v <= 0xFF) return 2;
        if (v <= 0xFFFF) return 3;
        if (v <= 0xFFFFFFFFLL) return 5;
        return 9;
    }
    if (v >= -32) return 1;
    if (v >= -128) return 2;
    if (v >= -32768) return 3;
    if (v >= -2147483648LL) return 5;
    return 9;
}

inline int64_t mp_str_width(uint64_t n) noexcept {
    if (n < 32) return static_cast<int64_t>(n) + 1;
    if (n <= 0xFF) return static_cast<int64_t>(n) + 2;
    if (n <= 0xFFFF) return static_cast<int64_t>(n) + 3;
    return static_cast<int64_t>(n) + 5;
}

inline int64_t mp_bin_width(uint64_t n) noexcept {
    if (n <= 0xFF) return static_cast<int64_t>(n) + 2;
    if (n <= 0xFFFF) return static_cast<int64_t>(n) + 3;
    return static_cast<int64_t>(n) + 5;
}

inline int64_t mp_container_header(uint64_t n) noexcept {
    if (n < 16) return 1;
    if (n <= 0xFFFF) return 3;
    return 5;
}

// Width of one value via the Python sizer. -1 with an exception set on failure.
inline int64_t python_value_width(PyObject* v) {
    if (g_value_sizer == nullptr) {
        PyErr_SetString(PyExc_RuntimeError, "orso._native: value sizer not installed (import orso.row first)");
        return -1;
    }
    PyObject* r = PyObject_CallOneArg(g_value_sizer, v);
    if (r == nullptr) return -1;
    const long long w = PyLong_AsLongLong(r);
    Py_DECREF(r);
    if (w == -1 && PyErr_Occurred()) return -1;
    return static_cast<int64_t>(w);
}

// Width of one Python value. -1 with an exception set on failure.
inline int64_t py_value_width(PyObject* v, int depth = 0) {
    if (v == Py_None || PyBool_Check(v)) return 1;
    PyTypeObject* t = Py_TYPE(v);
    if (t == &PyLong_Type) {
        int overflow = 0;
        const long long x = PyLong_AsLongLongAndOverflow(v, &overflow);
        if (overflow != 0) return python_value_width(v);  // uint64 range is 9; wider raises, as v1 did
        if (x == -1 && PyErr_Occurred()) return -1;
        return mp_int_width(x);
    }
    if (t == &PyFloat_Type) return 9;
    if (t == &PyUnicode_Type) {
        if (PyUnicode_IS_ASCII(v)) return mp_str_width(static_cast<uint64_t>(PyUnicode_GET_LENGTH(v)));
        Py_ssize_t n = 0;
        if (PyUnicode_AsUTF8AndSize(v, &n) == nullptr) {
            PyErr_Clear();  // lone surrogates: let the v1 expression decide (it raises)
            return python_value_width(v);
        }
        return mp_str_width(static_cast<uint64_t>(n));
    }
    if (t == &PyBytes_Type) return mp_bin_width(static_cast<uint64_t>(PyBytes_GET_SIZE(v)));
    if (PyDateTime_CheckExact(v)) {
        if (PyDateTime_DATE_GET_TZINFO(v) != Py_None) return python_value_width(v);
        return PyDateTime_DATE_GET_MICROSECOND(v) ? 27 : 20;
    }
    if (PyDate_CheckExact(v)) return 11;
    if (PyTime_CheckExact(v)) {
        if (PyDateTime_TIME_GET_TZINFO(v) != Py_None) return python_value_width(v);
        return PyDateTime_TIME_GET_MICROSECOND(v) ? 16 : 9;
    }
    if (depth < 32 && (t == &PyTuple_Type || t == &PyList_Type)) {
        const Py_ssize_t n = PySequence_Fast_GET_SIZE(v);
        PyObject** items = PySequence_Fast_ITEMS(v);
        int64_t total = mp_container_header(static_cast<uint64_t>(n));
        for (Py_ssize_t i = 0; i < n; ++i) {
            const int64_t w = py_value_width(items[i], depth + 1);
            if (w < 0) return -1;
            total += w;
        }
        return total;
    }
    if (depth < 32 && t == &PyDict_Type) {
        int64_t total = mp_container_header(static_cast<uint64_t>(PyDict_GET_SIZE(v)));
        Py_ssize_t pos = 0;
        PyObject *key, *value;
        while (PyDict_Next(v, &pos, &key, &value)) {
            if (!PyUnicode_CheckExact(key)) return python_value_width(v);  // v1 raises "Dict key must be str"
            const int64_t kw = py_value_width(key, depth + 1);
            if (kw < 0) return -1;
            const int64_t vw = py_value_width(value, depth + 1);
            if (vw < 0) return -1;
            total += kw + vw;
        }
        return total;
    }
    return python_value_width(v);
}

// nbytes() of one row: msgpack array header + elements + the 14-byte record header.
// -1 with an exception set on failure.
inline int64_t py_row_width(PyObject* row) {
    PyObject* seq = PySequence_Fast(row, "orso: a row must be a sequence");
    if (seq == nullptr) return -1;
    const Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
    PyObject** items = PySequence_Fast_ITEMS(seq);
    int64_t total = mp_container_header(static_cast<uint64_t>(n)) + RECORD_HEADER_SIZE;
    for (Py_ssize_t i = 0; i < n; ++i) {
        const int64_t w = py_value_width(items[i], 1);
        if (w < 0) {
            Py_DECREF(seq);
            return -1;
        }
        total += w;
    }
    Py_DECREF(seq);
    return total;
}

}  // namespace orso
