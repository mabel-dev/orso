#pragma once
// orso/native/columns.h — the last helpers v1 had in Cython (orso/compute/compiled.pyx),
// so that module can go (DESIGN.md §10, phase 6).
//
//   collect_into    DataFrame.collect's fill: values written straight into a numpy object
//                   array's buffer, as v1's typed memoryview did. numpy creates the array
//                   (orso never links numpy); this only needs its address and shape.
//   extract_columns rows to column lists (v1's extract_columns_to_lists).
//   column_widths   display widths per column (v1's calculate_column_widths).

#include <Python.h>

#include <cstdint>
#include <vector>

#include "store.h"

namespace orso {

// v1's bounds rule: every index must lie inside the first row.
inline bool check_columns(const std::vector<Py_ssize_t>& cols, Py_ssize_t row_width) {
    for (Py_ssize_t c : cols) {
        if (c < 0 || c >= row_width) {
            PyErr_Format(PyExc_IndexError, "Column index out of bounds (0 <= %zd < %zd)", c, row_width);
            return false;
        }
    }
    return true;
}

// Store `v` (borrowed) at out[k], releasing what was there (numpy fills with None).
inline void put_object_at(PyObject** out, size_t k, PyObject* v) {
    Py_INCREF(v);
    PyObject* old = out[k];
    out[k] = v;
    Py_XDECREF(old);
}

// Fill a C-contiguous (ncols, n) object array from Python rows. 0, or -1 with an exception.
inline int collect_rows_into(PyObject** out, PyObject* rows, const std::vector<Py_ssize_t>& cols, Py_ssize_t n) {
    if (n == 0 || cols.empty()) return 0;
    PyObject* first = PySequence_Fast(PyList_GET_ITEM(rows, 0), "orso: a row must be a sequence");
    if (first == nullptr) return -1;
    const Py_ssize_t width = PySequence_Fast_GET_SIZE(first);
    Py_DECREF(first);
    if (!check_columns(cols, width)) return -1;
    const size_t stride = static_cast<size_t>(n);
    const size_t ncols = cols.size();
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* row = PyList_GET_ITEM(rows, i);
        // rows are tuples (Row is one) or lists: read their items in place, as v1's
        // `<tuple>rows[i]` did; anything else goes through the sequence protocol
        PyObject* seq = nullptr;
        PyObject** items;
        Py_ssize_t rw;
        if (PyTuple_Check(row)) {
            items = reinterpret_cast<PyTupleObject*>(row)->ob_item;
            rw = Py_SIZE(row);
        } else if (PyList_Check(row)) {
            items = reinterpret_cast<PyListObject*>(row)->ob_item;
            rw = Py_SIZE(row);
        } else {
            seq = PySequence_Fast(row, "orso: a row must be a sequence");
            if (seq == nullptr) return -1;
            items = PySequence_Fast_ITEMS(seq);
            rw = PySequence_Fast_GET_SIZE(seq);
        }
        if (ncols == 1) {  // the common case, as v1 special-cased it
            const Py_ssize_t j = cols[0];
            if (j >= rw) {
                Py_XDECREF(seq);
                PyErr_SetString(PyExc_IndexError, "tuple index out of range");
                return -1;
            }
            put_object_at(out, static_cast<size_t>(i), items[j]);
        } else {
            for (size_t c = 0; c < ncols; ++c) {
                const Py_ssize_t j = cols[c];
                if (j >= rw) {
                    Py_XDECREF(seq);
                    PyErr_SetString(PyExc_IndexError, "tuple index out of range");
                    return -1;
                }
                put_object_at(out, c * stride + static_cast<size_t>(i), items[j]);
            }
        }
        Py_XDECREF(seq);
    }
    return 0;
}

// The same fill from a store frame's records, decoding only the requested fields.
inline int collect_frame_into(PyObject** out, const Store& st, uint64_t offset, const std::vector<Py_ssize_t>& cols,
                              Py_ssize_t n) {
    if (n == 0 || cols.empty()) return 0;
    if (!check_columns(cols, static_cast<Py_ssize_t>(st.nfields))) return -1;
    for (Py_ssize_t i = 0; i < n; ++i) {
        const uint8_t* rec = st.record(offset + static_cast<uint64_t>(i));
        for (size_t c = 0; c < cols.size(); ++c) {
            const uint32_t j = static_cast<uint32_t>(cols[c]);
            PyObject* v = decode_value(st, Store::tag_at(rec, j), st.slot_at(rec, j));
            if (v == nullptr) return -1;
            const size_t k = c * static_cast<size_t>(n) + static_cast<size_t>(i);
            PyObject* old = out[k];
            out[k] = v;  // steal the new reference
            Py_XDECREF(old);
        }
    }
    return 0;
}

// v1's extract_columns_to_lists(rows, limit): [] for no rows, else one list per column
// of the first row's width.
inline PyObject* extract_columns(PyObject* rows, Py_ssize_t limit) {
    Py_ssize_t n = PyList_GET_SIZE(rows);
    if (n == 0) return PyList_New(0);
    PyObject* first = PySequence_Fast(PyList_GET_ITEM(rows, 0), "orso: a row must be a sequence");
    if (first == nullptr) return nullptr;
    const Py_ssize_t width = PySequence_Fast_GET_SIZE(first);
    Py_DECREF(first);
    if (limit >= 0 && limit < n) n = limit;
    PyObject* out = PyList_New(width);
    if (out == nullptr) return nullptr;
    for (Py_ssize_t c = 0; c < width; ++c) {
        PyObject* col = PyList_New(n);
        if (col == nullptr) {
            Py_DECREF(out);
            return nullptr;
        }
        PyList_SET_ITEM(out, c, col);
    }
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* seq = PySequence_Fast(PyList_GET_ITEM(rows, i), "orso: a row must be a sequence");
        if (seq == nullptr || PySequence_Fast_GET_SIZE(seq) < width) {
            if (seq != nullptr) {
                Py_DECREF(seq);
                PyErr_SetString(PyExc_IndexError, "tuple index out of range");
            }
            Py_DECREF(out);
            return nullptr;
        }
        PyObject** items = PySequence_Fast_ITEMS(seq);
        for (Py_ssize_t c = 0; c < width; ++c) {
            Py_INCREF(items[c]);
            PyList_SET_ITEM(PyList_GET_ITEM(out, c), i, items[c]);
        }
        Py_DECREF(seq);
    }
    return out;
}

// v1's calculate_column_widths(rows): per column, the longest str(value) in code points,
// at least 4; None does not count.
inline PyObject* column_widths(PyObject* rows) {
    const Py_ssize_t n = PyList_GET_SIZE(rows);
    if (n == 0) return PyList_New(0);
    PyObject* first = PySequence_Fast(PyList_GET_ITEM(rows, 0), "orso: a row must be a sequence");
    if (first == nullptr) return nullptr;
    const Py_ssize_t width = PySequence_Fast_GET_SIZE(first);
    Py_DECREF(first);
    std::vector<Py_ssize_t> widths(static_cast<size_t>(width), 4);
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* seq = PySequence_Fast(PyList_GET_ITEM(rows, i), "orso: a row must be a sequence");
        if (seq == nullptr) return nullptr;
        if (PySequence_Fast_GET_SIZE(seq) < width) {
            Py_DECREF(seq);
            PyErr_SetString(PyExc_IndexError, "tuple index out of range");
            return nullptr;
        }
        PyObject** items = PySequence_Fast_ITEMS(seq);
        for (Py_ssize_t c = 0; c < width; ++c) {
            PyObject* v = items[c];
            if (v == Py_None) continue;
            Py_ssize_t w;
            if (PyUnicode_CheckExact(v)) {
                w = PyUnicode_GET_LENGTH(v);
            } else {
                PyObject* s = PyObject_Str(v);
                if (s == nullptr) {
                    Py_DECREF(seq);
                    return nullptr;
                }
                w = PyUnicode_GET_LENGTH(s);
                Py_DECREF(s);
            }
            if (w > widths[static_cast<size_t>(c)]) widths[static_cast<size_t>(c)] = w;
        }
        Py_DECREF(seq);
    }
    PyObject* out = PyList_New(width);
    if (out == nullptr) return nullptr;
    for (Py_ssize_t c = 0; c < width; ++c) PyList_SET_ITEM(out, c, PyLong_FromSsize_t(widths[static_cast<size_t>(c)]));
    return out;
}

}  // namespace orso
