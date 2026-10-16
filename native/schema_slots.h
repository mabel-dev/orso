#pragma once
// orso/native/schema_slots.h — storage and cache invalidation for FlatColumn and
// RelationSchema (DESIGN.md §4.10, phase 4), and the native ISO-8601 parser.
//
// TrackedSlot is a data descriptor over a hidden __slots__ member. Reads cost what a slot
// read costs; a write to a field that cached lookups depend on (a column's name, aliases,
// type, nullability, precision, scale, element type or child fields; a schema's columns)
// bumps one process-wide counter, the schema epoch. RelationSchema keeps its name→column
// dict, its name tuples and its Arrow schema with the epoch they were built at, and
// rebuilds them when the epoch has moved. That is what makes caching safe while the
// PyPI opteryx binder renames, re-aliases and retypes columns in place.
//
// An unset slot reads as the field's default, which is what v1's dataclass gave through
// class attributes: FlatColumn.to_flatcolumn() builds columns with object.__new__ and
// never sets `disposition` or `length`.

#include <Python.h>
#include <datetime.h>

#include <atomic>
#include <cstdint>

#include "core/iso_datetime.h"
#include "logical_type.h"  // us_epoch_to_parts

namespace orso {

// Atomic so free-threaded builds can bump it from any thread; on GIL builds the relaxed
// increment costs what a plain one does.
inline std::atomic<uint64_t> g_schema_epoch{1};

struct TrackedSlotObject {
    PyObject_HEAD
    PyObject* member;   // the hidden slot's member descriptor
    PyObject* dflt;     // the field default, or nullptr when there is none
    PyObject* name;     // the public field name, for error messages
    int bump;           // writes invalidate schema caches
};

inline PyObject* TrackedSlot_get(PyObject* self, PyObject* obj, PyObject* type) {
    auto* t = reinterpret_cast<TrackedSlotObject*>(self);
    if (obj == nullptr || obj == Py_None) {
        Py_INCREF(self);
        return self;
    }
    PyObject* v = Py_TYPE(t->member)->tp_descr_get(t->member, obj, type);
    if (v == nullptr && PyErr_ExceptionMatches(PyExc_AttributeError)) {
        PyErr_Clear();
        if (t->dflt != nullptr) {
            Py_INCREF(t->dflt);
            return t->dflt;
        }
        PyErr_Format(PyExc_AttributeError, "'%.100s' object has no attribute '%U'", Py_TYPE(obj)->tp_name, t->name);
    }
    return v;
}

inline int TrackedSlot_set(PyObject* self, PyObject* obj, PyObject* value) {
    auto* t = reinterpret_cast<TrackedSlotObject*>(self);
    const int r = Py_TYPE(t->member)->tp_descr_set(t->member, obj, value);
    if (r == 0 && t->bump) g_schema_epoch.fetch_add(1, std::memory_order_relaxed);
    return r;
}

inline PyObject* TrackedSlot_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    static const char* kwlist[] = {"member", "name", "default", "bump", nullptr};
    PyObject* member = nullptr;
    PyObject* name = nullptr;
    PyObject* dflt = nullptr;
    int bump = 0;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OU|Op", const_cast<char**>(kwlist), &member, &name, &dflt, &bump)) {
        return nullptr;
    }
    if (Py_TYPE(member)->tp_descr_get == nullptr || Py_TYPE(member)->tp_descr_set == nullptr) {
        PyErr_SetString(PyExc_TypeError, "TrackedSlot wraps a data descriptor (a __slots__ member)");
        return nullptr;
    }
    auto* t = reinterpret_cast<TrackedSlotObject*>(type->tp_alloc(type, 0));
    if (t == nullptr) return nullptr;
    Py_INCREF(member);
    t->member = member;
    Py_XINCREF(dflt);
    t->dflt = dflt;
    Py_INCREF(name);
    t->name = name;
    t->bump = bump;
    return reinterpret_cast<PyObject*>(t);
}

inline void TrackedSlot_dealloc(PyObject* self) {
    auto* t = reinterpret_cast<TrackedSlotObject*>(self);
    PyTypeObject* tp = Py_TYPE(self);
    Py_XDECREF(t->member);
    Py_XDECREF(t->dflt);
    Py_XDECREF(t->name);
    tp->tp_free(self);
    Py_DECREF(tp);
}

inline PyType_Slot TrackedSlot_slots[] = {
    {Py_tp_new, reinterpret_cast<void*>(TrackedSlot_new)},
    {Py_tp_dealloc, reinterpret_cast<void*>(TrackedSlot_dealloc)},
    {Py_tp_descr_get, reinterpret_cast<void*>(TrackedSlot_get)},
    {Py_tp_descr_set, reinterpret_cast<void*>(TrackedSlot_set)},
    {Py_tp_doc, const_cast<char*>("Slot-backed field whose writes can invalidate schema caches.")},
    {0, nullptr},
};

inline PyType_Spec TrackedSlot_spec = {
    "orso._native.TrackedSlot", sizeof(TrackedSlotObject), 0, Py_TPFLAGS_DEFAULT, TrackedSlot_slots,
};

// ----- ISO-8601 text -> datetime ------------------------------------------------------

inline bool civil_date_valid(int y, int m, int d) noexcept {
    static const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (y < 1 || y > 9999 || m < 1 || m > 12 || d < 1) return false;
    int dim = days_in_month[m - 1];
    if (m == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) dim = 29;
    return d <= dim;
}

// A naive datetime for ISO-8601 text, or None when draken's parser refuses it. An
// offset is applied, so the result is UTC (DESIGN.md §8 fix 11); fractional seconds are
// kept to the microsecond. The caller falls back to v1's parser for anything refused,
// so nothing v1 accepted stops parsing.
inline PyObject* parse_iso_text(PyObject* v) {
    const char* p = nullptr;
    Py_ssize_t n = 0;
    if (PyUnicode_Check(v)) {
        p = PyUnicode_AsUTF8AndSize(v, &n);
        if (p == nullptr) {
            PyErr_Clear();
            Py_RETURN_NONE;
        }
    } else if (PyBytes_Check(v)) {
        p = PyBytes_AS_STRING(v);
        n = PyBytes_GET_SIZE(v);
    } else {
        Py_RETURN_NONE;
    }
    if (n <= 0 || n > 64) Py_RETURN_NONE;
    int y, mo, d, h, mi, s, us, offset = 0;
    if (!draken::iso_datetime::parse_iso_timestamp(reinterpret_cast<const uint8_t*>(p), static_cast<uint32_t>(n),
                                                   &y, &mo, &d, &h, &mi, &s, &us, &offset)) {
        Py_RETURN_NONE;
    }
    if (!civil_date_valid(y, mo, d)) Py_RETURN_NONE;
    if (offset != 0) {
        const int64_t t = draken::iso_datetime::civil_to_micros(y, mo, d, h, mi, s, us) -
                          static_cast<int64_t>(offset) * 1000000LL;
        us_epoch_to_parts(t, y, mo, d, h, mi, s, us);
        if (y < 1 || y > 9999) Py_RETURN_NONE;
    }
    return PyDateTime_FromDateAndTime(y, mo, d, h, mi, s, us);
}

}  // namespace orso
