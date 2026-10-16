#pragma once
// orso/native/row.h — the C base of orso.Row (DESIGN.md §4.9).
//
// Row stays a tuple subclass: CPython reads tuple items directly, so there is no lazy
// tuple and no proxy. What moves into C is everything v1 did per row in Python:
// construction from a dict or a sequence, as_dict, as_map, values, get, keys, nbytes.
//
//     RowBase (this file)  ->  orso.row.Row (Python: as_bytes, as_json, from_bytes,
//                                            create_class)  ->  RowFactory subclasses
//
// Subclasses carry their field names as the class attribute `_fields` (a tuple of str)
// and `_field_map` ({name: index}). Reading a class attribute costs a type-dict lookup,
// so the pair is cached per type and the cache entry is trusted only while the type's
// version tag is unchanged — CPython bumps the tag whenever a class attribute changes.
//
// Rows are allocated with the subtype's tp_alloc and filled in place. On 3.14+ a tuple
// caches its hash in `ob_hash`, which must start at -1 ("not computed"); tp_alloc
// zero-fills, and a zero would be returned as every row's hash.

#include <Python.h>

#include <cstdint>

#include "width.h"

namespace orso {

inline PyObject* g_str_fields = nullptr;     // interned "_fields"
inline PyObject* g_str_field_map = nullptr;  // interned "_field_map"
inline PyTypeObject* g_row_base = nullptr;   // RowBase, set at module init

struct TypeFields {
    PyTypeObject* type = nullptr;  // identity only, never dereferenced
    unsigned int tag = 0;
    PyObject* fields = nullptr;     // strong: a tuple, or Py_None for the bare Row class
    PyObject* field_map = nullptr;  // strong: a dict, or nullptr when the class has none
};

#ifdef Py_GIL_DISABLED
inline thread_local TypeFields g_fields_cache[16];
#else
inline TypeFields g_fields_cache[16];
#endif

// The `_fields` / `_field_map` pair for a Row type. nullptr with an exception set if the
// class has no `_fields` at all.
inline TypeFields* type_fields(PyTypeObject* type) {
    TypeFields& e = g_fields_cache[(reinterpret_cast<uintptr_t>(type) >> 5) & 15];
    if (e.type == type && e.tag != 0 && e.tag == type->tp_version_tag) return &e;

    PyObject* fields = PyObject_GetAttr(reinterpret_cast<PyObject*>(type), g_str_fields);
    if (fields == nullptr) return nullptr;
    if (fields != Py_None && !PyTuple_CheckExact(fields)) {
        PyObject* as_tuple = PySequence_Tuple(fields);
        Py_DECREF(fields);
        if (as_tuple == nullptr) return nullptr;
        fields = as_tuple;
    }
    PyObject* field_map = PyObject_GetAttr(reinterpret_cast<PyObject*>(type), g_str_field_map);
    if (field_map == nullptr) PyErr_Clear();

    Py_XDECREF(e.fields);
    Py_XDECREF(e.field_map);
    e.fields = fields;
    e.field_map = field_map;
    e.type = type;
    e.tag = type->tp_version_tag;  // the lookups above assign one when the type allows it
    return &e;
}

// A new, empty instance of `type` (a tuple subclass) with room for n items. The items are
// NULL until the caller fills them; nothing outside the caller can see the row until then.
inline PyObject* row_alloc(PyTypeObject* type, Py_ssize_t n) {
    PyObject* op = type->tp_alloc(type, n);
    if (op == nullptr) return nullptr;
#if PY_VERSION_HEX >= 0x030E0000
    reinterpret_cast<PyTupleObject*>(op)->ob_hash = -1;
#endif
    return op;
}

// Fill a fresh row from a dict by field name; missing names become None.
inline PyObject* row_from_dict(PyTypeObject* type, PyObject* fields, PyObject* data) {
    const Py_ssize_t n = PyTuple_GET_SIZE(fields);
    PyObject* op = row_alloc(type, n);
    if (op == nullptr) return nullptr;
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* v = PyDict_GetItemWithError(data, PyTuple_GET_ITEM(fields, i));
        if (v == nullptr) {
            if (PyErr_Occurred()) {
                Py_DECREF(op);
                return nullptr;
            }
            v = Py_None;
        }
        Py_INCREF(v);
        PyTuple_SET_ITEM(op, i, v);
    }
    return op;
}

// Fill a fresh row from any iterable, positionally (v1: tuple.__new__(cls, data)).
inline PyObject* row_from_iterable(PyTypeObject* type, PyObject* data) {
    PyObject* seq = PySequence_Fast(data, "Row() argument must be a dict or an iterable");
    if (seq == nullptr) return nullptr;
    const Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
    PyObject** items = PySequence_Fast_ITEMS(seq);
    PyObject* op = row_alloc(type, n);
    if (op != nullptr) {
        for (Py_ssize_t i = 0; i < n; ++i) {
            Py_INCREF(items[i]);
            PyTuple_SET_ITEM(op, i, items[i]);
        }
    }
    Py_DECREF(seq);
    return op;
}

inline PyObject* RowBase_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    if (kwds != nullptr && PyDict_GET_SIZE(kwds) != 0) {
        PyErr_SetString(PyExc_TypeError, "Row() takes no keyword arguments");
        return nullptr;
    }
    if (PyTuple_GET_SIZE(args) != 1) {
        PyErr_Format(PyExc_TypeError, "Row() takes exactly one argument (%zd given)", PyTuple_GET_SIZE(args));
        return nullptr;
    }
    PyObject* data = PyTuple_GET_ITEM(args, 0);
    if (PyDict_Check(data)) {
        TypeFields* tf = type_fields(type);
        if (tf == nullptr) return nullptr;
        if (tf->fields == Py_None) {
            PyErr_SetString(PyExc_TypeError, "Row has no fields to map a dict onto; use Row.create_class(fields)");
            return nullptr;
        }
        return row_from_dict(type, tf->fields, data);
    }
    return row_from_iterable(type, data);
}

inline PyObject* Row_as_dict(PyObject* self, void*) {
    TypeFields* tf = type_fields(Py_TYPE(self));
    if (tf == nullptr) return nullptr;
    if (tf->fields == Py_None) {
        PyErr_SetString(PyExc_AttributeError, "Row has no field names");
        return nullptr;
    }
    const Py_ssize_t n = PyTuple_GET_SIZE(tf->fields);
    if (PyTuple_GET_SIZE(self) < n) {
        PyErr_SetString(PyExc_IndexError, "tuple index out of range");
        return nullptr;
    }
    PyObject* d = PyDict_New();
    if (d == nullptr) return nullptr;
    for (Py_ssize_t i = 0; i < n; ++i) {
        if (PyDict_SetItem(d, PyTuple_GET_ITEM(tf->fields, i), PyTuple_GET_ITEM(self, i)) < 0) {
            Py_DECREF(d);
            return nullptr;
        }
    }
    return d;
}

inline PyObject* Row_as_map(PyObject* self, void*) {
    TypeFields* tf = type_fields(Py_TYPE(self));
    if (tf == nullptr) return nullptr;
    if (tf->fields == Py_None) {
        PyErr_SetString(PyExc_AttributeError, "Row has no field names");
        return nullptr;
    }
    Py_ssize_t n = PyTuple_GET_SIZE(tf->fields);
    if (PyTuple_GET_SIZE(self) < n) n = PyTuple_GET_SIZE(self);  // zip() stops at the shorter
    PyObject* out = PyTuple_New(n);
    if (out == nullptr) return nullptr;
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* pair = PyTuple_Pack(2, PyTuple_GET_ITEM(tf->fields, i), PyTuple_GET_ITEM(self, i));
        if (pair == nullptr) {
            Py_DECREF(out);
            return nullptr;
        }
        PyTuple_SET_ITEM(out, i, pair);
    }
    return out;
}

inline PyObject* Row_values(PyObject* self, void*) {
    return PyTuple_GetSlice(self, 0, PyTuple_GET_SIZE(self));  // an exact tuple
}

inline PyObject* Row_keys(PyObject* self, PyObject*) {
    TypeFields* tf = type_fields(Py_TYPE(self));
    if (tf == nullptr) return nullptr;
    Py_INCREF(tf->fields);
    return tf->fields;
}

inline PyObject* Row_get(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs < 1 || nargs > 2) {
        PyErr_Format(PyExc_TypeError, "get() takes 1 or 2 arguments (%zd given)", nargs);
        return nullptr;
    }
    PyObject* item = args[0];
    PyObject* dflt = nargs == 2 ? args[1] : Py_None;
    TypeFields* tf = type_fields(Py_TYPE(self));
    if (tf == nullptr) return nullptr;
    Py_ssize_t idx = -1;
    if (tf->field_map != nullptr && PyDict_Check(tf->field_map)) {
        PyObject* found = PyDict_GetItemWithError(tf->field_map, item);
        if (found == nullptr) {
            if (PyErr_Occurred()) return nullptr;
        } else {
            idx = PyLong_AsSsize_t(found);
            if (idx == -1 && PyErr_Occurred()) return nullptr;
        }
    } else if (tf->fields != Py_None) {
        idx = PySequence_Index(tf->fields, item);
        if (idx < 0) {
            if (!PyErr_ExceptionMatches(PyExc_ValueError)) return nullptr;
            PyErr_Clear();
        }
    } else {
        PyErr_SetString(PyExc_AttributeError, "Row has no field names");
        return nullptr;
    }
    if (idx < 0) {
        Py_INCREF(dflt);
        return dflt;
    }
    if (idx >= PyTuple_GET_SIZE(self)) {
        PyErr_SetString(PyExc_IndexError, "tuple index out of range");
        return nullptr;
    }
    PyObject* v = PyTuple_GET_ITEM(self, idx);
    Py_INCREF(v);
    return v;
}

inline PyObject* Row_nbytes(PyObject* self, PyObject*) {
    const int64_t w = py_row_width(self);
    if (w < 0) return nullptr;
    return PyLong_FromLongLong(w);
}

inline PyGetSetDef Row_getset[] = {
    {"as_dict", Row_as_dict, nullptr, "The row as {field: value}, in field order.", nullptr},
    {"as_map", Row_as_map, nullptr, "The row as ((field, value), ...).", nullptr},
    {"values", Row_values, nullptr, "The row's values as a plain tuple.", nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

inline PyMethodDef Row_methods[] = {
    {"get", reinterpret_cast<PyCFunction>(reinterpret_cast<void (*)(void)>(Row_get)), METH_FASTCALL,
     "get(name, default=None): the value of field `name`, or `default`."},
    {"keys", Row_keys, METH_NOARGS, "The field names."},
    {"nbytes", Row_nbytes, METH_NOARGS,
     "Approximate serialised size: msgpack length of the values plus the 14-byte record header."},
    {nullptr, nullptr, 0, nullptr},
};

inline PyType_Slot Row_slots[] = {
    {Py_tp_new, reinterpret_cast<void*>(RowBase_new)},
    {Py_tp_getset, Row_getset},
    {Py_tp_methods, Row_methods},
    {Py_tp_doc, const_cast<char*>("Native base of orso.Row: a tuple with named fields.")},
    {0, nullptr},
};

// basicsize and itemsize of 0 inherit tuple's, so instances are plain tuple layouts.
inline PyType_Spec Row_spec = {
    "orso._native.RowBase", 0, 0, Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, Row_slots,
};

inline PyTypeObject* make_row_base() {
    g_str_fields = PyUnicode_InternFromString("_fields");
    g_str_field_map = PyUnicode_InternFromString("_field_map");
    if (g_str_fields == nullptr || g_str_field_map == nullptr) return nullptr;
    PyObject* t = PyType_FromSpecWithBases(&Row_spec, reinterpret_cast<PyObject*>(&PyTuple_Type));
    if (t == nullptr) return nullptr;
    g_row_base = reinterpret_cast<PyTypeObject*>(t);
    return g_row_base;
}

// DataFrame(dictionaries) in one pass (v1: `[factory([d.get(k) for k in keys]) for d in ...]`).
// `keys` are the first dict's original key objects; exact dicts are read directly and
// anything else through its own .get(key, None), as v1 did.
inline PyObject* rows_from_dicts(PyTypeObject* type, PyObject* keys, PyObject* first, PyObject* rest) {
    PyObject* out = PyList_New(0);
    if (out == nullptr) return nullptr;
    const Py_ssize_t n = PyTuple_GET_SIZE(keys);
    PyObject* get_name = nullptr;
    PyObject* item = first;
    Py_INCREF(item);
    while (item != nullptr) {
        PyObject* op = row_alloc(type, n);
        if (op == nullptr) goto fail;
        for (Py_ssize_t i = 0; i < n; ++i) {
            PyObject* key = PyTuple_GET_ITEM(keys, i);
            PyObject* v;
            if (PyDict_CheckExact(item)) {
                v = PyDict_GetItemWithError(item, key);
                if (v == nullptr) {
                    if (PyErr_Occurred()) {
                        Py_DECREF(op);
                        goto fail;
                    }
                    v = Py_None;
                }
                Py_INCREF(v);
            } else {
                if (get_name == nullptr && (get_name = PyUnicode_InternFromString("get")) == nullptr) {
                    Py_DECREF(op);
                    goto fail;
                }
                PyObject* call_args[3] = {item, key, Py_None};
                v = PyObject_VectorcallMethod(get_name, call_args, 3, nullptr);
                if (v == nullptr) {
                    Py_DECREF(op);
                    goto fail;
                }
            }
            PyTuple_SET_ITEM(op, i, v);
        }
        if (PyList_Append(out, op) < 0) {
            Py_DECREF(op);
            goto fail;
        }
        Py_DECREF(op);
        Py_DECREF(item);
        item = PyIter_Next(rest);
    }
    if (PyErr_Occurred()) goto fail_noitem;
    Py_XDECREF(get_name);
    return out;

fail:
    Py_XDECREF(item);
fail_noitem:
    Py_XDECREF(get_name);
    Py_DECREF(out);
    return nullptr;
}

}  // namespace orso
