#pragma once
// orso/native/store.h — the row-major record store (DESIGN.md §4.1–4.7).
//
//   Plan   what a frame accepts: the Row type, the field names, and for frames with a
//          RelationSchema the compiled validation plan (name, nullable, Python type,
//          JSONB) that v1 interpreted in Python on every append.
//   Store  the records: fixed-stride [tags | 16-byte slots], one arena for string bytes
//          longer than 12, and a strong reference for every value held as an object.
//   Frame  a window over a Store. The owner's window grows with appends; views (slice,
//          head, tail, to_batches) have a fixed window and share the store.
//
// Encoding is exact-type only: a value is turned into a draken slot only when decoding
// that slot gives back an equal value of the same type. Everything else (subclasses,
// aware datetimes, Decimal, containers, ints outside 64 bits) is held as the object
// itself under the orso-private tag TAG_OBJECT, which is exactly what v1 stored. So a
// round trip through the store is faithful by construction, and the per-value tag means
// heterogeneous rows need no schema.

#include <Python.h>
#include <datetime.h>

#include <cstdint>
#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

#include <nanobind/nanobind.h>

#include "record.h"
#include "row.h"
#include "width.h"
#include "core/append_buffer.h"
#include "core/interval_slot.h"
#include "core/iso_datetime.h"
#include "core/string_slot.h"
#include "logical_type.h"  // us_epoch_to_parts

namespace orso {

namespace nb = nanobind;

constexpr uint8_t TAG_OBJECT = 0xFF;   // orso-private: a PyObject* held by reference; never leaves orso
constexpr uint8_t PARAM_ASCII = 0x01;  // on DRAKEN_VARCHAR: the payload is pure ASCII

constexpr int64_t US_PER_SECOND = 1000000LL;
constexpr int64_t US_PER_DAY = 86400LL * US_PER_SECOND;
constexpr int64_t MAX_DELTA_DAYS = 106751990;  // |timedelta| whose microseconds fit int64

[[noreturn]] inline void raise_python() { throw nb::python_error(); }

// ---------------------------------------------------------------------------------
// Plan
// ---------------------------------------------------------------------------------

struct PlanColumn {
    nb::object name;               // interned str
    bool nullable = true;
    PyTypeObject* expected = nullptr;  // borrowed from `expected_ref`; nullptr = any type
    nb::object expected_ref;
    bool jsonb = false;
};

inline bool is_jsonb_container(PyObject* v) {
    // v1: isinstance(value, (dict, list, tuple, set))
    return PyDict_Check(v) || PyList_Check(v) || PyTuple_Check(v) || PySet_Check(v);
}

// Owns up to `n` new references and releases them on scope exit.
struct ValueBuffer {
    PyObject* inline_vals[32];
    std::vector<PyObject*> heap;
    PyObject** vals;
    Py_ssize_t filled = 0;
    explicit ValueBuffer(Py_ssize_t n) : vals(inline_vals) {
        if (n > 32) {
            heap.resize(static_cast<size_t>(n), nullptr);
            vals = heap.data();
        }
    }
    void push(PyObject* owned) { vals[filled++] = owned; }
    void release() {
        for (Py_ssize_t i = 0; i < filled; ++i) Py_XDECREF(vals[i]);
        filled = 0;
    }
    ~ValueBuffer() { release(); }
    ValueBuffer(const ValueBuffer&) = delete;
    ValueBuffer& operator=(const ValueBuffer&) = delete;
};

class Plan {
public:
    nb::object row_type_ref;
    PyTypeObject* row_type = nullptr;
    nb::object fields;  // tuple of str, column order
    Py_ssize_t nfields = 0;
    bool typed = false;
    bool has_jsonb = false;
    std::vector<PlanColumn> columns;
    nb::object validate;  // RelationSchema.validate, for v1's error reporting
    nb::object dumps;     // orso._json.dumps_bytes, for JSONB values

    Plan(nb::handle row_type_h, nb::handle columns_h, nb::handle validate_h, nb::handle dumps_h) {
        if (!PyType_Check(row_type_h.ptr()) ||
            !PyType_IsSubtype(reinterpret_cast<PyTypeObject*>(row_type_h.ptr()), g_row_base)) {
            throw nb::type_error("Plan: row_type must be a Row subclass");
        }
        row_type_ref = nb::borrow(row_type_h);
        row_type = reinterpret_cast<PyTypeObject*>(row_type_h.ptr());
        TypeFields* tf = type_fields(row_type);
        if (tf == nullptr) raise_python();
        if (tf->fields == Py_None) throw nb::type_error("Plan: row_type has no fields");
        fields = nb::borrow(tf->fields);
        nfields = PyTuple_GET_SIZE(tf->fields);

        if (!columns_h.is_none()) {
            typed = true;
            for (nb::handle spec : columns_h) {
                nb::tuple t = nb::cast<nb::tuple>(spec);
                if (t.size() != 4) throw nb::value_error("Plan: column spec is (name, nullable, type, jsonb)");
                PlanColumn c;
                PyObject* name = t[0].ptr();
                if (!PyUnicode_Check(name)) throw nb::type_error("Plan: column name must be str");
                Py_INCREF(name);
                PyUnicode_InternInPlace(&name);
                c.name = nb::steal(name);
                c.nullable = PyObject_IsTrue(t[1].ptr()) == 1;
                if (!t[2].is_none()) {
                    if (!PyType_Check(t[2].ptr())) throw nb::type_error("Plan: column type must be a type or None");
                    c.expected_ref = nb::borrow(t[2]);
                    c.expected = reinterpret_cast<PyTypeObject*>(t[2].ptr());
                }
                c.jsonb = PyObject_IsTrue(t[3].ptr()) == 1;
                has_jsonb |= c.jsonb;
                columns.push_back(std::move(c));
            }
            if (static_cast<Py_ssize_t>(columns.size()) != nfields) {
                throw nb::value_error("Plan: column count does not match the row type's fields");
            }
            validate = nb::borrow(validate_h);
            dumps = nb::borrow(dumps_h);
        }
    }

    // The value to store for a JSONB column: containers become JSON bytes (v1 semantics).
    // Returns a new reference, or nullptr with an exception set.
    PyObject* jsonb_value(const PlanColumn& c, PyObject* v) const {
        if (c.jsonb && is_jsonb_container(v)) {
            if (!dumps.is_valid() || dumps.is_none()) {
                PyErr_SetString(PyExc_RuntimeError, "orso: JSONB column without a serialiser");
                return nullptr;
            }
            return PyObject_CallOneArg(dumps.ptr(), v);
        }
        Py_INCREF(v);
        return v;
    }

    // v1's fast path: an exact match of keys, nullability and isinstance(). 1 = values in
    // `buf`, 0 = not a fast-path record (caller falls back), -1 = error.
    int gather_typed_fast(PyObject* d, ValueBuffer& buf) const {
        if (!PyDict_Check(d) || PyDict_GET_SIZE(d) != nfields) return 0;
        for (Py_ssize_t i = 0; i < nfields; ++i) {
            const PlanColumn& c = columns[static_cast<size_t>(i)];
            PyObject* raw = PyDict_GetItemWithError(d, c.name.ptr());
            if (raw == nullptr) {
                buf.release();
                return PyErr_Occurred() ? -1 : 0;
            }
            PyObject* v = jsonb_value(c, raw);
            if (v == nullptr) {
                buf.release();
                return -1;
            }
            buf.push(v);
            if (v == Py_None) {
                if (!c.nullable) {
                    buf.release();
                    return 0;
                }
            } else if (c.expected != nullptr && !PyObject_TypeCheck(v, c.expected)) {
                buf.release();
                return 0;
            }
        }
        return 1;
    }

    // After RelationSchema.validate() accepted the record: values by name, no checks.
    int gather_by_name(PyObject* record, ValueBuffer& buf) const {
        for (Py_ssize_t i = 0; i < nfields; ++i) {
            const PlanColumn& c = columns[static_cast<size_t>(i)];
            PyObject* raw = PyObject_GetItem(record, c.name.ptr());
            if (raw == nullptr) {
                buf.release();
                return -1;
            }
            PyObject* v = jsonb_value(c, raw);
            Py_DECREF(raw);
            if (v == nullptr) {
                buf.release();
                return -1;
            }
            buf.push(v);
        }
        return 1;
    }

    // v1 converted JSONB containers before validating, so validation saw bytes. Do the
    // same so the errors raised are the ones v1 raised. Returns a new reference.
    PyObject* jsonb_normalised(PyObject* entry) const {
        if (!has_jsonb || !PyDict_Check(entry)) {
            Py_INCREF(entry);
            return entry;
        }
        PyObject* copy = nullptr;
        for (const PlanColumn& c : columns) {
            if (!c.jsonb) continue;
            PyObject* v = PyDict_GetItemWithError(entry, c.name.ptr());
            if (v == nullptr) {
                if (PyErr_Occurred()) {
                    Py_XDECREF(copy);
                    return nullptr;
                }
                continue;
            }
            if (!is_jsonb_container(v)) continue;
            if (copy == nullptr && (copy = PyDict_Copy(entry)) == nullptr) return nullptr;
            PyObject* enc = jsonb_value(c, v);
            if (enc == nullptr || PyDict_SetItem(copy, c.name.ptr(), enc) < 0) {
                Py_XDECREF(enc);
                Py_DECREF(copy);
                return nullptr;
            }
            Py_DECREF(enc);
        }
        if (copy != nullptr) return copy;
        Py_INCREF(entry);
        return entry;
    }

    // List-of-names frames: a dict by field name (missing -> None), anything else
    // positionally with exactly one value per field. The record store needs the width;
    // v1 never checked it, and a short row only failed later, inside arrow().
    int gather_untyped(PyObject* entry, ValueBuffer& buf) const {
        if (PyDict_Check(entry)) {
            for (Py_ssize_t i = 0; i < nfields; ++i) {
                PyObject* v = PyDict_GetItemWithError(entry, PyTuple_GET_ITEM(fields.ptr(), i));
                if (v == nullptr) {
                    if (PyErr_Occurred()) {
                        buf.release();
                        return -1;
                    }
                    v = Py_None;
                }
                Py_INCREF(v);
                buf.push(v);
            }
            return 1;
        }
        PyObject* seq = PySequence_Fast(entry, "orso: append() takes a dict or a sequence");
        if (seq == nullptr) return -1;
        const Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
        if (n != nfields) {
            Py_DECREF(seq);
            PyErr_Format(PyExc_ValueError, "orso: row has %zd values but the frame has %zd fields", n, nfields);
            return -1;
        }
        PyObject** items = PySequence_Fast_ITEMS(seq);
        for (Py_ssize_t i = 0; i < n; ++i) {
            Py_INCREF(items[i]);
            buf.push(items[i]);
        }
        Py_DECREF(seq);
        return 1;
    }

    // Values for one appended record, in column order, with v1's validation semantics.
    int gather(PyObject* entry, ValueBuffer& buf) const {
        if (!typed) return gather_untyped(entry, buf);
        const int fast = gather_typed_fast(entry, buf);
        if (fast != 0) return fast;
        // Not a clean record: let RelationSchema.validate() raise v1's exact error. If it
        // accepts the record (non-dict mappings, duplicate column names), take the values
        // by name as v1 did.
        PyObject* record = jsonb_normalised(entry);
        if (record == nullptr) return -1;
        PyObject* ok = PyObject_CallOneArg(validate.ptr(), record);
        if (ok == nullptr) {
            Py_DECREF(record);
            return -1;
        }
        Py_DECREF(ok);
        const int r = gather_by_name(record, buf);
        Py_DECREF(record);
        return r;
    }

    // A validated Row object, for frames whose rows are Python objects (v1 list frames).
    nb::object build_row(nb::handle entry) const {
        ValueBuffer buf(nfields);
        if (gather(entry.ptr(), buf) < 0) raise_python();
        PyObject* op = row_alloc(row_type, buf.filled);
        if (op == nullptr) raise_python();
        for (Py_ssize_t i = 0; i < buf.filled; ++i) PyTuple_SET_ITEM(op, i, buf.vals[i]);  // steal
        buf.filled = 0;
        return nb::steal(op);
    }
};

// ---------------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------------

struct Store {
    uint32_t nfields;
    uint32_t stride;
    uint32_t slot_offset;
    draken::AppendBuffer<uint8_t> records;
    draken::AppendBuffer<uint8_t> arena;
    std::vector<PyObject*> objects;  // one strong reference per TAG_OBJECT slot
    uint64_t rows = 0;

    explicit Store(uint32_t n) : nfields(n), stride(record_stride(n)), slot_offset(record_slot_offset(n)) {}
    ~Store() {
        for (PyObject* o : objects) Py_DECREF(o);  // destroyed from a Python dealloc: GIL held
    }
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    uint8_t* record(uint64_t i) noexcept { return records.data() + i * stride; }
    const uint8_t* record(uint64_t i) const noexcept { return records.data() + i * stride; }
    static uint16_t tag_at(const uint8_t* rec, uint32_t j) noexcept {
        uint16_t t;
        std::memcpy(&t, rec + 2 * j, sizeof t);
        return t;
    }
    const uint8_t* slot_at(const uint8_t* rec, uint32_t j) const noexcept { return rec + slot_offset + SLOT_BYTES * j; }
};

// ----- encode: Python value -> tag + slot (the only unboxing site) -----------------

inline void put_object(Store& st, PyObject* v, uint16_t& tag, uint8_t* slot) {
    st.objects.push_back(v);  // may throw before the INCREF below: nothing to undo
    Py_INCREF(v);
    std::memcpy(slot, &v, sizeof v);
    tag = TAG_OBJECT;  // not a DrakenType: written as a raw tag, never cast to the enum
}

inline bool put_string(Store& st, const char* p, Py_ssize_t n, uint8_t* slot) {
    if (n < 0 || static_cast<uint64_t>(n) > 0xFFFFFFFFull) return false;
    const uint32_t len = static_cast<uint32_t>(n);
    auto* s = reinterpret_cast<DrakenStringSlot*>(slot);
    if (len <= STR_INLINE_MAX) {
        str_init_inline(s, reinterpret_cast<const uint8_t*>(p), len);
        return true;
    }
    const uint64_t off = st.arena.size();
    if (off + len > 0xFFFFFFFFull) return false;  // draken slots address 4 GB of arena
    st.arena.append(reinterpret_cast<const uint8_t*>(p), len);
    str_init_extern(s, reinterpret_cast<const uint8_t*>(p), len, static_cast<uint32_t>(off));
    return true;
}

// 0 on success, -1 with an exception set.
inline int encode_value(Store& st, PyObject* v, uint16_t& tag, uint8_t* slot) {
    std::memset(slot, 0, SLOT_BYTES);
    if (v == Py_None) {
        tag = make_tag(DRAKEN_NULL);
        return 0;
    }
    PyTypeObject* t = Py_TYPE(v);
    if (t == &PyBool_Type) {
        slot[0] = v == Py_True;
        tag = make_tag(DRAKEN_BOOL);
        return 0;
    }
    if (t == &PyLong_Type) {
        int overflow = 0;
        const long long x = PyLong_AsLongLongAndOverflow(v, &overflow);
        if (overflow != 0) {
            put_object(st, v, tag, slot);
            return 0;
        }
        if (x == -1 && PyErr_Occurred()) return -1;
        const int64_t x64 = x;
        std::memcpy(slot, &x64, sizeof x64);
        tag = make_tag(DRAKEN_INT64);
        return 0;
    }
    if (t == &PyFloat_Type) {
        const double d = PyFloat_AS_DOUBLE(v);
        std::memcpy(slot, &d, sizeof d);
        tag = make_tag(DRAKEN_FLOAT64);
        return 0;
    }
    if (t == &PyUnicode_Type) {
        Py_ssize_t n = 0;
        const char* p = PyUnicode_AsUTF8AndSize(v, &n);
        if (p == nullptr) {  // lone surrogates cannot be UTF-8: keep the object
            PyErr_Clear();
            put_object(st, v, tag, slot);
            return 0;
        }
        if (!put_string(st, p, n, slot)) {
            put_object(st, v, tag, slot);
            return 0;
        }
        tag = make_tag(DRAKEN_VARCHAR, PyUnicode_IS_ASCII(v) ? PARAM_ASCII : 0);
        return 0;
    }
    if (t == &PyBytes_Type) {
        if (!put_string(st, PyBytes_AS_STRING(v), PyBytes_GET_SIZE(v), slot)) {
            put_object(st, v, tag, slot);
            return 0;
        }
        tag = make_tag(DRAKEN_VARBINARY);
        return 0;
    }
    if (PyDateTime_CheckExact(v)) {
        if (PyDateTime_DATE_GET_TZINFO(v) != Py_None || PyDateTime_DATE_GET_FOLD(v) != 0) {
            put_object(st, v, tag, slot);
            return 0;
        }
        const int64_t us = draken::iso_datetime::civil_to_micros(
            PyDateTime_GET_YEAR(v), PyDateTime_GET_MONTH(v), PyDateTime_GET_DAY(v),
            PyDateTime_DATE_GET_HOUR(v), PyDateTime_DATE_GET_MINUTE(v), PyDateTime_DATE_GET_SECOND(v),
            PyDateTime_DATE_GET_MICROSECOND(v));
        std::memcpy(slot, &us, sizeof us);
        tag = make_tag(DRAKEN_TIMESTAMP64);
        return 0;
    }
    if (PyDate_CheckExact(v)) {
        const int32_t days = static_cast<int32_t>(
            draken::iso_datetime::civil_to_days(PyDateTime_GET_YEAR(v), PyDateTime_GET_MONTH(v), PyDateTime_GET_DAY(v)));
        std::memcpy(slot, &days, sizeof days);
        tag = make_tag(DRAKEN_DATE32);
        return 0;
    }
    if (PyTime_CheckExact(v)) {
        if (PyDateTime_TIME_GET_TZINFO(v) != Py_None || PyDateTime_TIME_GET_FOLD(v) != 0) {
            put_object(st, v, tag, slot);
            return 0;
        }
        const int64_t us = static_cast<int64_t>(PyDateTime_TIME_GET_HOUR(v)) * 3600 * US_PER_SECOND +
                           static_cast<int64_t>(PyDateTime_TIME_GET_MINUTE(v)) * 60 * US_PER_SECOND +
                           static_cast<int64_t>(PyDateTime_TIME_GET_SECOND(v)) * US_PER_SECOND +
                           PyDateTime_TIME_GET_MICROSECOND(v);
        std::memcpy(slot, &us, sizeof us);
        tag = make_tag(DRAKEN_TIME64);
        return 0;
    }
    if (PyDelta_CheckExact(v)) {
        const int64_t days = PyDateTime_DELTA_GET_DAYS(v);
        if (days > MAX_DELTA_DAYS || days < -MAX_DELTA_DAYS) {
            put_object(st, v, tag, slot);
            return 0;
        }
        DrakenIntervalSlot iv;
        iv.months = 0;
        iv.us = days * US_PER_DAY + static_cast<int64_t>(PyDateTime_DELTA_GET_SECONDS(v)) * US_PER_SECOND +
                PyDateTime_DELTA_GET_MICROSECONDS(v);
        std::memcpy(slot, &iv, sizeof iv);
        tag = make_tag(DRAKEN_INTERVAL);
        return 0;
    }
    put_object(st, v, tag, slot);
    return 0;
}

// ----- decode: tag + slot -> Python value (the only boxing site) -------------------

inline int64_t load_i64(const uint8_t* slot) noexcept {
    int64_t x;
    std::memcpy(&x, slot, sizeof x);
    return x;
}

// A new reference, or nullptr with an exception set.
inline PyObject* decode_value(const Store& st, uint16_t tag, const uint8_t* slot) {
    switch (tag & 0xFFu) {
        case DRAKEN_NULL:
            Py_RETURN_NONE;
        case DRAKEN_BOOL:
            if (slot[0]) Py_RETURN_TRUE;
            Py_RETURN_FALSE;
        case DRAKEN_INT64:
            return PyLong_FromLongLong(load_i64(slot));
        case DRAKEN_FLOAT64: {
            double d;
            std::memcpy(&d, slot, sizeof d);
            return PyFloat_FromDouble(d);
        }
        case DRAKEN_VARCHAR: {
            const auto* s = reinterpret_cast<const DrakenStringSlot*>(slot);
            const uint32_t n = str_length(s);
            const uint8_t* p = str_data(s, st.arena.data());
            if (tag_param(tag) & PARAM_ASCII) {
                PyObject* o = PyUnicode_New(n, 127);
                if (o == nullptr) return nullptr;
                if (n) std::memcpy(PyUnicode_1BYTE_DATA(o), p, n);
                return o;
            }
            return PyUnicode_DecodeUTF8(reinterpret_cast<const char*>(p), n, "strict");
        }
        case DRAKEN_VARBINARY: {
            const auto* s = reinterpret_cast<const DrakenStringSlot*>(slot);
            return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(str_data(s, st.arena.data())),
                                             str_length(s));
        }
        case DRAKEN_TIMESTAMP64: {
            int y, mo, d, h, mi, s, us;
            us_epoch_to_parts(load_i64(slot), y, mo, d, h, mi, s, us);
            return PyDateTime_FromDateAndTime(y, mo, d, h, mi, s, us);
        }
        case DRAKEN_DATE32: {
            int32_t days;
            std::memcpy(&days, slot, sizeof days);
            int y, mo, d, h, mi, s, us;
            us_epoch_to_parts(static_cast<int64_t>(days) * US_PER_DAY, y, mo, d, h, mi, s, us);
            return PyDate_FromDate(y, mo, d);
        }
        case DRAKEN_TIME64: {
            int64_t us = load_i64(slot);
            const int h = static_cast<int>(us / (3600 * US_PER_SECOND));
            us -= static_cast<int64_t>(h) * 3600 * US_PER_SECOND;
            const int mi = static_cast<int>(us / (60 * US_PER_SECOND));
            us -= static_cast<int64_t>(mi) * 60 * US_PER_SECOND;
            const int s = static_cast<int>(us / US_PER_SECOND);
            return PyTime_FromTime(h, mi, s, static_cast<int>(us - s * US_PER_SECOND));
        }
        case DRAKEN_INTERVAL: {
            DrakenIntervalSlot iv;
            std::memcpy(&iv, slot, sizeof iv);
            int64_t days = iv.us / US_PER_DAY;
            int64_t rem = iv.us % US_PER_DAY;
            if (rem < 0) {
                rem += US_PER_DAY;
                days -= 1;
            }
            return PyDelta_FromDSU(static_cast<int>(days), static_cast<int>(rem / US_PER_SECOND),
                                   static_cast<int>(rem % US_PER_SECOND));
        }
        case TAG_OBJECT: {
            PyObject* o;
            std::memcpy(&o, slot, sizeof o);
            Py_INCREF(o);
            return o;
        }
        default:
            PyErr_Format(PyExc_SystemError, "orso: unknown record tag %u", static_cast<unsigned>(tag));
            return nullptr;
    }
}

inline PyObject* decode_row(const Store& st, PyTypeObject* row_type, uint64_t i) {
    PyObject* op = row_alloc(row_type, st.nfields);
    if (op == nullptr) return nullptr;
    const uint8_t* rec = st.record(i);
    for (uint32_t j = 0; j < st.nfields; ++j) {
        PyObject* v = decode_value(st, Store::tag_at(rec, j), st.slot_at(rec, j));
        if (v == nullptr) {
            Py_DECREF(op);
            return nullptr;
        }
        PyTuple_SET_ITEM(op, j, v);
    }
    return op;
}

// ----- width: the nbytes() estimate straight from the record -----------------------

inline int64_t slot_width(const Store& st, uint16_t tag, const uint8_t* slot) {
    switch (tag & 0xFFu) {
        case DRAKEN_NULL:
        case DRAKEN_BOOL:
            return 1;
        case DRAKEN_INT64:
            return mp_int_width(load_i64(slot));
        case DRAKEN_FLOAT64:
            return 9;
        case DRAKEN_VARCHAR:
            return mp_str_width(str_length(reinterpret_cast<const DrakenStringSlot*>(slot)));
        case DRAKEN_VARBINARY:
            return mp_bin_width(str_length(reinterpret_cast<const DrakenStringSlot*>(slot)));
        case DRAKEN_TIMESTAMP64: {
            int64_t sub = load_i64(slot) % US_PER_SECOND;
            return sub != 0 ? 27 : 20;
        }
        case DRAKEN_DATE32:
            return 11;
        case DRAKEN_TIME64:
            return load_i64(slot) % US_PER_SECOND != 0 ? 16 : 9;
        case TAG_OBJECT: {
            PyObject* o;
            std::memcpy(&o, slot, sizeof o);
            return py_value_width(o, 1);
        }
        default: {  // INTERVAL and anything rare: size the decoded value as v1 did
            PyObject* o = decode_value(st, tag, slot);
            if (o == nullptr) return -1;
            const int64_t w = py_value_width(o, 1);
            Py_DECREF(o);
            return w;
        }
    }
}

inline int64_t record_width(const Store& st, uint64_t i) {
    const uint8_t* rec = st.record(i);
    int64_t total = mp_container_header(st.nfields) + RECORD_HEADER_SIZE;
    for (uint32_t j = 0; j < st.nfields; ++j) {
        const int64_t w = slot_width(st, Store::tag_at(rec, j), st.slot_at(rec, j));
        if (w < 0) return -1;
        total += w;
    }
    return total;
}

// ---------------------------------------------------------------------------------
// Frame
// ---------------------------------------------------------------------------------

class Frame {
public:
    std::shared_ptr<Store> store;
    nb::object plan_ref;
    const Plan* plan = nullptr;
    uint64_t offset = 0;
    uint64_t view_length = 0;
    bool is_view = false;
    uint64_t counted = 0;  // nbytes() watermark: rows already sized ...
    int64_t total = 0;     // ... and their size

    explicit Frame(nb::handle plan_h) {
        plan_ref = nb::borrow(plan_h);
        plan = nb::cast<const Plan*>(plan_h);
        store = std::make_shared<Store>(static_cast<uint32_t>(plan->nfields));
    }

    uint64_t length() const noexcept { return is_view ? view_length : store->rows; }

    // A view's window is fixed and its store is shared: before writing, take a private
    // copy of exactly the rows the view shows (copy-on-write).
    void detach() {
        auto fresh = std::make_shared<Store>(store->nfields);
        const uint64_t n = view_length;
        if (n) {
            fresh->records.append(store->record(offset), static_cast<size_t>(n * store->stride));
        }
        fresh->arena.append(store->arena.data(), store->arena.size());  // string offsets stay valid
        for (uint64_t i = 0; i < n; ++i) {
            const uint8_t* rec = fresh->record(i);
            for (uint32_t j = 0; j < fresh->nfields; ++j) {
                if ((Store::tag_at(rec, j) & 0xFFu) == TAG_OBJECT) {
                    PyObject* o;
                    std::memcpy(&o, fresh->slot_at(rec, j), sizeof o);
                    fresh->objects.push_back(o);
                    Py_INCREF(o);
                }
            }
        }
        fresh->rows = n;
        store = std::move(fresh);
        offset = 0;
        is_view = false;
    }

    void append(nb::handle entry) {
        if (is_view) detach();
        ValueBuffer buf(plan->nfields);
        if (plan->gather(entry.ptr(), buf) < 0) raise_python();

        Store& st = *store;
        const size_t records_before = st.records.size();
        const size_t arena_before = st.arena.size();
        const size_t objects_before = st.objects.size();
        auto rollback = [&]() {
            for (size_t k = objects_before; k < st.objects.size(); ++k) Py_DECREF(st.objects[k]);
            st.objects.resize(objects_before);
            st.records.resize_uninit(records_before);
            st.arena.resize_uninit(arena_before);
        };
        try {
            uint8_t* rec = st.records.extend(st.stride);
            if (st.slot_offset) std::memset(rec, 0, st.slot_offset);  // deterministic tag padding
            for (uint32_t j = 0; j < st.nfields; ++j) {
                uint16_t tag = 0;
                if (encode_value(st, buf.vals[j], tag, rec + st.slot_offset + SLOT_BYTES * j) < 0) {
                    rollback();
                    raise_python();
                }
                std::memcpy(rec + 2 * j, &tag, sizeof tag);
            }
        } catch (const nb::python_error&) {
            throw;
        } catch (...) {
            rollback();
            throw;
        }
        st.rows += 1;
    }

    uint64_t resolve(int64_t i) const {
        const int64_t n = static_cast<int64_t>(length());
        if (i < 0) i += n;
        if (i < 0 || i >= n) throw nb::index_error("row index out of range");
        return offset + static_cast<uint64_t>(i);
    }

    nb::object row(int64_t i) const {
        PyObject* op = decode_row(*store, plan->row_type, resolve(i));
        if (op == nullptr) raise_python();
        return nb::steal(op);
    }

    // Rows [start, stop) of this frame as a list of Row objects; bounds clamp like a slice.
    nb::object rows(int64_t start, int64_t stop) const {
        const int64_t n = static_cast<int64_t>(length());
        if (stop < 0 || stop > n) stop = n;
        if (start < 0) start = 0;
        if (start > stop) start = stop;
        PyObject* out = PyList_New(stop - start);
        if (out == nullptr) raise_python();
        for (int64_t i = start; i < stop; ++i) {
            PyObject* op = decode_row(*store, plan->row_type, offset + static_cast<uint64_t>(i));
            if (op == nullptr) {
                Py_DECREF(out);
                raise_python();
            }
            PyList_SET_ITEM(out, i - start, op);
        }
        return nb::steal(out);
    }

    Frame view(int64_t start, int64_t count) const {
        const int64_t n = static_cast<int64_t>(length());
        if (start < 0) start = 0;
        if (start > n) start = n;
        if (count < 0) count = 0;
        if (count > n - start) count = n - start;
        Frame v(*this);
        v.offset = offset + static_cast<uint64_t>(start);
        v.view_length = static_cast<uint64_t>(count);
        v.is_view = true;
        v.counted = 0;
        v.total = 0;
        return v;
    }

    int64_t nbytes() {
        const uint64_t n = length();
        if (counted > n) {  // cannot shrink today; recount if it ever does
            counted = 0;
            total = 0;
        }
        while (counted < n) {
            const int64_t w = record_width(*store, offset + counted);
            if (w < 0) raise_python();
            total += w;
            counted += 1;
        }
        return total;
    }

    // Diagnostics for tests and DESIGN.md's memory claims.
    nb::dict stats() const {
        nb::dict d;
        d["rows"] = length();
        d["stride"] = store->stride;
        d["record_bytes"] = static_cast<uint64_t>(store->records.size());
        d["arena_bytes"] = static_cast<uint64_t>(store->arena.size());
        d["objects"] = static_cast<uint64_t>(store->objects.size());
        d["is_view"] = is_view;
        d["shared"] = store.use_count() > 1;
        return d;
    }
};

// ---------------------------------------------------------------------------------
// FrameCursor: the DB-API cursor of a store frame
// ---------------------------------------------------------------------------------

// v1's cursor was a list iterator and fetchone() was `next(cursor)`. A store frame hands
// out one of these instead so fetchone() stays exactly that, with no branch on the kind
// of frame. It reads a fixed window of the frame taken when the cursor was made;
// appending invalidates the DataFrame's cursor anyway (v1 semantics).
class FrameCursor {
public:
    Frame frame;
    uint64_t pos = 0;

    explicit FrameCursor(const Frame& f) : frame(f.view(0, static_cast<int64_t>(f.length()))) {}

    nb::object next() {
        if (pos >= frame.length()) throw nb::stop_iteration();
        PyObject* op = decode_row(*frame.store, frame.plan->row_type, frame.offset + pos);
        if (op == nullptr) raise_python();
        pos += 1;
        return nb::steal(op);
    }

    nb::object take(int64_t n) {
        const uint64_t len = frame.length();
        const uint64_t stop = n <= 0 ? pos : std::min<uint64_t>(len, pos + static_cast<uint64_t>(n));
        nb::object out = frame.rows(static_cast<int64_t>(pos), static_cast<int64_t>(stop));
        pos = stop > pos ? stop : pos;
        return out;
    }

    nb::object rest() { return take(static_cast<int64_t>(frame.length() - std::min(pos, frame.length()))); }

    uint64_t remaining() const noexcept { return pos >= frame.length() ? 0 : frame.length() - pos; }
};

}  // namespace orso
