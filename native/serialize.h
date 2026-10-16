#pragma once
// orso/native/serialize.h — Row.as_bytes / from_bytes / as_json without Python per value
// (DESIGN.md §4.9, phase 5).
//
// Byte-identical to v1 by construction:
//
//   as_bytes  v1 wrote `ormsgpack.packb(tuple(row), option=OPT_SERIALIZE_NUMPY,
//             default=serialize)` behind a 14-byte header. msgpack is concatenative, so
//             the encoding of a tuple is its array header followed by each element's
//             encoding. The common exact types are encoded here exactly as ormsgpack
//             encodes them (measured: positive ints use the unsigned family, str8
//             exists, naive datetimes become RFC 3339 text, subclasses encode as their
//             base type); every other value (numpy, enums, UUIDs, dataclasses, aware
//             datetimes, ints wider than 64 bits...) is packed by ormsgpack itself, one
//             value at a time, through a Python callable.
//   from_bytes  a msgpack reader for what the writer emits; anything else in a payload
//             hands the whole payload to ormsgpack.unpackb. Then v1's post-processing:
//             a top-level ["__datetime__", ts] becomes datetime.fromtimestamp(ts).
//   as_json   v1 was `json.dumps(row.as_dict, default=str, separators=(",", ":"))`:
//             ASCII-only output with \u escapes (surrogate pairs above the BMP),
//             float.__repr__ for floats, NaN/Infinity allowed, int.__repr__ for ints,
//             dict keys converted the json way, str(value) for anything else. Anything
//             this writer is not sure of (non-str field names, duplicate fields,
//             circular containers, very deep nesting) returns None and Python's json
//             does the row.

#include <Python.h>
#include <datetime.h>

#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "core/append_buffer.h"
#include "row.h"

namespace orso {

inline PyObject* g_value_packer = nullptr;     // v -> ormsgpack bytes for one value
inline PyObject* g_payload_unpacker = nullptr;  // payload bytes -> list (ormsgpack.unpackb)
inline PyObject* g_data_error = nullptr;        // orso.exceptions.DataError
inline PyObject* g_fromtimestamp = nullptr;     // datetime.datetime.fromtimestamp

constexpr Py_ssize_t RECORD_HEADER_BYTES = 14;
constexpr Py_ssize_t MAXIMUM_RECORD_BYTES = 16 * 1024 * 1024;
constexpr int MAX_PACK_DEPTH = 200;

using Bytes = draken::AppendBuffer<uint8_t>;

// ----- msgpack writer ------------------------------------------------------------------

inline void put_u8(Bytes& out, uint8_t b) { out.push_back(b); }

template <typename T>
inline void put_be(Bytes& out, T v) {
    uint8_t buf[sizeof(T)];
    for (size_t i = 0; i < sizeof(T); ++i) buf[i] = static_cast<uint8_t>(static_cast<uint64_t>(v) >> (8 * (sizeof(T) - 1 - i)));
    out.append(buf, sizeof(T));
}

inline void mp_put_int(Bytes& out, int64_t v) {
    if (v >= 0) {
        if (v <= 0x7f) put_u8(out, static_cast<uint8_t>(v));
        else if (v <= 0xff) { put_u8(out, 0xcc); put_u8(out, static_cast<uint8_t>(v)); }
        else if (v <= 0xffff) { put_u8(out, 0xcd); put_be<uint16_t>(out, static_cast<uint16_t>(v)); }
        else if (v <= 0xffffffffLL) { put_u8(out, 0xce); put_be<uint32_t>(out, static_cast<uint32_t>(v)); }
        else { put_u8(out, 0xcf); put_be<uint64_t>(out, static_cast<uint64_t>(v)); }
        return;
    }
    if (v >= -32) put_u8(out, static_cast<uint8_t>(v));
    else if (v >= -128) { put_u8(out, 0xd0); put_u8(out, static_cast<uint8_t>(v)); }
    else if (v >= -32768) { put_u8(out, 0xd1); put_be<uint16_t>(out, static_cast<uint16_t>(v)); }
    else if (v >= -2147483648LL) { put_u8(out, 0xd2); put_be<uint32_t>(out, static_cast<uint32_t>(v)); }
    else { put_u8(out, 0xd3); put_be<uint64_t>(out, static_cast<uint64_t>(v)); }
}

inline void mp_put_str(Bytes& out, const char* p, size_t n) {
    if (n <= 31) put_u8(out, static_cast<uint8_t>(0xa0 | n));
    else if (n <= 0xff) { put_u8(out, 0xd9); put_u8(out, static_cast<uint8_t>(n)); }
    else if (n <= 0xffff) { put_u8(out, 0xda); put_be<uint16_t>(out, static_cast<uint16_t>(n)); }
    else { put_u8(out, 0xdb); put_be<uint32_t>(out, static_cast<uint32_t>(n)); }
    out.append(reinterpret_cast<const uint8_t*>(p), n);
}

inline void mp_put_bin(Bytes& out, const char* p, size_t n) {
    if (n <= 0xff) { put_u8(out, 0xc4); put_u8(out, static_cast<uint8_t>(n)); }
    else if (n <= 0xffff) { put_u8(out, 0xc5); put_be<uint16_t>(out, static_cast<uint16_t>(n)); }
    else { put_u8(out, 0xc6); put_be<uint32_t>(out, static_cast<uint32_t>(n)); }
    out.append(reinterpret_cast<const uint8_t*>(p), n);
}

inline void mp_put_container(Bytes& out, size_t n, uint8_t fix, uint8_t c16, uint8_t c32) {
    if (n <= 15) put_u8(out, static_cast<uint8_t>(fix | n));
    else if (n <= 0xffff) { put_u8(out, c16); put_be<uint16_t>(out, static_cast<uint16_t>(n)); }
    else { put_u8(out, c32); put_be<uint32_t>(out, static_cast<uint32_t>(n)); }
}

// One value through ormsgpack (exact by construction). 0, or -1 with an exception set.
inline int mp_put_python(Bytes& out, PyObject* v) {
    if (g_value_packer == nullptr) {
        PyErr_SetString(PyExc_RuntimeError, "orso._native: value packer not installed (import orso.row first)");
        return -1;
    }
    PyObject* b = PyObject_CallOneArg(g_value_packer, v);
    if (b == nullptr) return -1;
    if (!PyBytes_Check(b)) {
        Py_DECREF(b);
        PyErr_SetString(PyExc_TypeError, "orso: value packer must return bytes");
        return -1;
    }
    out.append(reinterpret_cast<const uint8_t*>(PyBytes_AS_STRING(b)), static_cast<size_t>(PyBytes_GET_SIZE(b)));
    Py_DECREF(b);
    return 0;
}

inline void put_2d(char* p, int v) {
    p[0] = static_cast<char>('0' + v / 10);
    p[1] = static_cast<char>('0' + v % 10);
}

// "YYYY-MM-DD", "HH:MM:SS" and ".ffffff" as ormsgpack writes them
inline size_t fmt_date(char* p, int y, int m, int d) {
    p[0] = static_cast<char>('0' + y / 1000);
    p[1] = static_cast<char>('0' + (y / 100) % 10);
    p[2] = static_cast<char>('0' + (y / 10) % 10);
    p[3] = static_cast<char>('0' + y % 10);
    p[4] = '-';
    put_2d(p + 5, m);
    p[7] = '-';
    put_2d(p + 8, d);
    return 10;
}

inline size_t fmt_time(char* p, int h, int mi, int s, int us) {
    put_2d(p, h);
    p[2] = ':';
    put_2d(p + 3, mi);
    p[5] = ':';
    put_2d(p + 6, s);
    if (us == 0) return 8;
    p[8] = '.';
    for (int i = 6; i >= 1; --i) {
        p[8 + i] = static_cast<char>('0' + us % 10);
        us /= 10;
    }
    return 15;
}

inline int mp_put_value(Bytes& out, PyObject* v, int depth) {
    if (v == Py_None) { put_u8(out, 0xc0); return 0; }
    if (v == Py_True) { put_u8(out, 0xc3); return 0; }
    if (v == Py_False) { put_u8(out, 0xc2); return 0; }
    if (depth > MAX_PACK_DEPTH) return mp_put_python(out, v);  // ormsgpack decides (it raises)
    PyTypeObject* t = Py_TYPE(v);
    if (PyLong_Check(v) && !PyBool_Check(v)) {
        int overflow = 0;
        const long long x = PyLong_AsLongLongAndOverflow(v, &overflow);
        if (overflow == 0) {
            if (x == -1 && PyErr_Occurred()) return -1;
            mp_put_int(out, x);
            return 0;
        }
        if (overflow > 0) {
            const unsigned long long u = PyLong_AsUnsignedLongLong(v);
            if (!(u == static_cast<unsigned long long>(-1) && PyErr_Occurred())) {
                put_u8(out, 0xcf);
                put_be<uint64_t>(out, u);
                return 0;
            }
            PyErr_Clear();
        }
        return mp_put_python(out, v);  // wider than 64 bits: ormsgpack raises, as v1 did
    }
    if (PyFloat_Check(v)) {
        double d = PyFloat_AS_DOUBLE(v);
        uint64_t bits;
        std::memcpy(&bits, &d, 8);
        put_u8(out, 0xcb);
        put_be<uint64_t>(out, bits);
        return 0;
    }
    if (PyUnicode_Check(v)) {
        Py_ssize_t n = 0;
        const char* p = PyUnicode_AsUTF8AndSize(v, &n);
        if (p == nullptr) {
            PyErr_Clear();
            return mp_put_python(out, v);  // surrogates: ormsgpack raises
        }
        mp_put_str(out, p, static_cast<size_t>(n));
        return 0;
    }
    if (t == &PyBytes_Type) {
        mp_put_bin(out, PyBytes_AS_STRING(v), static_cast<size_t>(PyBytes_GET_SIZE(v)));
        return 0;
    }
    if (PyList_Check(v) || PyTuple_Check(v)) {
        const Py_ssize_t n = PySequence_Fast_GET_SIZE(v);
        mp_put_container(out, static_cast<size_t>(n), 0x90, 0xdc, 0xdd);
        PyObject** items = PySequence_Fast_ITEMS(v);
        for (Py_ssize_t i = 0; i < n; ++i) {
            if (mp_put_value(out, items[i], depth + 1) < 0) return -1;
        }
        return 0;
    }
    if (PyDict_Check(v)) {
        Py_ssize_t pos = 0;
        PyObject *key, *value;
        while (PyDict_Next(v, &pos, &key, &value)) {
            if (!PyUnicode_Check(key)) return mp_put_python(out, v);  // "Dict key must be str"
        }
        const size_t mark = out.size();
        mp_put_container(out, static_cast<size_t>(PyDict_GET_SIZE(v)), 0x80, 0xde, 0xdf);
        pos = 0;
        while (PyDict_Next(v, &pos, &key, &value)) {
            if (mp_put_value(out, key, depth + 1) < 0 || mp_put_value(out, value, depth + 1) < 0) {
                out.resize_uninit(mark);
                return -1;
            }
        }
        return 0;
    }
    if (PyDateTime_CheckExact(v) && PyDateTime_DATE_GET_TZINFO(v) == Py_None) {
        char buf[32];
        size_t n = fmt_date(buf, PyDateTime_GET_YEAR(v), PyDateTime_GET_MONTH(v), PyDateTime_GET_DAY(v));
        buf[n++] = 'T';
        n += fmt_time(buf + n, PyDateTime_DATE_GET_HOUR(v), PyDateTime_DATE_GET_MINUTE(v),
                      PyDateTime_DATE_GET_SECOND(v), PyDateTime_DATE_GET_MICROSECOND(v));
        mp_put_str(out, buf, n);
        return 0;
    }
    if (PyDate_CheckExact(v)) {
        char buf[16];
        const size_t n = fmt_date(buf, PyDateTime_GET_YEAR(v), PyDateTime_GET_MONTH(v), PyDateTime_GET_DAY(v));
        mp_put_str(out, buf, n);
        return 0;
    }
    if (PyTime_CheckExact(v) && PyDateTime_TIME_GET_TZINFO(v) == Py_None) {
        char buf[16];
        const size_t n = fmt_time(buf, PyDateTime_TIME_GET_HOUR(v), PyDateTime_TIME_GET_MINUTE(v),
                                  PyDateTime_TIME_GET_SECOND(v), PyDateTime_TIME_GET_MICROSECOND(v));
        mp_put_str(out, buf, n);
        return 0;
    }
    if (reinterpret_cast<PyObject*>(t) == g_decimal_type) {  // default=serialize -> str(value)
        PyObject* s = PyObject_Str(v);
        if (s == nullptr) return -1;
        Py_ssize_t n = 0;
        const char* p = PyUnicode_AsUTF8AndSize(s, &n);
        if (p != nullptr) mp_put_str(out, p, static_cast<size_t>(n));
        Py_DECREF(s);
        return p == nullptr ? -1 : 0;
    }
    return mp_put_python(out, v);
}

// msgpack payload of a row (v1's Row._packed). New bytes, or nullptr with an exception.
inline PyObject* row_payload(PyObject* row, Bytes& out) {
    PyObject* seq = PySequence_Fast(row, "orso: a row must be a sequence");
    if (seq == nullptr) return nullptr;
    const Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
    mp_put_container(out, static_cast<size_t>(n), 0x90, 0xdc, 0xdd);
    PyObject** items = PySequence_Fast_ITEMS(seq);
    for (Py_ssize_t i = 0; i < n; ++i) {
        if (mp_put_value(out, items[i], 1) < 0) {
            Py_DECREF(seq);
            return nullptr;
        }
    }
    Py_DECREF(seq);
    Py_RETURN_NONE;
}

inline PyObject* row_packed(PyObject* row) {
    Bytes out;
    PyObject* ok = row_payload(row, out);
    if (ok == nullptr) return nullptr;
    Py_DECREF(ok);
    return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(out.data()), static_cast<Py_ssize_t>(out.size()));
}

// v1's record: 0x10 0x00, payload length (u32 BE), time.time_ns() (u64 BE), payload.
inline PyObject* row_as_bytes(PyObject* row) {
    Bytes out;
    out.resize_uninit(RECORD_HEADER_BYTES);
    PyObject* ok = row_payload(row, out);
    if (ok == nullptr) return nullptr;
    Py_DECREF(ok);
    const size_t payload = out.size() - RECORD_HEADER_BYTES;
    if (payload > static_cast<size_t>(MAXIMUM_RECORD_BYTES)) {
        PyErr_SetString(g_data_error ? g_data_error : PyExc_ValueError, "Record length cannot exceed 16Mb");
        return nullptr;
    }
    const uint64_t ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    uint8_t* h = out.data();
    h[0] = 0x10;
    h[1] = 0x00;
    for (int i = 0; i < 4; ++i) h[2 + i] = static_cast<uint8_t>(payload >> (8 * (3 - i)));
    for (int i = 0; i < 8; ++i) h[6 + i] = static_cast<uint8_t>(ns >> (8 * (7 - i)));
    return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(out.data()), static_cast<Py_ssize_t>(out.size()));
}

// ----- msgpack reader --------------------------------------------------------------------

struct MsgReader {
    const uint8_t* p;
    const uint8_t* end;
    bool foreign = false;  // met something only ormsgpack should interpret

    bool need(size_t n) const { return static_cast<size_t>(end - p) >= n; }
    template <typename T>
    T be() {
        T v = 0;
        for (size_t i = 0; i < sizeof(T); ++i) v = static_cast<T>((static_cast<uint64_t>(v) << 8) | p[i]);
        p += sizeof(T);
        return v;
    }
};

inline PyObject* mp_read(MsgReader& r, int depth);

inline PyObject* mp_read_n(MsgReader& r, size_t n, bool map, int depth) {
    if (map) {
        PyObject* d = PyDict_New();
        if (d == nullptr) return nullptr;
        for (size_t i = 0; i < n; ++i) {
            PyObject* k = mp_read(r, depth + 1);
            if (k == nullptr) { Py_DECREF(d); return nullptr; }
            if (!PyUnicode_CheckExact(k)) { r.foreign = true; Py_DECREF(k); Py_DECREF(d); return nullptr; }
            PyObject* v = mp_read(r, depth + 1);
            if (v == nullptr || PyDict_SetItem(d, k, v) < 0) { Py_DECREF(k); Py_XDECREF(v); Py_DECREF(d); return nullptr; }
            Py_DECREF(k);
            Py_DECREF(v);
        }
        return d;
    }
    PyObject* l = PyList_New(static_cast<Py_ssize_t>(n));
    if (l == nullptr) return nullptr;
    for (size_t i = 0; i < n; ++i) {
        PyObject* v = mp_read(r, depth + 1);
        if (v == nullptr) { Py_DECREF(l); return nullptr; }
        PyList_SET_ITEM(l, static_cast<Py_ssize_t>(i), v);
    }
    return l;
}

// nullptr with an exception, or with r.foreign set (caller falls back to ormsgpack)
inline PyObject* mp_read(MsgReader& r, int depth) {
    if (depth > MAX_PACK_DEPTH || !r.need(1)) { r.foreign = true; return nullptr; }
    const uint8_t b = *r.p++;
    auto take = [&](size_t n) { if (!r.need(n)) { r.foreign = true; return false; } return true; };
    if (b <= 0x7f) return PyLong_FromLong(b);
    if (b >= 0xe0) return PyLong_FromLong(static_cast<int8_t>(b));
    if ((b & 0xe0) == 0xa0 || b == 0xd9 || b == 0xda || b == 0xdb) {
        size_t n;
        if ((b & 0xe0) == 0xa0) n = b & 0x1f;
        else if (b == 0xd9) { if (!take(1)) return nullptr; n = r.be<uint8_t>(); }
        else if (b == 0xda) { if (!take(2)) return nullptr; n = r.be<uint16_t>(); }
        else { if (!take(4)) return nullptr; n = r.be<uint32_t>(); }
        if (!take(n)) return nullptr;
        PyObject* s = PyUnicode_DecodeUTF8(reinterpret_cast<const char*>(r.p), static_cast<Py_ssize_t>(n), "strict");
        if (s == nullptr) { PyErr_Clear(); r.foreign = true; return nullptr; }
        r.p += n;
        return s;
    }
    if ((b & 0xf0) == 0x90) return mp_read_n(r, b & 0x0f, false, depth);
    if ((b & 0xf0) == 0x80) return mp_read_n(r, b & 0x0f, true, depth);
    switch (b) {
        case 0xc0: Py_RETURN_NONE;
        case 0xc2: Py_RETURN_FALSE;
        case 0xc3: Py_RETURN_TRUE;
        case 0xc4: case 0xc5: case 0xc6: {
            size_t n;
            if (b == 0xc4) { if (!take(1)) return nullptr; n = r.be<uint8_t>(); }
            else if (b == 0xc5) { if (!take(2)) return nullptr; n = r.be<uint16_t>(); }
            else { if (!take(4)) return nullptr; n = r.be<uint32_t>(); }
            if (!take(n)) return nullptr;
            PyObject* o = PyBytes_FromStringAndSize(reinterpret_cast<const char*>(r.p), static_cast<Py_ssize_t>(n));
            r.p += n;
            return o;
        }
        case 0xca: { if (!take(4)) return nullptr; uint32_t bits = r.be<uint32_t>(); float f; std::memcpy(&f, &bits, 4); return PyFloat_FromDouble(f); }
        case 0xcb: { if (!take(8)) return nullptr; uint64_t bits = r.be<uint64_t>(); double d; std::memcpy(&d, &bits, 8); return PyFloat_FromDouble(d); }
        case 0xcc: if (!take(1)) return nullptr; return PyLong_FromUnsignedLong(r.be<uint8_t>());
        case 0xcd: if (!take(2)) return nullptr; return PyLong_FromUnsignedLong(r.be<uint16_t>());
        case 0xce: if (!take(4)) return nullptr; return PyLong_FromUnsignedLong(r.be<uint32_t>());
        case 0xcf: if (!take(8)) return nullptr; return PyLong_FromUnsignedLongLong(r.be<uint64_t>());
        case 0xd0: if (!take(1)) return nullptr; return PyLong_FromLong(static_cast<int8_t>(r.be<uint8_t>()));
        case 0xd1: if (!take(2)) return nullptr; return PyLong_FromLong(static_cast<int16_t>(r.be<uint16_t>()));
        case 0xd2: if (!take(4)) return nullptr; return PyLong_FromLong(static_cast<int32_t>(r.be<uint32_t>()));
        case 0xd3: if (!take(8)) return nullptr; return PyLong_FromLongLong(static_cast<int64_t>(r.be<uint64_t>()));
        case 0xdc: if (!take(2)) return nullptr; return mp_read_n(r, r.be<uint16_t>(), false, depth);
        case 0xdd: if (!take(4)) return nullptr; return mp_read_n(r, r.be<uint32_t>(), false, depth);
        case 0xde: if (!take(2)) return nullptr; return mp_read_n(r, r.be<uint16_t>(), true, depth);
        case 0xdf: if (!take(4)) return nullptr; return mp_read_n(r, r.be<uint32_t>(), true, depth);
        default: r.foreign = true; return nullptr;  // ext types and the like
    }
}

// v1's from_bytes_cython: header checks, payload, then ["__datetime__", ts] -> datetime.
inline PyObject* row_from_bytes(PyObject* data) {
    if (!PyBytes_Check(data)) {
        PyErr_SetString(PyExc_TypeError, "from_bytes() takes bytes");
        return nullptr;
    }
    const auto* p = reinterpret_cast<const uint8_t*>(PyBytes_AS_STRING(data));
    const Py_ssize_t length = PyBytes_GET_SIZE(data);
    if (length < RECORD_HEADER_BYTES || (p[0] & 0xF0) != 0x10) {
        PyErr_SetString(g_data_error, "Data malformed");
        return nullptr;
    }
    const Py_ssize_t record_size = (static_cast<Py_ssize_t>(p[2]) << 24) | (static_cast<Py_ssize_t>(p[3]) << 16) |
                                   (static_cast<Py_ssize_t>(p[4]) << 8) | static_cast<Py_ssize_t>(p[5]);
    if (record_size != length - RECORD_HEADER_BYTES) {
        PyErr_SetString(g_data_error, "Data malformed - incorrect length");
        return nullptr;
    }
    MsgReader r{p + RECORD_HEADER_BYTES, p + length};
    PyObject* items = mp_read(r, 0);
    if (items != nullptr && (r.p != r.end || !PyList_CheckExact(items))) {
        Py_DECREF(items);
        items = nullptr;
        r.foreign = true;
    }
    if (items == nullptr) {
        if (!r.foreign) return nullptr;
        PyErr_Clear();
        PyObject* payload = PyBytes_FromStringAndSize(reinterpret_cast<const char*>(p + RECORD_HEADER_BYTES),
                                                      length - RECORD_HEADER_BYTES);
        if (payload == nullptr) return nullptr;
        items = PyObject_CallOneArg(g_payload_unpacker, payload);  // ormsgpack: v1's errors for bad payloads
        Py_DECREF(payload);
        if (items == nullptr) return nullptr;
        if (!PyList_Check(items)) {  // v1 declared the payload `cdef list`
            PyErr_Format(PyExc_TypeError, "Expected list, got %.200s", Py_TYPE(items)->tp_name);
            Py_DECREF(items);
            return nullptr;
        }
    }
    const Py_ssize_t n = PyList_GET_SIZE(items);
    PyObject* out = PyTuple_New(n);
    if (out == nullptr) {
        Py_DECREF(items);
        return nullptr;
    }
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* item = PyList_GET_ITEM(items, i);
        PyObject* value = nullptr;
        if (PyList_Check(item) && PyList_GET_SIZE(item) == 2 && PyUnicode_Check(PyList_GET_ITEM(item, 0)) &&
            PyUnicode_CompareWithASCIIString(PyList_GET_ITEM(item, 0), "__datetime__") == 0) {
            value = PyObject_CallOneArg(g_fromtimestamp, PyList_GET_ITEM(item, 1));
            if (value == nullptr) {
                Py_DECREF(out);
                Py_DECREF(items);
                return nullptr;
            }
        } else {
            Py_INCREF(item);
            value = item;
        }
        PyTuple_SET_ITEM(out, i, value);
    }
    Py_DECREF(items);
    return out;
}

// ----- JSON writer (json.dumps(as_dict, default=str, separators=(",", ":"))) -----------

struct JsonOut {
    std::string s;
    std::vector<PyObject*> stack;  // containers being written: a repeat is a cycle
    bool unsure = false;           // hand the row to Python's json
};

inline void json_put_str(JsonOut& o, PyObject* u) {
    static const char hex[] = "0123456789abcdef";
    o.s.push_back('"');
    const int kind = PyUnicode_KIND(u);
    const void* data = PyUnicode_DATA(u);
    const Py_ssize_t n = PyUnicode_GET_LENGTH(u);
    auto esc = [&](Py_UCS4 c) {
        o.s.append("\\u");
        o.s.push_back(hex[(c >> 12) & 0xf]);
        o.s.push_back(hex[(c >> 8) & 0xf]);
        o.s.push_back(hex[(c >> 4) & 0xf]);
        o.s.push_back(hex[c & 0xf]);
    };
    for (Py_ssize_t i = 0; i < n; ++i) {
        Py_UCS4 c = PyUnicode_READ(kind, data, i);
        if (c >= ' ' && c <= '~' && c != '\\' && c != '"') {
            o.s.push_back(static_cast<char>(c));
            continue;
        }
        switch (c) {
            case '\\': o.s.append("\\\\"); break;
            case '"': o.s.append("\\\""); break;
            case '\b': o.s.append("\\b"); break;
            case '\f': o.s.append("\\f"); break;
            case '\n': o.s.append("\\n"); break;
            case '\r': o.s.append("\\r"); break;
            case '\t': o.s.append("\\t"); break;
            default:
                if (c >= 0x10000) {
                    const Py_UCS4 v = c - 0x10000;
                    esc(0xd800 | ((v >> 10) & 0x3ff));
                    esc(0xdc00 | (v & 0x3ff));
                } else {
                    esc(c);
                }
        }
    }
    o.s.push_back('"');
}

inline bool json_put_repr(JsonOut& o, PyObject* v, reprfunc repr) {
    PyObject* r = repr(v);
    if (r == nullptr) return false;
    Py_ssize_t n = 0;
    const char* p = PyUnicode_AsUTF8AndSize(r, &n);
    if (p != nullptr) o.s.append(p, static_cast<size_t>(n));
    Py_DECREF(r);
    return p != nullptr;
}

inline bool json_put_float(JsonOut& o, PyObject* v) {
    const double d = PyFloat_AS_DOUBLE(v);
    if (d != d) { o.s.append("NaN"); return true; }
    if (d == HUGE_VAL) { o.s.append("Infinity"); return true; }
    if (d == -HUGE_VAL) { o.s.append("-Infinity"); return true; }
    return json_put_repr(o, v, PyFloat_Type.tp_repr);
}

inline bool json_put(JsonOut& o, PyObject* v, int depth);

// json's key rules: str as is; float, int, bool, None converted; anything else is a
// TypeError in Python's json, so let Python raise it
inline bool json_put_key(JsonOut& o, PyObject* k) {
    if (PyUnicode_Check(k)) { json_put_str(o, k); return true; }
    if (k == Py_True) { o.s.append("\"true\""); return true; }
    if (k == Py_False) { o.s.append("\"false\""); return true; }
    if (k == Py_None) { o.s.append("\"null\""); return true; }
    if (PyFloat_Check(k)) {
        o.s.push_back('"');
        if (!json_put_float(o, k)) return false;
        o.s.push_back('"');
        return true;
    }
    if (PyLong_Check(k)) {
        o.s.push_back('"');
        if (!json_put_repr(o, k, PyLong_Type.tp_repr)) return false;
        o.s.push_back('"');
        return true;
    }
    o.unsure = true;
    return false;
}

inline bool json_put(JsonOut& o, PyObject* v, int depth) {
    if (depth > MAX_PACK_DEPTH) { o.unsure = true; return false; }
    if (v == Py_None) { o.s.append("null"); return true; }
    if (v == Py_True) { o.s.append("true"); return true; }
    if (v == Py_False) { o.s.append("false"); return true; }
    if (PyUnicode_Check(v)) { json_put_str(o, v); return true; }
    if (PyLong_Check(v)) {
        if (PyLong_CheckExact(v)) {
            int overflow = 0;
            const long long x = PyLong_AsLongLongAndOverflow(v, &overflow);
            if (overflow == 0 && !(x == -1 && PyErr_Occurred())) {
                o.s.append(std::to_string(x));
                return true;
            }
            PyErr_Clear();
        }
        return json_put_repr(o, v, PyLong_Type.tp_repr);
    }
    if (PyFloat_Check(v)) return json_put_float(o, v);
    const bool list = PyList_Check(v) || PyTuple_Check(v);
    const bool dict = !list && PyDict_Check(v);
    if (list || dict) {
        for (PyObject* seen : o.stack) {
            if (seen == v) { o.unsure = true; return false; }  // Python raises "Circular reference"
        }
        o.stack.push_back(v);
        bool ok = true;
        if (list) {
            o.s.push_back('[');
            const Py_ssize_t n = PySequence_Fast_GET_SIZE(v);
            PyObject** items = PySequence_Fast_ITEMS(v);
            for (Py_ssize_t i = 0; ok && i < n; ++i) {
                if (i) o.s.push_back(',');
                ok = json_put(o, items[i], depth + 1);
            }
            o.s.push_back(']');
        } else {
            o.s.push_back('{');
            Py_ssize_t pos = 0;
            PyObject *k, *val;
            bool first = true;
            while (ok && PyDict_Next(v, &pos, &k, &val)) {
                if (!first) o.s.push_back(',');
                first = false;
                ok = json_put_key(o, k);
                if (ok) {
                    o.s.push_back(':');
                    ok = json_put(o, val, depth + 1);
                }
            }
            o.s.push_back('}');
        }
        o.stack.pop_back();
        return ok;
    }
    // default=str
    PyObject* s = PyObject_Str(v);
    if (s == nullptr) return false;
    json_put_str(o, s);
    Py_DECREF(s);
    return true;
}

// Row.as_json, or None when Python's json should do the row. New reference or nullptr.
inline PyObject* row_as_json(PyObject* row) {
    TypeFields* tf = type_fields(Py_TYPE(row));
    if (tf == nullptr) return nullptr;
    if (tf->fields == Py_None) Py_RETURN_NONE;
    const Py_ssize_t n = PyTuple_GET_SIZE(tf->fields);
    if (PyTuple_GET_SIZE(row) < n) Py_RETURN_NONE;  // as_dict raises; let it
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* f = PyTuple_GET_ITEM(tf->fields, i);
        if (!PyUnicode_CheckExact(f)) Py_RETURN_NONE;
        for (Py_ssize_t j = 0; j < i; ++j) {
            if (PyUnicode_Compare(f, PyTuple_GET_ITEM(tf->fields, j)) == 0) Py_RETURN_NONE;  // duplicate field
        }
    }
    JsonOut o;
    o.s.reserve(64);
    o.s.push_back('{');
    for (Py_ssize_t i = 0; i < n; ++i) {
        if (i) o.s.push_back(',');
        json_put_str(o, PyTuple_GET_ITEM(tf->fields, i));
        o.s.push_back(':');
        if (!json_put(o, PyTuple_GET_ITEM(row, i), 1)) {
            if (o.unsure && !PyErr_Occurred()) Py_RETURN_NONE;
            if (PyErr_Occurred()) return nullptr;
            Py_RETURN_NONE;
        }
    }
    o.s.push_back('}');
    return PyBytes_FromStringAndSize(o.s.data(), static_cast<Py_ssize_t>(o.s.size()));
}

}  // namespace orso
