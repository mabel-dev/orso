#pragma once
// orso/native/arrow_import.h — Arrow record batches into the record store (DESIGN.md §4.8).
//
// v1 turned every column into Python objects with to_pylist(), zipped them into tuples
// and built a Row per tuple: ~560 ns a row. Here pyarrow hands over its buffers through
// the C Data Interface (`RecordBatch.__arrow_c_array__`) and each value is written
// straight into its slot; nothing is boxed until a row is read.
//
// The contract is that a row read back equals what v1's to_pylist() produced, value for
// value and type for type. A column is imported natively only when that is guaranteed:
// its Arrow type has a reader below AND, for temporal types, every value fits Python's
// datetime range (checked up front). Any other column — dictionary-encoded, timestamps
// with a time zone or in nanoseconds, float16, maps, unions, decimal256, string views,
// out-of-range dates — is converted by pyarrow's own to_pylist() and its Python values
// are encoded like appended ones. So the native path is an optimisation, never a change
// of answer.

#include <Python.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "store.h"
#include "interop/arrow_c_data_interface.h"

namespace orso {

enum class ReadKind {
    Null, Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64,
    Utf8, LargeUtf8, Binary, LargeBinary, Date32, Date64, Timestamp, Time, Duration, Decimal128,
    List, LargeList, FixedList, Struct,
};

struct Reader {
    ReadKind kind = ReadKind::Null;
    const ArrowArray* a = nullptr;
    int64_t off = 0;
    const uint8_t* valid = nullptr;
    const uint8_t* b1 = nullptr;
    const uint8_t* b2 = nullptr;
    int64_t mul = 1;     // value -> microseconds (timestamps, times, durations)
    uint8_t scale = 0;   // decimal scale
    int64_t fixed = 0;   // fixed-size list width
    std::vector<Reader> children;
    std::vector<std::string> names;  // struct field names, UTF-8

    bool is_null(int64_t i) const noexcept {
        if (kind == ReadKind::Null) return true;
        if (valid == nullptr) return false;
        const int64_t k = off + i;
        return ((valid[k >> 3] >> (k & 7)) & 1) == 0;
    }
};

constexpr int64_t MIN_DATETIME_US = -62135596800LL * US_PER_SECOND;                  // 0001-01-01
constexpr int64_t MAX_DATETIME_US = 253402300799LL * US_PER_SECOND + 999999;         // 9999-12-31T23:59:59.999999
constexpr int64_t MIN_DATE_DAYS = -719162;                                           // 0001-01-01
constexpr int64_t MAX_DATE_DAYS = 2932896;                                           // 9999-12-31

inline bool starts_with(const char* s, const char* p) { return std::strncmp(s, p, std::strlen(p)) == 0; }

inline int64_t floor_div(int64_t a, int64_t b) noexcept {
    int64_t q = a / b;
    if ((a % b != 0) && ((a < 0) != (b < 0))) --q;
    return q;
}

// Build a reader for one array; false when the type has no faithful native reader.
inline bool make_reader(const ArrowSchema* s, const ArrowArray* a, Reader& r, int depth) {
    if (s == nullptr || a == nullptr || s->dictionary != nullptr || depth > MAX_NEST_DEPTH) return false;
    const char* f = s->format;
    r.a = a;
    r.off = a->offset;
    r.valid = a->n_buffers > 0 ? static_cast<const uint8_t*>(a->buffers[0]) : nullptr;
    r.b1 = a->n_buffers > 1 ? static_cast<const uint8_t*>(a->buffers[1]) : nullptr;
    r.b2 = a->n_buffers > 2 ? static_cast<const uint8_t*>(a->buffers[2]) : nullptr;
    auto simple = [&](ReadKind k) { r.kind = k; return true; };
    if (!std::strcmp(f, "n")) return simple(ReadKind::Null);
    if (!std::strcmp(f, "b")) return simple(ReadKind::Bool);
    if (!std::strcmp(f, "c")) return simple(ReadKind::Int8);
    if (!std::strcmp(f, "s")) return simple(ReadKind::Int16);
    if (!std::strcmp(f, "i")) return simple(ReadKind::Int32);
    if (!std::strcmp(f, "l")) return simple(ReadKind::Int64);
    if (!std::strcmp(f, "C")) return simple(ReadKind::UInt8);
    if (!std::strcmp(f, "S")) return simple(ReadKind::UInt16);
    if (!std::strcmp(f, "I")) return simple(ReadKind::UInt32);
    if (!std::strcmp(f, "L")) return simple(ReadKind::UInt64);
    if (!std::strcmp(f, "f")) return simple(ReadKind::Float32);
    if (!std::strcmp(f, "g")) return simple(ReadKind::Float64);
    if (!std::strcmp(f, "u")) return simple(ReadKind::Utf8);
    if (!std::strcmp(f, "U")) return simple(ReadKind::LargeUtf8);
    if (!std::strcmp(f, "z")) return simple(ReadKind::Binary);
    if (!std::strcmp(f, "Z")) return simple(ReadKind::LargeBinary);
    if (!std::strcmp(f, "tdD")) return simple(ReadKind::Date32);
    if (!std::strcmp(f, "tdm")) return simple(ReadKind::Date64);
    if (starts_with(f, "ts")) {  // tss: tsm: tsu: — time-zone-naive only; ns gives pandas objects
        if (std::strlen(f) != 4 || f[3] != ':') return false;
        if (f[2] == 's') r.mul = US_PER_SECOND;
        else if (f[2] == 'm') r.mul = 1000;
        else if (f[2] == 'u') r.mul = 1;
        else return false;
        return simple(ReadKind::Timestamp);
    }
    if (!std::strcmp(f, "tts")) { r.mul = US_PER_SECOND; return simple(ReadKind::Time); }
    if (!std::strcmp(f, "ttm")) { r.mul = 1000; return simple(ReadKind::Time); }
    if (!std::strcmp(f, "ttu")) { r.mul = 1; return simple(ReadKind::Time); }
    if (!std::strcmp(f, "tDs")) { r.mul = US_PER_SECOND; return simple(ReadKind::Duration); }
    if (!std::strcmp(f, "tDm")) { r.mul = 1000; return simple(ReadKind::Duration); }
    if (!std::strcmp(f, "tDu")) { r.mul = 1; return simple(ReadKind::Duration); }
    if (starts_with(f, "d:")) {  // d:precision,scale[,bitwidth]
        int precision = 0, scale = 0, bits = 128;
        const int n = std::sscanf(f + 2, "%d,%d,%d", &precision, &scale, &bits);
        if (n < 2 || bits != 128 || scale < 0 || scale > DEC_MAX_DIGITS || precision > DEC_MAX_DIGITS) return false;
        r.scale = static_cast<uint8_t>(scale);
        return simple(ReadKind::Decimal128);
    }
    if (!std::strcmp(f, "+l") || !std::strcmp(f, "+L") || starts_with(f, "+w:")) {
        if (s->n_children != 1 || a->n_children != 1) return false;
        if (f[1] == 'w') {
            r.fixed = std::atoll(f + 3);
            if (r.fixed < 0) return false;
            r.kind = ReadKind::FixedList;
        } else {
            r.kind = f[1] == 'l' ? ReadKind::List : ReadKind::LargeList;
        }
        r.children.resize(1);
        return make_reader(s->children[0], a->children[0], r.children[0], depth + 1);
    }
    if (!std::strcmp(f, "+s")) {
        if (s->n_children != a->n_children) return false;
        r.kind = ReadKind::Struct;
        r.children.resize(static_cast<size_t>(s->n_children));
        for (int64_t k = 0; k < s->n_children; ++k) {
            const char* name = s->children[k]->name ? s->children[k]->name : "";
            for (const std::string& seen : r.names) {
                if (seen == name) return false;  // to_pylist refuses duplicate field names
            }
            r.names.emplace_back(name);
            if (!make_reader(s->children[k], a->children[k], r.children[static_cast<size_t>(k)], depth + 1)) return false;
        }
        return true;
    }
    return false;
}

// Every value of a reader (and its children, wholly) must come back as v1 would see it.
// Temporal values outside Python's range make to_pylist() raise; send those columns
// down the to_pylist() path so they raise the same way.
inline bool reader_in_range(const Reader& r, int64_t length) {
    auto i64 = [&](int64_t i) {
        int64_t v;
        std::memcpy(&v, r.b1 + (r.off + i) * 8, 8);
        return v;
    };
    auto i32 = [&](int64_t i) {
        int32_t v;
        std::memcpy(&v, r.b1 + (r.off + i) * 4, 4);
        return static_cast<int64_t>(v);
    };
    switch (r.kind) {
        case ReadKind::Date32:
        case ReadKind::Date64:
        case ReadKind::Timestamp:
        case ReadKind::Time:
        case ReadKind::Duration:
            for (int64_t i = 0; i < length; ++i) {
                if (r.is_null(i)) continue;
                if (r.kind == ReadKind::Date32) {
                    const int64_t d = i32(i);
                    if (d < MIN_DATE_DAYS || d > MAX_DATE_DAYS) return false;
                } else if (r.kind == ReadKind::Date64) {
                    const int64_t d = floor_div(i64(i), 86400000LL);
                    if (d < MIN_DATE_DAYS || d > MAX_DATE_DAYS) return false;
                } else {
                    const bool narrow = r.kind == ReadKind::Time && r.mul != 1 ? (r.mul == US_PER_SECOND || r.mul == 1000) : false;
                    const int64_t raw = (r.kind == ReadKind::Time && narrow) ? i32(i) : i64(i);
                    if (raw > INT64_MAX / r.mul || raw < INT64_MIN / r.mul) return false;
                    const int64_t us = raw * r.mul;
                    if (r.kind == ReadKind::Timestamp && (us < MIN_DATETIME_US || us > MAX_DATETIME_US)) return false;
                    if (r.kind == ReadKind::Time && (us < 0 || us >= US_PER_DAY)) return false;
                    if (r.kind == ReadKind::Duration && (us > MAX_DELTA_DAYS * US_PER_DAY || us < -MAX_DELTA_DAYS * US_PER_DAY)) return false;
                }
            }
            return true;
        case ReadKind::List:
        case ReadKind::LargeList:
        case ReadKind::FixedList:
            return reader_in_range(r.children[0], r.children[0].a->length);
        case ReadKind::Struct:
            for (const Reader& c : r.children) {
                if (!reader_in_range(c, c.a->length)) return false;
            }
            return true;
        default:
            return true;
    }
}

inline int read_value(Store& st, const Reader& r, int64_t i, uint16_t& tag, uint8_t* slot, int depth);

inline int read_string_like(Store& st, const Reader& r, int64_t i, uint16_t& tag, uint8_t* slot, bool large, bool text) {
    int64_t start, stop;
    if (large) {
        std::memcpy(&start, r.b1 + (r.off + i) * 8, 8);
        std::memcpy(&stop, r.b1 + (r.off + i + 1) * 8, 8);
    } else {
        int32_t s32, e32;
        std::memcpy(&s32, r.b1 + (r.off + i) * 4, 4);
        std::memcpy(&e32, r.b1 + (r.off + i + 1) * 4, 4);
        start = s32;
        stop = e32;
    }
    const uint8_t* p = r.b2 + start;
    const int64_t n = stop - start;
    std::memset(slot, 0, SLOT_BYTES);
    if (put_string(st, reinterpret_cast<const char*>(p), static_cast<Py_ssize_t>(n), slot)) {
        tag = text ? make_tag(DRAKEN_VARCHAR, bytes_are_ascii(p, static_cast<uint64_t>(n)) ? PARAM_ASCII : 0)
                   : make_tag(DRAKEN_VARBINARY);
        return 0;
    }
    // beyond what a draken slot addresses: hold the Python object instead
    PyObject* o = text ? PyUnicode_DecodeUTF8(reinterpret_cast<const char*>(p), static_cast<Py_ssize_t>(n), "strict")
                       : PyBytes_FromStringAndSize(reinterpret_cast<const char*>(p), static_cast<Py_ssize_t>(n));
    if (o == nullptr) return -1;
    put_object(st, o, tag, slot);
    Py_DECREF(o);
    return 0;
}

// Value `i` (logical, relative to the array) as a tag + slot in caller memory.
inline int read_value(Store& st, const Reader& r, int64_t i, uint16_t& tag, uint8_t* slot, int depth) {
    std::memset(slot, 0, SLOT_BYTES);
    if (r.is_null(i)) {
        tag = make_tag(DRAKEN_NULL);
        return 0;
    }
    const int64_t k = r.off + i;
    auto put_i64 = [&](int64_t v, DrakenType t) {
        std::memcpy(slot, &v, 8);
        tag = make_tag(t);
        return 0;
    };
    switch (r.kind) {
        case ReadKind::Null:
            tag = make_tag(DRAKEN_NULL);
            return 0;
        case ReadKind::Bool:
            slot[0] = (r.b1[k >> 3] >> (k & 7)) & 1;
            tag = make_tag(DRAKEN_BOOL);
            return 0;
        case ReadKind::Int8: { int8_t v; std::memcpy(&v, r.b1 + k, 1); return put_i64(v, DRAKEN_INT64); }
        case ReadKind::Int16: { int16_t v; std::memcpy(&v, r.b1 + k * 2, 2); return put_i64(v, DRAKEN_INT64); }
        case ReadKind::Int32: { int32_t v; std::memcpy(&v, r.b1 + k * 4, 4); return put_i64(v, DRAKEN_INT64); }
        case ReadKind::Int64: { int64_t v; std::memcpy(&v, r.b1 + k * 8, 8); return put_i64(v, DRAKEN_INT64); }
        case ReadKind::UInt8: return put_i64(r.b1[k], DRAKEN_INT64);
        case ReadKind::UInt16: { uint16_t v; std::memcpy(&v, r.b1 + k * 2, 2); return put_i64(v, DRAKEN_INT64); }
        case ReadKind::UInt32: { uint32_t v; std::memcpy(&v, r.b1 + k * 4, 4); return put_i64(v, DRAKEN_INT64); }
        case ReadKind::UInt64: {
            uint64_t v;
            std::memcpy(&v, r.b1 + k * 8, 8);
            if (v <= static_cast<uint64_t>(INT64_MAX)) return put_i64(static_cast<int64_t>(v), DRAKEN_INT64);
            PyObject* o = PyLong_FromUnsignedLongLong(v);
            if (o == nullptr) return -1;
            put_object(st, o, tag, slot);
            Py_DECREF(o);
            return 0;
        }
        case ReadKind::Float32: {
            float v;
            std::memcpy(&v, r.b1 + k * 4, 4);
            const double d = v;
            std::memcpy(slot, &d, 8);
            tag = make_tag(DRAKEN_FLOAT64);
            return 0;
        }
        case ReadKind::Float64:
            std::memcpy(slot, r.b1 + k * 8, 8);
            tag = make_tag(DRAKEN_FLOAT64);
            return 0;
        case ReadKind::Utf8: return read_string_like(st, r, i, tag, slot, false, true);
        case ReadKind::LargeUtf8: return read_string_like(st, r, i, tag, slot, true, true);
        case ReadKind::Binary: return read_string_like(st, r, i, tag, slot, false, false);
        case ReadKind::LargeBinary: return read_string_like(st, r, i, tag, slot, true, false);
        case ReadKind::Date32: {
            int32_t v;
            std::memcpy(&v, r.b1 + k * 4, 4);
            std::memcpy(slot, &v, 4);
            tag = make_tag(DRAKEN_DATE32);
            return 0;
        }
        case ReadKind::Date64: {  // to_pylist() gives the date, dropping any time part
            int64_t ms;
            std::memcpy(&ms, r.b1 + k * 8, 8);
            const int32_t days = static_cast<int32_t>(floor_div(ms, 86400000LL));
            std::memcpy(slot, &days, 4);
            tag = make_tag(DRAKEN_DATE32);
            return 0;
        }
        case ReadKind::Timestamp: {
            int64_t v;
            std::memcpy(&v, r.b1 + k * 8, 8);
            return put_i64(v * r.mul, DRAKEN_TIMESTAMP64);
        }
        case ReadKind::Time: {
            int64_t raw;
            if (r.mul == 1) {
                std::memcpy(&raw, r.b1 + k * 8, 8);
            } else {
                int32_t v;
                std::memcpy(&v, r.b1 + k * 4, 4);
                raw = v;
            }
            return put_i64(raw * r.mul, DRAKEN_TIME64);
        }
        case ReadKind::Duration: {
            int64_t v;
            std::memcpy(&v, r.b1 + k * 8, 8);
            DrakenIntervalSlot iv;
            iv.months = 0;
            iv.us = v * r.mul;
            std::memcpy(slot, &iv, sizeof iv);
            tag = make_tag(DRAKEN_INTERVAL);
            return 0;
        }
        case ReadKind::Decimal128:
            std::memcpy(slot, r.b1 + k * 16, 16);  // little-endian __int128, as Arrow lays it out
            tag = make_tag(DRAKEN_DECIMAL128, r.scale);
            return 0;
        case ReadKind::List:
        case ReadKind::LargeList:
        case ReadKind::FixedList: {
            int64_t start, stop;
            if (r.kind == ReadKind::FixedList) {
                start = k * r.fixed;
                stop = start + r.fixed;
            } else if (r.kind == ReadKind::List) {
                int32_t s32, e32;
                std::memcpy(&s32, r.b1 + k * 4, 4);
                std::memcpy(&e32, r.b1 + (k + 1) * 4, 4);
                start = s32;
                stop = e32;
            } else {
                std::memcpy(&start, r.b1 + k * 8, 8);
                std::memcpy(&stop, r.b1 + (k + 1) * 8, 8);
            }
            const uint64_t n = static_cast<uint64_t>(stop - start);
            const uint64_t block = nested_begin(st, n, NEST_LIST);
            for (uint64_t e = 0; e < n; ++e) {
                uint8_t child[SLOT_BYTES];
                uint16_t ctag = 0;
                if (read_value(st, r.children[0], start + static_cast<int64_t>(e), ctag, child, depth + 1) < 0) return -1;
                nested_set(st, block, n, e, ctag, child);
            }
            nested_finish(block, static_cast<uint32_t>(n), NEST_LIST, tag, slot);
            return 0;
        }
        case ReadKind::Struct: {  // to_pylist() gives {field name: value}
            const uint64_t pairs = r.children.size();
            const uint64_t elements = pairs * 2;
            const uint64_t block = nested_begin(st, elements, NEST_DICT);
            for (uint64_t c = 0; c < pairs; ++c) {
                uint8_t child[SLOT_BYTES];
                std::memset(child, 0, SLOT_BYTES);
                const std::string& name = r.names[c];
                if (!put_string(st, name.data(), static_cast<Py_ssize_t>(name.size()), child)) {
                    PyErr_SetString(PyExc_MemoryError, "orso: arena full");
                    return -1;
                }
                nested_set(st, block, elements, 2 * c,
                           make_tag(DRAKEN_VARCHAR, bytes_are_ascii(reinterpret_cast<const uint8_t*>(name.data()), name.size()) ? PARAM_ASCII : 0),
                           child);
                uint16_t ctag = 0;
                if (read_value(st, r.children[c], k, ctag, child, depth + 1) < 0) return -1;
                nested_set(st, block, elements, 2 * c + 1, ctag, child);
            }
            nested_finish(block, static_cast<uint32_t>(pairs), NEST_DICT, tag, slot);
            return 0;
        }
    }
    PyErr_SetString(PyExc_SystemError, "orso: unhandled Arrow reader");
    return -1;
}

// Import up to `limit` rows (all when negative) of one RecordBatch into the frame's store.
// Returns the number of rows imported.
inline int64_t import_batch(Store& st, PyObject* batch, int64_t limit) {
    PyObject* pair = PyObject_CallMethod(batch, "__arrow_c_array__", nullptr);
    if (pair == nullptr) return -1;
    struct PairGuard {
        PyObject* p;
        ~PairGuard() { Py_DECREF(p); }
    } guard{pair};
    if (!PyTuple_Check(pair) || PyTuple_GET_SIZE(pair) != 2) {
        PyErr_SetString(PyExc_TypeError, "__arrow_c_array__ must return (schema, array) capsules");
        return -1;
    }
    auto* schema = static_cast<ArrowSchema*>(PyCapsule_GetPointer(PyTuple_GET_ITEM(pair, 0), "arrow_schema"));
    if (schema == nullptr) return -1;
    auto* array = static_cast<ArrowArray*>(PyCapsule_GetPointer(PyTuple_GET_ITEM(pair, 1), "arrow_array"));
    if (array == nullptr) return -1;
    if (std::strcmp(schema->format, "+s") != 0 || array->n_children != schema->n_children) {
        PyErr_SetString(PyExc_TypeError, "orso: expected a record batch (struct array)");
        return -1;
    }
    if (static_cast<uint32_t>(array->n_children) != st.nfields) {
        PyErr_Format(PyExc_ValueError, "orso: batch has %lld columns but the frame has %u",
                     static_cast<long long>(array->n_children), st.nfields);
        return -1;
    }
    int64_t n = array->length;
    if (limit >= 0 && limit < n) n = limit;
    const int64_t base = array->offset;  // a sliced batch offsets its children by this

    std::vector<Reader> readers(st.nfields);
    std::vector<PyObject*> fallback(st.nfields, nullptr);  // to_pylist() lists, owned
    struct FallbackGuard {
        std::vector<PyObject*>& v;
        ~FallbackGuard() {
            for (PyObject* o : v) Py_XDECREF(o);
        }
    } fguard{fallback};
    for (uint32_t j = 0; j < st.nfields; ++j) {
        const bool native = make_reader(schema->children[j], array->children[j], readers[j], 0) &&
                            reader_in_range(readers[j], array->children[j]->length);
        if (native) continue;
        PyObject* column = PyObject_CallMethod(batch, "column", "I", j);
        if (column == nullptr) return -1;
        PyObject* values = PyObject_CallMethod(column, "to_pylist", nullptr);
        Py_DECREF(column);
        if (values == nullptr) return -1;
        if (!PyList_Check(values) || PyList_GET_SIZE(values) < n) {
            Py_DECREF(values);
            PyErr_SetString(PyExc_SystemError, "orso: to_pylist() returned a short list");
            return -1;
        }
        fallback[j] = values;
    }

    for (int64_t i = 0; i < n; ++i) {
        const size_t records_before = st.records.size();
        const size_t arena_before = st.arena.size();
        const size_t objects_before = st.objects.size();
        auto rollback = [&]() {
            for (size_t q = objects_before; q < st.objects.size(); ++q) Py_DECREF(st.objects[q]);
            st.objects.resize(objects_before);
            st.records.resize_uninit(records_before);
            st.arena.resize_uninit(arena_before);
        };
        try {
            st.records.extend(st.stride);
            if (st.slot_offset) std::memset(st.records.data() + records_before, 0, st.slot_offset);
            for (uint32_t j = 0; j < st.nfields; ++j) {
                uint8_t slot[SLOT_BYTES];
                uint16_t tag = 0;
                const int r = fallback[j] != nullptr ? encode_value(st, PyList_GET_ITEM(fallback[j], i), tag, slot)
                                                     : read_value(st, readers[j], base + i, tag, slot, 0);
                if (r < 0) {
                    rollback();
                    return -1;
                }
                uint8_t* rec = st.records.data() + records_before;
                std::memcpy(rec + 2 * j, &tag, sizeof tag);
                std::memcpy(rec + st.slot_offset + SLOT_BYTES * j, slot, SLOT_BYTES);
            }
        } catch (...) {
            rollback();
            throw;
        }
        st.rows += 1;
    }
    return n;
}

}  // namespace orso
