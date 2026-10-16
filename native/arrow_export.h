#pragma once
// orso/native/arrow_export.h — record store columns out as Arrow arrays (DESIGN.md §4.8).
//
// v1 built one Python list per column and let pyarrow convert it value by value. Here a
// builder per column reads the slots and writes Arrow buffers directly; the result goes
// to pyarrow through the C Data Interface as PyCapsules (`__arrow_c_array__`), and the
// release callback frees the buffers. Buffers are copies (decision 4), so the frame can
// keep taking appends after an export.
//
// The contract is that a natively built array equals what pyarrow would have built from
// v1's Python list. Each target type therefore accepts exactly the slot kinds pyarrow's
// converter accepts (measured, and pinned by tests/test_native_arrow.py):
//
//   int64            INT64                       (bool is rejected by pyarrow)
//   float64          FLOAT64, INT64 with |v| <= 2^53 (pyarrow refuses inexact ints)
//   bool             BOOL
//   string           VARCHAR
//   binary           VARBINARY, VARCHAR (as UTF-8)
//   timestamp[us]    TIMESTAMP64
//   date32           DATE32, TIMESTAMP64 (pyarrow truncates a datetime to its date)
//   time32[ms]       TIME64 (truncated to the millisecond, as pyarrow does)
//   time64[us]       TIME64
//   duration[us]     INTERVAL
//   month_day_nano   INTERVAL -> (0, days, nanoseconds), timedelta's own normalisation
//   decimal128(p,s)  DECIMAL128 rescaled exactly; a lossy or overflowing rescale is not native
//   list<T>          lists and tuples whose elements T accepts
//   struct<...>      dicts, by field name; missing keys are null, extra keys ignored
//   null             NULL
//
// Anything else in a column — an object slot, a value the target refuses — makes the
// column "not native" and the caller hands v1's Python list to pyarrow, which converts it
// or raises exactly as it did in v1. `infer` (list-of-names frames) picks the type pyarrow
// would infer when a column holds one kind of value, and declines otherwise.

#include <Python.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "store.h"
#include "interop/arrow_c_data_interface.h"

namespace orso {

enum class OutKind {
    Null, Bool, Int64, Float64, Utf8, Binary, TimestampUs, Date32, Time32Ms, Time64Us, DurationUs,
    MonthDayNano, Decimal128, List, Struct, Infer, Skip,
};

struct OutSpec {
    OutKind kind = OutKind::Infer;
    int precision = 0;
    int scale = 0;
    std::vector<OutSpec> children;
    std::vector<std::string> names;
    std::vector<bool> nullable;
};

// Spec tuples from Python: ("int64",) ("decimal128", p, s) ("list", spec)
// ("struct", ((name, spec, nullable), ...)) ("infer",) and so on.
inline OutSpec parse_spec(nb::handle h) {
    nb::tuple t = nb::cast<nb::tuple>(h);
    const std::string k = nb::cast<std::string>(t[0]);
    OutSpec s;
    static const std::pair<const char*, OutKind> simple[] = {
        {"null", OutKind::Null}, {"bool", OutKind::Bool}, {"int64", OutKind::Int64},
        {"float64", OutKind::Float64}, {"string", OutKind::Utf8}, {"binary", OutKind::Binary},
        {"timestamp_us", OutKind::TimestampUs}, {"date32", OutKind::Date32}, {"time32_ms", OutKind::Time32Ms},
        {"time64_us", OutKind::Time64Us}, {"duration_us", OutKind::DurationUs},
        {"month_day_nano", OutKind::MonthDayNano}, {"infer", OutKind::Infer}, {"skip", OutKind::Skip},
    };
    for (const auto& [name, kind] : simple) {
        if (k == name) {
            s.kind = kind;
            return s;
        }
    }
    if (k == "decimal128") {
        s.kind = OutKind::Decimal128;
        s.precision = nb::cast<int>(t[1]);
        s.scale = nb::cast<int>(t[2]);
        if (s.precision < 1 || s.precision > DEC_MAX_DIGITS || s.scale < 0 || s.scale > s.precision) {
            throw nb::value_error("decimal128 spec out of range");
        }
        return s;
    }
    if (k == "list") {
        s.kind = OutKind::List;
        s.children.push_back(parse_spec(t[1]));
        return s;
    }
    if (k == "struct") {
        s.kind = OutKind::Struct;
        for (nb::handle f : t[1]) {
            nb::tuple ft = nb::cast<nb::tuple>(f);
            s.names.push_back(nb::cast<std::string>(ft[0]));
            s.children.push_back(parse_spec(ft[1]));
            s.nullable.push_back(nb::cast<bool>(ft[2]));
        }
        return s;
    }
    throw nb::value_error(("unknown export spec: " + k).c_str());
}

// ----- ArrowArray / ArrowSchema ownership -------------------------------------------

struct ArrayPrivate {
    std::vector<void*> owned;               // draken_malloc'd buffers
    std::vector<const void*> buffers;       // what ArrowArray.buffers points at
    std::vector<ArrowArray*> children;      // owned, each with its own release
};

inline void release_array(ArrowArray* a) {
    if (a == nullptr || a->release == nullptr) return;
    auto* p = static_cast<ArrayPrivate*>(a->private_data);
    for (ArrowArray* c : p->children) {
        if (c->release) c->release(c);
        delete c;
    }
    for (void* b : p->owned) draken_free(b);
    delete p;
    a->release = nullptr;
}

struct SchemaPrivate {
    std::string format;
    std::string name;
    std::vector<ArrowSchema*> children;
};

inline void release_schema(ArrowSchema* s) {
    if (s == nullptr || s->release == nullptr) return;
    auto* p = static_cast<SchemaPrivate*>(s->private_data);
    for (ArrowSchema* c : p->children) {
        if (c->release) c->release(c);
        delete c;
    }
    delete p;
    s->release = nullptr;
}

inline void capsule_free_schema(PyObject* cap) {
    auto* s = static_cast<ArrowSchema*>(PyCapsule_GetPointer(cap, "arrow_schema"));
    if (s == nullptr) {
        PyErr_Clear();
        return;
    }
    release_schema(s);
    delete s;
}

inline void capsule_free_array(PyObject* cap) {
    auto* a = static_cast<ArrowArray*>(PyCapsule_GetPointer(cap, "arrow_array"));
    if (a == nullptr) {
        PyErr_Clear();
        return;
    }
    release_array(a);
    delete a;
}

// ----- builders -----------------------------------------------------------------------

constexpr int64_t MAX_EXACT_DOUBLE_INT = 9007199254740992LL;  // 2^53

class Builder {
public:
    OutSpec spec;
    int64_t length = 0;
    int64_t null_count = 0;
    draken::AppendBuffer<uint8_t> validity;  // bitmap, LSB first
    draken::AppendBuffer<uint8_t> values;    // fixed-width values, or the bool bitmap
    draken::AppendBuffer<int32_t> offsets;   // string / binary / list
    draken::AppendBuffer<uint8_t> data;      // string / binary bytes
    std::vector<Builder> children;

    explicit Builder(OutSpec s) : spec(std::move(s)) {
        if (spec.kind == OutKind::Utf8 || spec.kind == OutKind::Binary || spec.kind == OutKind::List) offsets.push_back(0);
        for (const OutSpec& c : spec.children) children.emplace_back(c);
    }

    static void set_bit(draken::AppendBuffer<uint8_t>& bits, int64_t i, bool v) {
        if ((i >> 3) >= static_cast<int64_t>(bits.size())) bits.push_back(0);
        if (v) bits[static_cast<size_t>(i >> 3)] |= static_cast<uint8_t>(1u << (i & 7));
    }

    template <typename T>
    void push_fixed(T v) {
        values.append(reinterpret_cast<const uint8_t*>(&v), sizeof v);
    }

    void push_zero_value() {
        switch (spec.kind) {
            case OutKind::Bool: set_bit(values, length, false); break;
            case OutKind::Int64: case OutKind::Float64: case OutKind::TimestampUs: case OutKind::Time64Us:
            case OutKind::DurationUs: push_fixed<int64_t>(0); break;
            case OutKind::Date32: case OutKind::Time32Ms: push_fixed<int32_t>(0); break;
            case OutKind::MonthDayNano: case OutKind::Decimal128: push_fixed<__int128>(0); break;
            case OutKind::Utf8: case OutKind::Binary: case OutKind::List: {
                const int32_t last = offsets.back();  // a copy: push_back may reallocate under a reference
                offsets.push_back(last);
                break;
            }
            case OutKind::Struct: for (Builder& c : children) c.append_null(); break;
            default: break;
        }
    }

    void append_null() {
        set_bit(validity, length, false);
        push_zero_value();
        ++null_count;
        ++length;
    }

    // False when this value is not something pyarrow would convert to this type exactly.
    bool append(const Store& st, uint16_t tag, const uint8_t* slot) {
        const unsigned t = tag & 0xFFu;
        if (t == DRAKEN_NULL) {
            if (spec.kind == OutKind::Null) {
                ++null_count;
                ++length;
                return true;
            }
            append_null();
            return true;
        }
        switch (spec.kind) {
            case OutKind::Null:
                return false;
            case OutKind::Bool:
                if (t != DRAKEN_BOOL) return false;
                set_bit(values, length, slot[0] != 0);
                break;
            case OutKind::Int64:
                if (t != DRAKEN_INT64) return false;
                push_fixed<int64_t>(load_i64(slot));
                break;
            case OutKind::Float64:
                if (t == DRAKEN_FLOAT64) {
                    double d;
                    std::memcpy(&d, slot, 8);
                    push_fixed<double>(d);
                } else if (t == DRAKEN_INT64) {
                    const int64_t v = load_i64(slot);
                    if (v > MAX_EXACT_DOUBLE_INT || v < -MAX_EXACT_DOUBLE_INT) return false;
                    push_fixed<double>(static_cast<double>(v));
                } else {
                    return false;
                }
                break;
            case OutKind::Utf8:
            case OutKind::Binary: {
                if (!(t == DRAKEN_VARCHAR || (t == DRAKEN_VARBINARY && spec.kind == OutKind::Binary))) return false;
                const auto* s = reinterpret_cast<const DrakenStringSlot*>(slot);
                const uint32_t n = str_length(s);
                if (static_cast<int64_t>(offsets.back()) + n > INT32_MAX) return false;  // pyarrow would chunk or go large
                data.append(str_data(s, st.arena.data()), n);
                const int32_t next = offsets.back() + static_cast<int32_t>(n);  // copy before a possible realloc
                offsets.push_back(next);
                break;
            }
            case OutKind::TimestampUs:
                if (t != DRAKEN_TIMESTAMP64) return false;
                push_fixed<int64_t>(load_i64(slot));
                break;
            case OutKind::Date32:
                if (t == DRAKEN_DATE32) {
                    int32_t d;
                    std::memcpy(&d, slot, 4);
                    push_fixed<int32_t>(d);
                } else if (t == DRAKEN_TIMESTAMP64) {
                    int64_t us = load_i64(slot);
                    int64_t days = us / US_PER_DAY;
                    if (us % US_PER_DAY < 0) --days;
                    push_fixed<int32_t>(static_cast<int32_t>(days));
                } else {
                    return false;
                }
                break;
            case OutKind::Time32Ms:
                if (t != DRAKEN_TIME64) return false;
                push_fixed<int32_t>(static_cast<int32_t>(load_i64(slot) / 1000));
                break;
            case OutKind::Time64Us:
                if (t != DRAKEN_TIME64) return false;
                push_fixed<int64_t>(load_i64(slot));
                break;
            case OutKind::DurationUs:
            case OutKind::MonthDayNano: {
                if (t != DRAKEN_INTERVAL) return false;
                DrakenIntervalSlot iv;
                std::memcpy(&iv, slot, sizeof iv);
                if (iv.months != 0) return false;
                if (spec.kind == OutKind::DurationUs) {
                    push_fixed<int64_t>(iv.us);
                } else {
                    int64_t days = iv.us / US_PER_DAY;
                    int64_t rem = iv.us % US_PER_DAY;
                    if (rem < 0) {
                        rem += US_PER_DAY;
                        days -= 1;
                    }
                    const int32_t months = 0;
                    const int32_t d32 = static_cast<int32_t>(days);
                    const int64_t ns = rem * 1000;
                    push_fixed<int32_t>(months);
                    push_fixed<int32_t>(d32);
                    push_fixed<int64_t>(ns);
                }
                break;
            }
            case OutKind::Decimal128: {
                if (t != DRAKEN_DECIMAL128) return false;
                __int128 v = load_i128(slot);
                const int from = tag_param(tag) & DEC_SCALE_MASK;
                const int to = spec.scale;
                if (from < to) {
                    const __int128 m = dec_pow10(to - from);
                    const __int128 lim = dec_pow10(DEC_MAX_DIGITS) / m;
                    if (v >= lim || v <= -lim) return false;
                    v *= m;
                } else if (from > to) {
                    const __int128 m = dec_pow10(from - to);
                    if (v % m != 0) return false;  // pyarrow: "would cause data loss"
                    v /= m;
                }
                const __int128 cap = dec_pow10(spec.precision);
                if (v >= cap || v <= -cap) return false;
                push_fixed<__int128>(v);
                break;
            }
            case OutKind::List: {
                if (t != DRAKEN_ARRAY) return false;
                const uint8_t kind = tag_param(tag);
                if (kind != NEST_LIST && kind != NEST_TUPLE) return false;
                const NestedRef ref = nested_ref(slot);
                for (uint64_t k = 0; k < ref.count; ++k) {
                    if (!children[0].append(st, nested_elem_tag(st, ref.block, k),
                                            nested_elem_slot(st, ref.block, ref.count, k))) {
                        return false;
                    }
                }
                if (children[0].length > INT32_MAX) return false;
                offsets.push_back(static_cast<int32_t>(children[0].length));
                break;
            }
            case OutKind::Struct: {
                if (t != DRAKEN_ARRAY || tag_param(tag) != NEST_DICT) return false;
                const NestedRef ref = nested_ref(slot);
                const uint64_t elements = uint64_t(ref.count) * 2;
                for (size_t c = 0; c < children.size(); ++c) {
                    const std::string& name = spec.names[c];
                    bool found = false;
                    for (uint64_t k = 0; k < elements; k += 2) {
                        if ((nested_elem_tag(st, ref.block, k) & 0xFFu) != DRAKEN_VARCHAR) continue;
                        const auto* ks = reinterpret_cast<const DrakenStringSlot*>(nested_elem_slot(st, ref.block, elements, k));
                        if (str_length(ks) != name.size() ||
                            std::memcmp(str_data(ks, st.arena.data()), name.data(), name.size()) != 0) {
                            continue;
                        }
                        if (!children[c].append(st, nested_elem_tag(st, ref.block, k + 1),
                                                nested_elem_slot(st, ref.block, elements, k + 1))) {
                            return false;
                        }
                        found = true;
                        break;
                    }
                    if (!found) children[c].append_null();
                }
                break;
            }
            case OutKind::Infer:
            case OutKind::Skip:
                return false;
        }
        set_bit(validity, length, true);
        ++length;
        return true;
    }

    // ----- hand-off ----------------------------------------------------------------

    static void* own(ArrayPrivate* p, draken::AppendBuffer<uint8_t>& b) {
        if (b.empty()) b.push_back(0);  // never hand Arrow a null pointer for a present buffer
        void* raw = b.release();
        p->owned.push_back(raw);
        return raw;
    }

    ArrowArray* finish_array() {
        auto* a = new ArrowArray();
        auto* p = new ArrayPrivate();
        a->length = length;
        a->null_count = null_count;
        a->offset = 0;
        a->dictionary = nullptr;
        a->private_data = p;
        a->release = release_array;
        const void* valid = nullptr;
        if (spec.kind != OutKind::Null && null_count > 0) valid = own(p, validity);
        switch (spec.kind) {
            case OutKind::Null:
                break;
            case OutKind::Utf8:
            case OutKind::Binary: {
                p->buffers = {valid, nullptr, nullptr};
                void* off = offsets.release();
                p->owned.push_back(off);
                p->buffers[1] = off;
                p->buffers[2] = own(p, data);
                break;
            }
            case OutKind::List: {
                p->buffers = {valid, nullptr};
                void* off = offsets.release();
                p->owned.push_back(off);
                p->buffers[1] = off;
                p->children.push_back(children[0].finish_array());
                break;
            }
            case OutKind::Struct:
                p->buffers = {valid};
                for (Builder& c : children) p->children.push_back(c.finish_array());
                break;
            default:
                p->buffers = {valid, own(p, values)};
                break;
        }
        a->n_buffers = static_cast<int64_t>(p->buffers.size());
        a->buffers = p->buffers.empty() ? nullptr : p->buffers.data();
        a->n_children = static_cast<int64_t>(p->children.size());
        a->children = p->children.empty() ? nullptr : p->children.data();
        return a;
    }

    ArrowSchema* finish_schema(const std::string& name, bool nullable) const {
        auto* s = new ArrowSchema();
        auto* p = new SchemaPrivate();
        p->name = name;
        switch (spec.kind) {
            case OutKind::Null: p->format = "n"; break;
            case OutKind::Bool: p->format = "b"; break;
            case OutKind::Int64: p->format = "l"; break;
            case OutKind::Float64: p->format = "g"; break;
            case OutKind::Utf8: p->format = "u"; break;
            case OutKind::Binary: p->format = "z"; break;
            case OutKind::TimestampUs: p->format = "tsu:"; break;
            case OutKind::Date32: p->format = "tdD"; break;
            case OutKind::Time32Ms: p->format = "ttm"; break;
            case OutKind::Time64Us: p->format = "ttu"; break;
            case OutKind::DurationUs: p->format = "tDu"; break;
            case OutKind::MonthDayNano: p->format = "tin"; break;
            case OutKind::Decimal128:
                p->format = "d:" + std::to_string(spec.precision) + "," + std::to_string(spec.scale);
                break;
            case OutKind::List:
                p->format = "+l";
                p->children.push_back(children[0].finish_schema("item", true));
                break;
            case OutKind::Struct:
                p->format = "+s";
                for (size_t c = 0; c < children.size(); ++c) {
                    p->children.push_back(children[c].finish_schema(spec.names[c], spec.nullable[c]));
                }
                break;
            case OutKind::Infer:
            case OutKind::Skip: p->format = "n"; break;
        }
        s->format = p->format.c_str();
        s->name = p->name.c_str();
        s->metadata = nullptr;
        s->flags = nullable ? ARROW_FLAG_NULLABLE : 0;
        s->n_children = static_cast<int64_t>(p->children.size());
        s->children = p->children.empty() ? nullptr : p->children.data();
        s->dictionary = nullptr;
        s->release = release_schema;
        s->private_data = p;
        return s;
    }
};

// The type pyarrow infers for a column of one kind of value, or Infer when the column is
// mixed (or holds something whose inference this does not reproduce).
inline OutKind infer_kind(const Store& st, uint64_t offset, uint64_t n, uint32_t j) {
    unsigned seen = DRAKEN_NULL;
    for (uint64_t i = 0; i < n; ++i) {
        const unsigned t = Store::tag_at(st.record(offset + i), j) & 0xFFu;
        if (t == DRAKEN_NULL) continue;
        if (seen == DRAKEN_NULL) {
            seen = t;
        } else if (t != seen) {
            return OutKind::Infer;
        }
    }
    switch (seen) {
        case DRAKEN_NULL: return OutKind::Null;
        case DRAKEN_BOOL: return OutKind::Bool;
        case DRAKEN_INT64: return OutKind::Int64;
        case DRAKEN_FLOAT64: return OutKind::Float64;
        case DRAKEN_VARCHAR: return OutKind::Utf8;
        case DRAKEN_VARBINARY: return OutKind::Binary;
        case DRAKEN_TIMESTAMP64: return OutKind::TimestampUs;
        case DRAKEN_DATE32: return OutKind::Date32;
        case DRAKEN_TIME64: return OutKind::Time64Us;
        case DRAKEN_INTERVAL: return OutKind::DurationUs;
        default: return OutKind::Infer;  // decimals, nested values, objects: pyarrow decides
    }
}

// (schema capsule, array capsule) for field j of rows [offset, offset + n), or None.
inline nb::object export_column(const Store& st, uint64_t offset, uint64_t n, uint32_t j, OutSpec spec) {
    if (spec.kind == OutKind::Skip) return nb::none();  // the caller has pyarrow convert this one
    if (spec.kind == OutKind::Infer) {
        spec.kind = infer_kind(st, offset, n, j);
        if (spec.kind == OutKind::Infer) return nb::none();
    }
    Builder b(spec);
    for (uint64_t i = 0; i < n; ++i) {
        const uint8_t* rec = st.record(offset + i);
        if (!b.append(st, Store::tag_at(rec, j), st.slot_at(rec, j))) return nb::none();
    }
    ArrowSchema* schema = b.finish_schema("", true);
    ArrowArray* array = b.finish_array();
    PyObject* sc = PyCapsule_New(schema, "arrow_schema", capsule_free_schema);
    if (sc == nullptr) {
        release_schema(schema);
        delete schema;
        release_array(array);
        delete array;
        raise_python();
    }
    PyObject* ac = PyCapsule_New(array, "arrow_array", capsule_free_array);
    if (ac == nullptr) {
        Py_DECREF(sc);
        release_array(array);
        delete array;
        raise_python();
    }
    PyObject* pair = PyTuple_Pack(2, sc, ac);
    Py_DECREF(sc);
    Py_DECREF(ac);
    if (pair == nullptr) raise_python();
    return nb::steal(pair);
}

}  // namespace orso
