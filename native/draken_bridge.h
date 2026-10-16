#pragma once
// orso/native/draken_bridge.h — a store window <-> draken Vectors (DESIGN.md §4.8, phase 7).
//
// Optional and resolved at runtime. orso never links draken: `import orso` works without
// it. The Python side (orso/_draken.py) imports draken, which loads draken_native with
// RTLD_GLOBAL, then hands its path to bind(), which looks the bridge symbols up in that
// already-loaded library. Nothing here is usable until bind() succeeds.
//
// Export (to_draken) is one gather per field: the window's slots for field j become a
// dense draken data buffer, the tags become the validity bitmap, and for strings the
// slots plus a compact copy of only that field's long-string bytes become a string
// block. Buffers are allocated the way draken_malloc allocates (libc) and charged to
// draken's own memory account through its exported symbol, because draken frees them
// with draken_free, which uncharges that account. orso's vendored mem_account.cpp is a
// private copy (hidden visibility); charging it would leave draken's account short.
//
// Import (from_draken) is the reverse scatter, field by field into pre-sized records:
// data[selection[i]] under the logical-row validity bit, so dense, dict and constant
// vectors all read the same way. A type orso cannot hold in a slot exactly (arrays,
// UUIDs, fp16 vectors, IPv4, intervals with months, UINT64 above INT64_MAX) falls back
// for that field to draken's own to_pylist() values, encoded like any appended value.

#include <Python.h>
#include <dlfcn.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include <nanobind/nanobind.h>

#include "core/alloc.h"  // DRAKEN_USABLE_SIZE, DRAKEN_MEM_ACCOUNT_MIN
#include "core/buffers.h"
#include "core/interval_slot.h"
#include "core/string_slot.h"
#include "decimal.h"
#include "record.h"
#include "store.h"

namespace orso {

namespace nb = nanobind;

// ----- symbols -----------------------------------------------------------------------

// LogicalKind / TimestampUnit ordinals (draken/logical_type.h), as the bridge takes them.
constexpr uint8_t DK_LOGICAL_NONE = 0;
constexpr uint8_t DK_LOGICAL_TIMESTAMP = 1;
constexpr uint8_t DK_LOGICAL_TIME = 2;
constexpr uint8_t DK_LOGICAL_DECIMAL = 3;
constexpr uint8_t DK_UNIT_S = 0;
constexpr uint8_t DK_UNIT_MS = 1;
constexpr uint8_t DK_UNIT_US = 2;
constexpr uint8_t DK_UNIT_NS = 3;

struct DrakenApi {
    bool bound = false;
    PyObject* (*own_raw_with_arena)(void*, uint8_t*, uint8_t*, uint32_t, DrakenType) = nullptr;
    PyObject* (*own_raw_logical)(void*, uint8_t*, uint32_t, DrakenType, uint8_t, uint8_t, int16_t, uint8_t, uint8_t,
                                 uint32_t) = nullptr;
    const DrakenVector* (*unwrap)(PyObject*) = nullptr;
    void (*mem_charge)(int64_t) = nullptr;
    void (*mem_uncharge)(int64_t) = nullptr;
};

inline DrakenApi g_draken;

// Resolve the bridge in the draken_native library at `path`, which must already be
// loaded (RTLD_NOLOAD: this never loads a second copy). Idempotent.
inline void draken_bind(const std::string& path) {
    if (g_draken.bound) return;
    void* h = dlopen(path.c_str(), RTLD_NOW | RTLD_NOLOAD);
    if (h == nullptr) throw nb::import_error(("orso: draken_native is not loaded: " + path).c_str());
    DrakenApi api;
    auto need = [&](const char* name) {
        void* p = dlsym(h, name);
        if (p == nullptr) {
            throw nb::import_error(
                (std::string("orso: this draken has no '") + name + "'; it is older than the bridge expects").c_str());
        }
        return p;
    };
    api.own_raw_with_arena = reinterpret_cast<decltype(api.own_raw_with_arena)>(need("draken_vector_own_raw_with_arena"));
    api.own_raw_logical = reinterpret_cast<decltype(api.own_raw_logical)>(need("draken_vector_own_raw_logical"));
    api.unwrap = reinterpret_cast<decltype(api.unwrap)>(need("draken_vector_unwrap"));
    api.mem_charge = reinterpret_cast<decltype(api.mem_charge)>(need("draken_mem_charge"));
    api.mem_uncharge = reinterpret_cast<decltype(api.mem_uncharge)>(need("draken_mem_uncharge"));
    api.bound = true;
    g_draken = api;  // the handle stays open for the process: draken is never unloaded
}

inline void require_bound() {
    if (!g_draken.bound) throw nb::import_error("orso: the draken bridge is not bound (import it through orso)");
}

// ----- buffers handed to draken ----------------------------------------------------

// draken_malloc as draken_native would run it: libc, charged to draken's account.
inline void* dk_malloc(size_t n) {
    void* p = std::malloc(n ? n : 1);
    if (p == nullptr) throw std::bad_alloc();
    const int64_t usable = static_cast<int64_t>(DRAKEN_USABLE_SIZE(p));
    if (usable >= DRAKEN_MEM_ACCOUNT_MIN) g_draken.mem_charge(usable);
    return p;
}

inline void* dk_calloc(size_t n) {
    void* p = dk_malloc(n);
    std::memset(p, 0, n ? n : 1);
    return p;
}

// For buffers that never reached draken (an error before the hand-off).
inline void dk_free(void* p) noexcept {
    if (p == nullptr) return;
    const int64_t usable = static_cast<int64_t>(DRAKEN_USABLE_SIZE(p));
    if (usable >= DRAKEN_MEM_ACCOUNT_MIN) g_draken.mem_uncharge(usable);
    std::free(p);
}

// Owns draken-bound buffers until the hand-off; release() once draken has them.
struct DkBuffers {
    void* data = nullptr;
    uint8_t* arena = nullptr;
    uint8_t* validity = nullptr;
    ~DkBuffers() {
        dk_free(data);
        dk_free(arena);
        dk_free(validity);
    }
    void release() noexcept { data = arena = validity = nullptr; }
};

// ----- export ------------------------------------------------------------------------

// What Python asks for per field: a draken type from the schema (0 = infer from the
// tags) and, for decimals, precision and scale.
struct DrakenSpec {
    int target = 0;
    int precision = 0;
    int scale = -1;
};

// The outcome per field.
enum class ExportKind : int { Vector = 0, AllNull = 1, Fallback = 2 };

inline bool is_supported_target(int t) {
    switch (t) {
        case DRAKEN_BOOL:
        case DRAKEN_INT64:
        case DRAKEN_FLOAT64:
        case DRAKEN_VARCHAR:
        case DRAKEN_VARBINARY:
        case DRAKEN_DATE32:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME64:
        case DRAKEN_INTERVAL:
        case DRAKEN_DECIMAL128:
            return true;
        default:
            return false;
    }
}

inline __int128 pow10_i128(int k) noexcept {
    __int128 p = 1;
    for (int i = 0; i < k; ++i) p *= 10;
    return p;
}

// The draken type field j's window can be exported as, or 0 when it cannot (fallback),
// or DRAKEN_NULL when every row is null. Also fills the decimal scale to export at.
inline int resolve_export_type(const Store& st, uint64_t offset, uint64_t n, uint32_t j, const DrakenSpec& spec,
                               int& dec_scale) {
    bool any = false, all_int = true, all_same = true;
    DrakenType first = DRAKEN_NULL;
    int max_scale = 0;
    for (uint64_t i = 0; i < n; ++i) {
        const uint16_t tag = Store::tag_at(st.record(offset + i), j);
        if ((tag & 0xFFu) == TAG_OBJECT) return 0;
        const DrakenType t = tag_type(tag);
        if (t == DRAKEN_NULL) continue;
        if (t == DRAKEN_ARRAY) return 0;  // nested values: draken builds those itself
        if (t == DRAKEN_DECIMAL128) {
            const int s = tag_param(tag) & DEC_SCALE_MASK;
            if (s > max_scale) max_scale = s;
        }
        if (!any) {
            first = t;
            any = true;
        } else if (t != first) {
            all_same = false;
        }
        if (t != DRAKEN_INT64 && t != DRAKEN_FLOAT64) all_int = false;
    }
    if (spec.target != 0) {
        if (!is_supported_target(spec.target)) return 0;
        if (!any) return DRAKEN_NULL;  // typed but all null: still that type, all invalid
        if (spec.target == DRAKEN_FLOAT64 && all_int) {
            dec_scale = 0;
            return DRAKEN_FLOAT64;
        }
        if (!all_same || first != static_cast<DrakenType>(spec.target)) return 0;
        if (first == DRAKEN_DECIMAL128) {
            const int s = spec.scale >= 0 ? spec.scale : max_scale;
            if (max_scale > s) return 0;  // a value with more places than the column holds
            dec_scale = s;
        }
        return first;
    }
    if (!any) return DRAKEN_NULL;
    if (all_same) {
        if (first == DRAKEN_DECIMAL128) dec_scale = max_scale;
        return is_supported_target(first) ? first : 0;
    }
    if (all_int) return DRAKEN_FLOAT64;  // INT64 and FLOAT64 mixed: as Arrow export does
    return 0;
}

inline uint8_t* build_validity(const Store& st, uint64_t offset, uint64_t n, uint32_t j, DkBuffers& bufs) {
    bool any_null = false;
    for (uint64_t i = 0; i < n && !any_null; ++i) {
        any_null = tag_type(Store::tag_at(st.record(offset + i), j)) == DRAKEN_NULL;
    }
    if (!any_null) return nullptr;
    auto* bits = static_cast<uint8_t*>(dk_calloc(static_cast<size_t>((n + 7) / 8)));
    bufs.validity = bits;
    for (uint64_t i = 0; i < n; ++i) {
        if (tag_type(Store::tag_at(st.record(offset + i), j)) != DRAKEN_NULL) bits[i >> 3] |= uint8_t(1u << (i & 7));
    }
    return bits;
}

template <class T, class F>
inline void gather_fixed(const Store& st, uint64_t offset, uint64_t n, uint32_t j, DkBuffers& bufs, F&& read) {
    T* out = static_cast<T*>(dk_calloc(static_cast<size_t>(n) * sizeof(T)));
    bufs.data = out;
    for (uint64_t i = 0; i < n; ++i) {
        const uint8_t* rec = st.record(offset + i);
        const uint16_t tag = Store::tag_at(rec, j);
        if (tag_type(tag) == DRAKEN_NULL) continue;
        out[i] = read(tag, st.slot_at(rec, j));
    }
}

// String block: [DrakenStringArena | DrakenStringSlot × n], plus a separate arena of
// only this field's long payloads (the store's arena holds every field's, and nested
// blocks, so copying it whole would multiply memory by the number of string fields).
inline void gather_strings(const Store& st, uint64_t offset, uint64_t n, uint32_t j, DrakenType type,
                           DkBuffers& bufs) {
    uint64_t long_bytes = 0;
    for (uint64_t i = 0; i < n; ++i) {
        const uint8_t* rec = st.record(offset + i);
        if (tag_type(Store::tag_at(rec, j)) == DRAKEN_NULL) continue;
        const auto* s = reinterpret_cast<const DrakenStringSlot*>(st.slot_at(rec, j));
        if (!str_is_inline(s)) long_bytes += str_length(s);
    }
    if (long_bytes > 0xFFFFFFFFull) throw nb::value_error("orso: a string column over 4 GB cannot become one draken vector");
    constexpr size_t kAlign = alignof(DrakenStringSlot);
    const size_t struct_end = (sizeof(DrakenStringArena) + kAlign - 1u) & ~(kAlign - 1u);
    const size_t total = struct_end + static_cast<size_t>(n ? n : 1) * sizeof(DrakenStringSlot);
    auto* block = static_cast<uint8_t*>(dk_calloc(total));
    bufs.data = block;
    uint8_t* arena = nullptr;
    if (long_bytes) {
        arena = static_cast<uint8_t*>(dk_malloc(static_cast<size_t>(long_bytes)));
        bufs.arena = arena;
    }
    auto* sa = reinterpret_cast<DrakenStringArena*>(block);
    auto* slots = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint32_t used = 0;
    const uint8_t* src_arena = st.arena.data();
    for (uint64_t i = 0; i < n; ++i) {
        const uint8_t* rec = st.record(offset + i);
        if (tag_type(Store::tag_at(rec, j)) == DRAKEN_NULL) {
            str_init_null(&slots[i]);
            continue;
        }
        const auto* s = reinterpret_cast<const DrakenStringSlot*>(st.slot_at(rec, j));
        if (str_is_inline(s)) {
            slots[i] = *s;
            continue;
        }
        const uint32_t len = str_length(s);
        std::memcpy(arena + used, src_arena + s->ext.arena_offset, len);
        str_clone_with_offset(&slots[i], s, used);
        used += len;
    }
    sa->slots = slots;
    sa->arena = arena;
    sa->length = static_cast<size_t>(n);
    sa->arena_used = used;
    sa->arena_cap = used;
    sa->null_bitmap = nullptr;  // validity travels on the vector
    sa->owns_buffers = 0;
    sa->payloads_elided = 0;
    sa->type = type;
}

// Field j of rows [offset, offset + n) as a draken Vector (new reference), or the reason
// it is not one.
inline std::pair<ExportKind, nb::object> export_draken_column(const Store& st, uint64_t offset, uint64_t n, uint32_t j,
                                                               const DrakenSpec& spec) {
    require_bound();
    if (n > 0xFFFFFFFFull) throw nb::value_error("orso: a draken vector holds at most 2^32 - 1 rows");
    int dec_scale = 0;
    const int t = resolve_export_type(st, offset, n, j, spec, dec_scale);
    if (t == 0) return {ExportKind::Fallback, nb::none()};
    if (t == DRAKEN_NULL) {
        if (spec.target == 0) return {ExportKind::AllNull, nb::none()};
    }
    // a typed column that is entirely null is still that type, every row invalid
    const DrakenType type = static_cast<DrakenType>(t == DRAKEN_NULL ? spec.target : t);
    DkBuffers bufs;
    const bool all_null = t == DRAKEN_NULL;
    uint8_t* validity = nullptr;
    if (all_null) {
        validity = static_cast<uint8_t*>(dk_calloc(static_cast<size_t>((n + 7) / 8)));
        bufs.validity = validity;
    } else {
        validity = build_validity(st, offset, n, j, bufs);
    }
    int dec_precision = 38;
    switch (type) {
        case DRAKEN_BOOL: {
            auto* bits = static_cast<uint8_t*>(dk_calloc(static_cast<size_t>((n + 7) / 8)));
            bufs.data = bits;
            for (uint64_t i = 0; i < n && !all_null; ++i) {
                const uint8_t* rec = st.record(offset + i);
                if (tag_type(Store::tag_at(rec, j)) == DRAKEN_NULL) continue;
                if (st.slot_at(rec, j)[0]) bits[i >> 3] |= uint8_t(1u << (i & 7));
            }
            break;
        }
        case DRAKEN_INT64:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME64:
            gather_fixed<int64_t>(st, offset, n, j, bufs, [](uint16_t, const uint8_t* s) { return load_i64(s); });
            break;
        case DRAKEN_FLOAT64:
            gather_fixed<double>(st, offset, n, j, bufs, [](uint16_t tag, const uint8_t* s) {
                if (tag_type(tag) == DRAKEN_INT64) return static_cast<double>(load_i64(s));
                double d;
                std::memcpy(&d, s, sizeof d);
                return d;
            });
            break;
        case DRAKEN_DATE32:
            gather_fixed<int32_t>(st, offset, n, j, bufs, [](uint16_t, const uint8_t* s) {
                int32_t d;
                std::memcpy(&d, s, sizeof d);
                return d;
            });
            break;
        case DRAKEN_INTERVAL:
            gather_fixed<DrakenIntervalSlot>(st, offset, n, j, bufs, [](uint16_t, const uint8_t* s) {
                DrakenIntervalSlot iv;
                std::memcpy(&iv, s, sizeof iv);
                return iv;
            });
            break;
        case DRAKEN_DECIMAL128: {
            if (all_null) dec_scale = spec.scale >= 0 ? spec.scale : 0;
            if (spec.precision > 0) dec_precision = spec.precision;
            if (dec_scale > dec_precision) return {ExportKind::Fallback, nb::none()};
            const __int128 limit = pow10_i128(dec_precision);
            bool fits = true;
            const int target_scale = dec_scale;
            gather_fixed<__int128>(st, offset, n, j, bufs, [&](uint16_t tag, const uint8_t* s) {
                const int vs = tag_param(tag) & DEC_SCALE_MASK;
                __int128 v = load_i128(s) * pow10_i128(target_scale - vs);
                if (v >= limit || v <= -limit) fits = false;
                return v;
            });
            if (!fits) return {ExportKind::Fallback, nb::none()};  // bufs frees what was built
            break;
        }
        case DRAKEN_VARCHAR:
        case DRAKEN_VARBINARY:
            gather_strings(st, offset, n, j, type, bufs);
            break;
        default:
            return {ExportKind::Fallback, nb::none()};
    }

    PyObject* v;
    const uint32_t len = static_cast<uint32_t>(n);
    if (type == DRAKEN_TIMESTAMP64) {
        v = g_draken.own_raw_logical(bufs.data, bufs.validity, len, type, DK_LOGICAL_TIMESTAMP, DK_UNIT_US, 0, 0, 0, 0);
    } else if (type == DRAKEN_TIME64) {
        v = g_draken.own_raw_logical(bufs.data, bufs.validity, len, type, DK_LOGICAL_TIME, DK_UNIT_US, 0, 0, 0, 0);
    } else if (type == DRAKEN_DECIMAL128) {
        v = g_draken.own_raw_logical(bufs.data, bufs.validity, len, type, DK_LOGICAL_DECIMAL, 0, 0,
                                     static_cast<uint8_t>(dec_precision), static_cast<uint8_t>(dec_scale), 0);
    } else {
        v = g_draken.own_raw_with_arena(bufs.data, bufs.arena, bufs.validity, len, type);
    }
    // Ownership has passed to draken either way: on failure its constructor may already
    // have freed the buffers, so freeing them here could free them twice. A failed
    // hand-off leaks them instead, which only happens on allocation failure inside draken.
    bufs.release();
    if (v == nullptr) throw nb::python_error();
    return {ExportKind::Vector, nb::steal(v)};
}

// ----- import ------------------------------------------------------------------------

// The per-field description Python passes for a draken Vector: its handle and the
// logical-type facts draken keeps on the owner rather than in the DrakenVector.
struct DrakenSource {
    nb::object handle;
    int logical_kind = 0;
    int unit = 0;
    int scale = 0;
};

inline int64_t floor_div_i64(int64_t a, int64_t b) noexcept {
    int64_t q = a / b;
    if ((a % b != 0) && ((a < 0) != (b < 0))) --q;
    return q;
}

inline bool bit_at(const uint8_t* bits, uint64_t i) noexcept { return (bits[i >> 3] >> (i & 7)) & 1u; }

inline void put_tag(uint8_t* rec, uint32_t j, uint16_t tag) noexcept { std::memcpy(rec + 2 * j, &tag, sizeof tag); }

// Field j of records [base, base + n) from Python values (a draken to_pylist()).
inline void scatter_values(Store& st, uint64_t base, uint64_t n, uint32_t j, nb::handle values) {
    PyObject* seq = PySequence_Fast(values.ptr(), "orso: to_pylist() did not return a sequence");
    if (seq == nullptr) raise_python();
    if (static_cast<uint64_t>(PySequence_Fast_GET_SIZE(seq)) != n) {
        Py_DECREF(seq);
        throw nb::value_error("orso: a draken vector's values do not match its length");
    }
    PyObject** items = PySequence_Fast_ITEMS(seq);
    for (uint64_t i = 0; i < n; ++i) {
        uint8_t slot[SLOT_BYTES];
        uint16_t tag = 0;
        if (encode_value(st, items[i], tag, slot) < 0) {
            Py_DECREF(seq);
            raise_python();
        }
        uint8_t* rec = st.record(base + i);  // records are pre-sized: encoding never moves them
        put_tag(rec, j, tag);
        std::memcpy(rec + st.slot_offset + SLOT_BYTES * j, slot, SLOT_BYTES);
    }
    Py_DECREF(seq);
}

// Field j natively; false when the vector holds a value orso keeps only as an object.
// Returning false part-way is safe: the caller rewrites every row of field j from
// draken's values, and the only types that write outside the record (strings, into the
// arena) never return false once they start.
inline bool scatter_native(Store& st, uint64_t base, uint64_t n, uint32_t j, const DrakenVector* v,
                           const DrakenSource& src) {
    const DrakenType type = v->type;
    if (src.logical_kind > DK_LOGICAL_DECIMAL) return false;  // VECTOR, IPV4: draken's values
    switch (type) {
        case DRAKEN_ARRAY:
        case DRAKEN_UUID:
        case DRAKEN_VECTOR_FP16:
            return false;
        default:
            break;
    }
    const uint32_t* sel = v->selection;
    const uint8_t* valid = v->validity;
    const DrakenStringArena* sa = nullptr;
    if (type == DRAKEN_VARCHAR || type == DRAKEN_NVARCHAR || type == DRAKEN_VARIANT || type == DRAKEN_VARBINARY) {
        sa = static_cast<const DrakenStringArena*>(v->data);
        if (n && sa == nullptr) return false;
        if (sa != nullptr && sa->payloads_elided) {
            throw nb::value_error("orso: this draken string vector was read without its payloads");
        }
    }
    for (uint64_t i = 0; i < n; ++i) {
        uint8_t* rec = st.record(base + i);
        uint8_t* slot = rec + st.slot_offset + SLOT_BYTES * j;
        std::memset(slot, 0, SLOT_BYTES);
        if (type == DRAKEN_NULL || (valid != nullptr && !bit_at(valid, i))) {
            put_tag(rec, j, make_tag(DRAKEN_NULL));
            continue;
        }
        const uint32_t k = sel[i];
        const void* d = v->data;
        auto put_i64 = [&](int64_t x, DrakenType as) {
            std::memcpy(slot, &x, sizeof x);
            put_tag(rec, j, make_tag(as));
        };
        // value in `unit` -> microseconds; false when it would overflow
        auto to_us = [&](int64_t x, int64_t& out) {
            switch (src.unit) {
                case DK_UNIT_S: return !__builtin_mul_overflow(x, US_PER_SECOND, &out);
                case DK_UNIT_MS: return !__builtin_mul_overflow(x, int64_t(1000), &out);
                case DK_UNIT_NS: out = floor_div_i64(x, 1000); return true;
                default: out = x; return true;
            }
        };
        switch (type) {
            case DRAKEN_INT8: put_i64(static_cast<const int8_t*>(d)[k], DRAKEN_INT64); break;
            case DRAKEN_INT16: put_i64(static_cast<const int16_t*>(d)[k], DRAKEN_INT64); break;
            case DRAKEN_INT32: put_i64(static_cast<const int32_t*>(d)[k], DRAKEN_INT64); break;
            case DRAKEN_INT64: put_i64(static_cast<const int64_t*>(d)[k], DRAKEN_INT64); break;
            case DRAKEN_UINT8: put_i64(static_cast<const uint8_t*>(d)[k], DRAKEN_INT64); break;
            case DRAKEN_UINT16: put_i64(static_cast<const uint16_t*>(d)[k], DRAKEN_INT64); break;
            case DRAKEN_UINT32: put_i64(static_cast<const uint32_t*>(d)[k], DRAKEN_INT64); break;
            case DRAKEN_UINT64: {
                const uint64_t x = static_cast<const uint64_t*>(d)[k];
                if (x > uint64_t(INT64_MAX)) return false;  // an int orso holds as an object
                put_i64(static_cast<int64_t>(x), DRAKEN_INT64);
                break;
            }
            case DRAKEN_FLOAT32:
            case DRAKEN_FLOAT64: {
                const double x = type == DRAKEN_FLOAT32 ? static_cast<double>(static_cast<const float*>(d)[k])
                                                        : static_cast<const double*>(d)[k];
                std::memcpy(slot, &x, sizeof x);
                put_tag(rec, j, make_tag(DRAKEN_FLOAT64));
                break;
            }
            case DRAKEN_BOOL:
                slot[0] = bit_at(static_cast<const uint8_t*>(d), k) ? 1 : 0;
                put_tag(rec, j, make_tag(DRAKEN_BOOL));
                break;
            case DRAKEN_DATE32: {
                const int32_t x = static_cast<const int32_t*>(d)[k];
                std::memcpy(slot, &x, sizeof x);
                put_tag(rec, j, make_tag(DRAKEN_DATE32));
                break;
            }
            case DRAKEN_TIMESTAMP64: {
                int64_t us;
                if (!to_us(static_cast<const int64_t*>(d)[k], us)) return false;
                put_i64(us, DRAKEN_TIMESTAMP64);
                break;
            }
            case DRAKEN_TIME32:
            case DRAKEN_TIME64: {
                const int64_t x = type == DRAKEN_TIME32 ? static_cast<int64_t>(static_cast<const int32_t*>(d)[k])
                                                        : static_cast<const int64_t*>(d)[k];
                int64_t us;
                if (!to_us(x, us)) return false;
                put_i64(us, DRAKEN_TIME64);
                break;
            }
            case DRAKEN_INTERVAL: {
                const DrakenIntervalSlot iv = static_cast<const DrakenIntervalSlot*>(d)[k];
                if (iv.months != 0) return false;  // no timedelta for months: draken's (months, us)
                std::memcpy(slot, &iv, sizeof iv);
                put_tag(rec, j, make_tag(DRAKEN_INTERVAL));
                break;
            }
            case DRAKEN_DECIMAL:
            case DRAKEN_DECIMAL128: {
                const __int128 x = type == DRAKEN_DECIMAL ? static_cast<__int128>(static_cast<const int64_t*>(d)[k])
                                                          : static_cast<const __int128*>(d)[k];
                std::memcpy(slot, &x, sizeof x);
                put_tag(rec, j, make_tag(DRAKEN_DECIMAL128, static_cast<uint8_t>(src.scale & DEC_SCALE_MASK)));
                break;
            }
            case DRAKEN_VARCHAR:
            case DRAKEN_NVARCHAR:
            case DRAKEN_VARIANT:
            case DRAKEN_VARBINARY: {
                const DrakenStringSlot* s = &sa->slots[k];
                const uint32_t len = str_length(s);
                const uint8_t* p = str_data(s, sa->arena);
                // put_string may grow the arena; the record does not move
                if (!put_string(st, reinterpret_cast<const char*>(p), len, slot)) {
                    throw nb::value_error("orso: the frame's string arena is full (4 GB)");
                }
                const bool text = type != DRAKEN_VARBINARY;
                put_tag(rec, j,
                        text ? make_tag(DRAKEN_VARCHAR, bytes_are_ascii(p, len) ? PARAM_ASCII : 0)
                             : make_tag(DRAKEN_VARBINARY));
                break;
            }
            default:
                return false;  // a type this build does not know: draken's own values
        }
    }
    return true;
}

// Append rows from draken sources (one per field, all `n` rows long). On failure the
// store is as it was.
inline void import_draken(Store& st, const std::vector<DrakenSource>& sources, uint64_t n) {
    require_bound();
    if (sources.size() != st.nfields) throw nb::value_error("orso: one draken vector per field is required");
    std::vector<const DrakenVector*> vecs(sources.size());
    for (size_t j = 0; j < sources.size(); ++j) {
        const DrakenVector* v = g_draken.unwrap(sources[j].handle.ptr());
        if (v == nullptr) raise_python();
        if (v->length != n) throw nb::value_error("orso: draken vectors of different lengths");
        vecs[j] = v;
    }
    const size_t records_before = st.records.size();
    const size_t arena_before = st.arena.size();
    const size_t objects_before = st.objects.size();
    const uint64_t base = st.rows;
    try {
        uint8_t* recs = st.records.extend(static_cast<size_t>(n * st.stride));
        std::memset(recs, 0, static_cast<size_t>(n * st.stride));
        for (uint32_t j = 0; j < st.nfields; ++j) {
            if (!scatter_native(st, base, n, j, vecs[j], sources[j])) {
                nb::object values = sources[j].handle.attr("to_pylist")();
                scatter_values(st, base, n, j, values);
            }
        }
    } catch (...) {
        for (size_t k = objects_before; k < st.objects.size(); ++k) Py_DECREF(st.objects[k]);
        st.objects.resize(objects_before);
        st.records.resize_uninit(records_before);
        st.arena.resize_uninit(arena_before);
        throw;
    }
    st.rows += n;
}

}  // namespace orso
