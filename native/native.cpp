// orso/native/native.cpp — the one nanobind translation unit for `orso._native`.
//
// Phase 1 (DESIGN.md §10): the module exists, the vendored draken headers compile under
// orso's flags, and the record layout arithmetic (record.h) is exposed so the tests can
// pin it. Everything below `abi_report` is a probe that proves a header links; the real
// store, encoders and parsers arrive in phases 2 to 5 and replace them.
//
// Binding rules (draken docs/design/03_binding.md): this file owns C++ objects, marshals
// at the Python edge, calls C++ free functions. No row loops, no dispatch, no Python
// objects mid-pipeline.

#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>

#include "record.h"
#include "core/alloc.h"
#include "core/append_buffer.h"
#include "core/buffers.h"
#include "core/decimal_text.h"
#include "core/interval_slot.h"
#include "core/iso_datetime.h"
#include "core/string_slot.h"
#include "interop/arrow_c_data_interface.h"
#include "interop/value_format.hpp"
#include "logical_type.h"
#include "ryu.h"

#ifndef ORSO_DRAKEN_SOURCE
#define ORSO_DRAKEN_SOURCE "unknown"
#endif

namespace nb = nanobind;

namespace {

// ISO-8601 timestamp text -> microseconds since the Unix epoch, naive UTC. An offset
// suffix is applied, not dropped (DESIGN.md §8 fix 11). Returns nullopt on malformed
// input; the caller decides what that means.
std::optional<int64_t> parse_timestamp_us(const std::string& text) {
    int year, month, day, hour, minute, second, usec, offset_seconds;
    const bool ok = draken::iso_datetime::parse_iso_timestamp(
        reinterpret_cast<const uint8_t*>(text.data()), static_cast<uint32_t>(text.size()),
        &year, &month, &day, &hour, &minute, &second, &usec, &offset_seconds);
    if (!ok) return std::nullopt;
    const int64_t micros = draken::iso_datetime::civil_to_micros(year, month, day, hour, minute, second, usec);
    return micros - static_cast<int64_t>(offset_seconds) * 1'000'000;
}

// Shortest round-trip rendering of a double (ryu), the renderer display() will use.
std::string format_double(double value) {
    char buf[32];
    const int n = d2s_buffered_n(value, buf);
    return std::string(buf, static_cast<size_t>(n));
}

// Exercises AppendBuffer over the draken allocator: the growth path and the size
// accounting the store will lean on. Returns the bytes the buffer ended up holding.
int64_t append_buffer_probe(int64_t count) {
    draken::AppendBuffer<int64_t> buf;
    for (int64_t i = 0; i < count; ++i) buf.push_back(i);
    int64_t total = 0;
    for (int64_t i = 0; i < count; ++i) total += buf[static_cast<size_t>(i)];
    return total;
}

}  // namespace

NB_MODULE(_native, m) {
    m.doc() = "orso native layer (phase 1 skeleton)";

    m.def("draken_source", []() { return std::string(ORSO_DRAKEN_SOURCE); },
          "Commit of opteryx-core the vendored draken headers were copied from.");

    m.def("abi_report", []() {
        nb::dict report;
        report["slot_bytes"] = static_cast<int>(orso::SLOT_BYTES);
        report["tag_bytes"] = static_cast<int>(orso::TAG_BYTES);
        report["string_slot_bytes"] = static_cast<int>(sizeof(DrakenStringSlot));
        report["interval_slot_bytes"] = static_cast<int>(sizeof(DrakenIntervalSlot));
        report["decimal128_bytes"] = static_cast<int>(sizeof(__int128));
        report["draken_vector_bytes"] = static_cast<int>(sizeof(DrakenVector));
        report["arrow_array_bytes"] = static_cast<int>(sizeof(struct ArrowArray));
        return report;
    }, "Sizes of the slot formats and the draken ABI this build was compiled against.");

    m.def("record_stride", [](uint32_t nfields) { return orso::record_stride(nfields); },
          "Bytes per row for a frame with `nfields` fields (DESIGN.md §4.1).");
    m.def("record_slot_offset", [](uint32_t nfields) { return orso::record_slot_offset(nfields); },
          "Bytes from the start of a record to its first slot.");

    m.def("parse_timestamp_us", &parse_timestamp_us, nb::arg("text"),
          "ISO-8601 text to microseconds since the epoch (naive UTC), or None.");
    m.def("format_double", &format_double, nb::arg("value"),
          "Shortest round-trip text for a double.");
    m.def("append_buffer_probe", &append_buffer_probe, nb::arg("count"));
}
