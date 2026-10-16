#pragma once
// orso/native/record.h — the row record layout (DESIGN.md §4.1).
//
// A frame with `n` fields stores every row as one fixed-stride record:
//
//     record = [ tags: n × uint16, padded to 16 bytes ][ slots: n × 16 bytes ]
//     stride = round_up(2n, 16) + 16n
//
// tag low byte  = DrakenType of the value (DRAKEN_NULL for a null)
// tag high byte = per-value parameter (decimal scale; struct marker on DRAKEN_ARRAY)
//
// Every slot is 16 bytes so that a field's slots across rows are a strided view of
// exactly what a draken column stores: int64/double in the first 8 bytes, a German
// string slot (core/string_slot.h) verbatim, a DrakenIntervalSlot verbatim, a 16-byte
// __int128 unscaled decimal. Row i is at `records + i * stride`; there is no offsets
// index.
//
// Pure C++, no Python.

#include <cstddef>
#include <cstdint>

#include "core/buffers.h"
#include "core/interval_slot.h"
#include "core/string_slot.h"

namespace orso {

constexpr uint32_t SLOT_BYTES = 16;
constexpr uint32_t TAG_BYTES = 2;
constexpr uint32_t RECORD_ALIGN = 16;

// Bytes from the start of a record to its first slot: the tag array, padded so slots
// stay 16-byte aligned when the record buffer is.
constexpr uint32_t record_slot_offset(uint32_t nfields) noexcept {
    const uint32_t tags = nfields * TAG_BYTES;
    return (tags + RECORD_ALIGN - 1) / RECORD_ALIGN * RECORD_ALIGN;
}

constexpr uint32_t record_stride(uint32_t nfields) noexcept {
    return record_slot_offset(nfields) + nfields * SLOT_BYTES;
}

// A tag is the DrakenType in the low byte and a per-value parameter in the high byte.
constexpr uint16_t make_tag(DrakenType type, uint8_t param = 0) noexcept {
    return static_cast<uint16_t>((static_cast<uint16_t>(param) << 8) | static_cast<uint8_t>(type));
}
constexpr DrakenType tag_type(uint16_t tag) noexcept { return static_cast<DrakenType>(tag & 0xFFu); }
constexpr uint8_t tag_param(uint16_t tag) noexcept { return static_cast<uint8_t>(tag >> 8); }

// The slot formats this layout borrows from draken must stay 16 bytes, or a strided
// gather of a field is no longer a draken column.
static_assert(sizeof(DrakenIntervalSlot) == SLOT_BYTES, "interval slot must be 16 bytes");
static_assert(sizeof(__int128) == SLOT_BYTES, "decimal128 slot must be 16 bytes");
static_assert(sizeof(void*) <= 8, "object slots hold a PyObject* in their first 8 bytes");
static_assert(record_stride(6) == 112, "six fields: 16 bytes of tags + 96 bytes of slots");
static_assert(record_stride(8) == 144, "eight fields: 16 bytes of tags + 128 bytes of slots");
static_assert(record_stride(9) == 176, "nine fields: 32 bytes of tags + 144 bytes of slots");

}  // namespace orso
