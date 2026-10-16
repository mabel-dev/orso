// orso/native/native.cpp — the one nanobind translation unit for `orso._native`.
//
// Phase 2 (DESIGN.md §10): the C base of Row (row.h), the msgpack size estimate
// (width.h), and the record store with its validation plan (store.h). The phase 1
// probes (`parse_timestamp_us`, `format_double`, `append_buffer_probe`) stay until the
// phase 4 parsers and phase 5 renderers replace them.
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
#include "row.h"
#include "store.h"
#include "arrow_export.h"
#include "arrow_import.h"
#include "decimal.h"
#include "schema_slots.h"
#include "serialize.h"
#include "render.h"
#include "width.h"
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
    m.doc() = "orso native layer: Row base type, record store, size estimates";

    // nanobind's exit-time leak report fires for any Frame still referenced by a module
    // global when the interpreter shuts down, which is every script that holds a
    // DataFrame at exit. Real leaks are caught by tests/test_native_rows.py instead.
    nb::set_leak_warnings(false);

    PyDateTime_IMPORT;
    if (PyDateTimeAPI == nullptr) throw nb::python_error();
    {
        nb::object decimal_type = nb::module_::import_("decimal").attr("Decimal");
        orso::g_decimal_type = decimal_type.release().ptr();  // kept for the process lifetime
    }
    PyTypeObject* row_base = orso::make_row_base();
    if (row_base == nullptr) throw nb::python_error();
    m.attr("RowBase") = nb::borrow(reinterpret_cast<PyObject*>(row_base));

    PyObject* tracked = PyType_FromSpec(&orso::TrackedSlot_spec);
    if (tracked == nullptr) throw nb::python_error();
    m.attr("TrackedSlot") = nb::steal(tracked);

    // Raw C functions: these sit on hot paths (every find_column, every timestamp parse)
    // where nanobind's argument dispatch would be most of the cost.
    static PyMethodDef fast_methods[] = {
        {"schema_epoch", [](PyObject*, PyObject*) -> PyObject* { return PyLong_FromUnsignedLongLong(orso::g_schema_epoch); },
         METH_NOARGS, "The schema epoch: bumped by every write that can invalidate a schema cache."},
        {"bump_schema_epoch", [](PyObject*, PyObject*) -> PyObject* { ++orso::g_schema_epoch; Py_RETURN_NONE; },
         METH_NOARGS, "Invalidate every schema cache (after an in-place change orso cannot see)."},
        {"parse_iso_text", [](PyObject*, PyObject* v) -> PyObject* { return orso::parse_iso_text(v); },
         METH_O, "ISO-8601 str/bytes to a naive UTC datetime, or None when the strict parser refuses it."},
        {"row_packed", [](PyObject*, PyObject* row) -> PyObject* { return orso::row_packed(row); },
         METH_O, "msgpack payload of a row, byte-identical to v1's Row._packed()."},
        {"row_as_bytes", [](PyObject*, PyObject* row) -> PyObject* { return orso::row_as_bytes(row); },
         METH_O, "v1's record bytes for a row: header, length, timestamp, payload."},
        {"row_from_bytes", [](PyObject*, PyObject* data) -> PyObject* { return orso::row_from_bytes(data); },
         METH_O, "The values tuple of a v1 record."},
        {"row_as_json", [](PyObject*, PyObject* row) -> PyObject* { return orso::row_as_json(row); },
         METH_O, "Row.as_json bytes, or None when Python's json should do the row."},
        {"trunc_printable", reinterpret_cast<PyCFunction>(reinterpret_cast<void (*)(void)>(
             +[](PyObject*, PyObject* const* args, Py_ssize_t nargs) -> PyObject* {
                 if (nargs < 1 || nargs > 3 || !PyUnicode_Check(args[0])) {
                     PyErr_SetString(PyExc_TypeError, "trunc_printable(str, width=None, full_line=True)");
                     return nullptr;
                 }
                 PyObject* width = nargs > 1 ? args[1] : Py_None;
                 const int full_line = nargs > 2 ? PyObject_IsTrue(args[2]) : 1;
                 if (full_line < 0) return nullptr;
                 return orso::trunc_printable(args[0], width, full_line);
             })),
         METH_FASTCALL, "display.py's trunc_printable, natively."},
        {nullptr, nullptr, 0, nullptr},
    };
    if (PyModule_AddFunctions(m.ptr(), fast_methods) < 0) throw nb::python_error();

    m.def("set_value_sizer", [](nb::handle fn) {
        Py_XINCREF(fn.ptr());
        Py_XSETREF(orso::g_value_sizer, fn.ptr());
    }, nb::arg("fn"), "Install the Python fallback that sizes one value the way v1 did.");

    m.def("set_serializers", [](nb::handle packer, nb::handle unpacker, nb::handle data_error, nb::handle fromtimestamp) {
        for (auto [slot, h] : {std::pair{&orso::g_value_packer, packer}, std::pair{&orso::g_payload_unpacker, unpacker},
                              std::pair{&orso::g_data_error, data_error}, std::pair{&orso::g_fromtimestamp, fromtimestamp}}) {
            Py_INCREF(h.ptr());
            Py_XSETREF(*slot, h.ptr());
        }
    }, nb::arg("packer"), nb::arg("unpacker"), nb::arg("data_error"), nb::arg("fromtimestamp"),
       "Install the Python fallbacks the native record and JSON writers use.");

    m.def("rows_from_dicts", [](nb::handle row_type, nb::handle keys, nb::handle first, nb::handle rest) {
        if (!PyType_Check(row_type.ptr()) || !PyTuple_Check(keys.ptr())) throw nb::type_error("rows_from_dicts(type, tuple, dict, iterator)");
        PyObject* out = orso::rows_from_dicts(reinterpret_cast<PyTypeObject*>(row_type.ptr()), keys.ptr(), first.ptr(), rest.ptr());
        if (out == nullptr) throw nb::python_error();
        return nb::steal(out);
    }, nb::arg("row_type"), nb::arg("keys"), nb::arg("first").none(), nb::arg("rest"),
       "Build the Row list for DataFrame(dictionaries) in one pass.");

    m.def("rows_nbytes", [](nb::handle rows, Py_ssize_t start, Py_ssize_t stop) {
        if (!PyList_Check(rows.ptr())) throw nb::type_error("rows_nbytes: rows must be a list");
        const Py_ssize_t n = PyList_GET_SIZE(rows.ptr());
        if (stop < 0 || stop > n) stop = n;
        if (start < 0) start = 0;
        int64_t total = 0;
        for (Py_ssize_t i = start; i < stop; ++i) {
            const int64_t w = orso::py_row_width(PyList_GET_ITEM(rows.ptr(), i));
            if (w < 0) throw nb::python_error();
            total += w;
        }
        return total;
    }, nb::arg("rows"), nb::arg("start") = 0, nb::arg("stop") = -1,
       "Sum of Row.nbytes() over rows[start:stop], for any tuples.");

    m.def("value_nbytes", [](nb::handle v) {
        const int64_t w = orso::py_value_width(v.ptr());
        if (w < 0) throw nb::python_error();
        return w;
    }, nb::arg("value").none(), "msgpack size of one value, as v1's serialiser would write it.");

    nb::class_<orso::Plan>(m, "Plan")
        .def(nb::init<nb::handle, nb::handle, nb::handle, nb::handle>(), nb::arg("row_type"),
             nb::arg("columns").none(), nb::arg("validate").none(), nb::arg("dumps").none())
        .def("row", &orso::Plan::build_row, nb::arg("entry").none(), "A validated Row for `entry`, v1 append() semantics.")
        .def("check", [](const orso::Plan& p, nb::handle entry) {
            // True when `entry` passes v1's fast checks (exact keys, nullability,
            // isinstance); False means "ask RelationSchema.validate's slow path".
            orso::ValueBuffer buf(p.nfields);
            const int r = p.gather_typed_fast(entry.ptr(), buf);
            if (r < 0) throw nb::python_error();
            return r == 1;
        }, nb::arg("entry").none())
        .def_prop_ro("typed", [](const orso::Plan& p) { return p.typed; })
        .def_prop_ro("nfields", [](const orso::Plan& p) { return p.nfields; });

    nb::class_<orso::Frame>(m, "Frame")
        .def(nb::init<nb::handle>(), nb::arg("plan"))
        .def("append", &orso::Frame::append, nb::arg("entry").none())
        .def("__len__", &orso::Frame::length)
        .def("row", &orso::Frame::row, nb::arg("i"))
        .def("rows", &orso::Frame::rows, nb::arg("start") = 0, nb::arg("stop") = -1)
        .def("view", &orso::Frame::view, nb::arg("start"), nb::arg("count"))
        .def("nbytes", &orso::Frame::nbytes)
        .def("stats", &orso::Frame::stats)
        .def("cursor", [](const orso::Frame& f) { return orso::FrameCursor(f); },
             "A DB-API cursor over the rows this frame holds now.")
        .def("extend", &orso::Frame::extend, nb::arg("rows"), nb::arg("limit") = -1,
             "Append Python rows positionally, unvalidated; each must have nfields values.")
        .def("column_values", &orso::Frame::column_values, nb::arg("j"), nb::arg("limit") = -1,
             "Field j of the first `limit` rows as a list of Python values.")
        .def("import_arrow", [](orso::Frame& f, nb::handle source, int64_t limit) {
            // A Table imports batch by batch; a RecordBatch directly. Returns rows imported.
            if (f.is_view) f.detach();
            int64_t imported = 0;
            auto one = [&](PyObject* batch) {
                const int64_t remaining = limit < 0 ? -1 : limit - imported;
                if (limit >= 0 && remaining <= 0) return;
                const int64_t n = orso::import_batch(*f.store, batch, remaining);
                if (n < 0) throw nb::python_error();
                imported += n;
            };
            if (PyObject_HasAttrString(source.ptr(), "to_batches")) {
                nb::object batches = source.attr("to_batches")();
                for (nb::handle b : batches) one(b.ptr());
            } else {
                one(source.ptr());
            }
            return imported;
        }, nb::arg("source"), nb::arg("limit") = -1,
           "Import a pyarrow Table or RecordBatch through the C Data Interface.")
        .def("export_columns", [](const orso::Frame& f, nb::handle specs, int64_t limit) {
            uint64_t n = f.length();
            if (limit >= 0 && static_cast<uint64_t>(limit) < n) n = static_cast<uint64_t>(limit);
            nb::list out;
            uint32_t j = 0;
            for (nb::handle spec : specs) {
                if (j >= f.store->nfields) throw nb::value_error("more export specs than fields");
                out.append(orso::export_column(*f.store, f.offset, n, j, orso::parse_spec(spec)));
                ++j;
            }
            if (j != f.store->nfields) throw nb::value_error("one export spec per field is required");
            return out;
        }, nb::arg("specs"), nb::arg("limit") = -1,
           "Per field: (schema, array) PyCapsules for pyarrow, or None where pyarrow must convert.");

    nb::class_<orso::FrameCursor>(m, "FrameCursor")
        .def("__iter__", [](nb::handle self) { return nb::borrow(self); })
        .def("__next__", &orso::FrameCursor::next)
        .def("__length_hint__", &orso::FrameCursor::remaining)
        .def("take", &orso::FrameCursor::take, nb::arg("n"), "The next `n` rows as a list.")
        .def("rest", &orso::FrameCursor::rest, "Every remaining row as a list.")
        .def_ro("position", &orso::FrameCursor::pos, "Rows fetched so far.");

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
