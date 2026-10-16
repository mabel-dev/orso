#pragma once
// orso/native/render.h — the per-character work of display() (DESIGN.md §4.11, phase 5).
//
// Profiling display() put ~70% of its time in one inner function, trunc_printable: a
// character-by-character walk of every cell and line, sizing each character. This is
// that function, character for character, so the table display() draws is unchanged;
// the layout, colours and type formatting stay in display.py.
//
// Display width follows v1: ASCII is 1; anything else is 2 when
// unicodedata.east_asian_width() says "F" or "W", else 1. The answer comes from the
// running Python's unicodedata (asked once per distinct character, then cached), so the
// width table always matches the Unicode version v1 would have used.

#include <Python.h>

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace orso {

inline PyObject* g_east_asian_width = nullptr;  // unicodedata.east_asian_width
inline std::unordered_map<Py_UCS4, uint8_t> g_char_width;

// 1 or 2, or 0 with an exception set
inline int char_width(Py_UCS4 c) {
    if (c < 128) return 1;
    auto it = g_char_width.find(c);
    if (it != g_char_width.end()) return it->second;
    if (g_east_asian_width == nullptr) {
        PyObject* mod = PyImport_ImportModule("unicodedata");
        if (mod == nullptr) return 0;
        g_east_asian_width = PyObject_GetAttrString(mod, "east_asian_width");
        Py_DECREF(mod);
        if (g_east_asian_width == nullptr) return 0;
    }
    PyObject* ch = PyUnicode_FromOrdinal(static_cast<int>(c));
    if (ch == nullptr) return 0;
    PyObject* kind = PyObject_CallOneArg(g_east_asian_width, ch);
    Py_DECREF(ch);
    if (kind == nullptr) return 0;
    const int w = (PyUnicode_CompareWithASCIIString(kind, "F") == 0 || PyUnicode_CompareWithASCIIString(kind, "W") == 0) ? 2 : 1;
    Py_DECREF(kind);
    if (g_char_width.size() < 65536) g_char_width.emplace(c, static_cast<uint8_t>(w));
    return w;
}

// display.py's trunc_printable(value, width=None, full_line=True), exactly.
inline PyObject* trunc_printable(PyObject* value, PyObject* width_obj, int full_line) {
    const bool has_width = width_obj != Py_None;
    Py_ssize_t width = 0;
    if (has_width) {
        width = PyLong_AsSsize_t(width_obj);
        if (width == -1 && PyErr_Occurred()) return nullptr;
    }
    const int kind = PyUnicode_KIND(value);
    const void* data = PyUnicode_DATA(value);
    const Py_ssize_t n = PyUnicode_GET_LENGTH(value);

    static const Py_UCS4 OFF[] = {1, 'O', 'F', 'F', 'm'};
    std::vector<Py_UCS4> out;
    out.reserve(static_cast<size_t>(n) + 16);
    auto emit_off = [&]() { out.insert(out.end(), OFF, OFF + 5); };

    bool simple = true;
    for (Py_ssize_t i = 0; i < n; ++i) {
        const Py_UCS4 c = PyUnicode_READ(kind, data, i);
        if (c == '\n' || c == '\r' || c == 033 || c == 1) {
            simple = false;
            break;
        }
    }
    if (simple) {
        // the simple path measures in characters, not display width, as v1 did
        // value[:width] when longer: a negative width drops characters from the end
        Py_ssize_t keep = n;
        if (has_width && n > width) keep = width >= 0 ? width : (n + width > 0 ? n + width : 0);
        for (Py_ssize_t i = 0; i < keep; ++i) out.push_back(PyUnicode_READ(kind, data, i));
        if (has_width && full_line && keep < width) out.insert(out.end(), static_cast<size_t>(width - keep), ' ');
        emit_off();
        return PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND, out.data(), static_cast<Py_ssize_t>(out.size()));
    }

    static const Py_UCS4 CRLF[] = {1, 'C', 'R', 'L', 'F', 'm', 0x21B5, 1, 'V', 'A', 'R', 'C', 'H', 'A', 'R', 'm'};
    Py_ssize_t offset = 0;
    bool ignoring = false;
    for (Py_ssize_t i = 0; i < n; ++i) {
        const Py_UCS4 c = PyUnicode_READ(kind, data, i);
        if (c == '\n') {
            out.insert(out.end(), CRLF, CRLF + 16);
            offset += 1;
            continue;
        }
        if (c == '\r') continue;
        out.push_back(c);
        if (c == 033 || c == 1) ignoring = true;
        if (!ignoring) {
            const int w = char_width(c);
            if (w == 0) return nullptr;
            offset += w;
        }
        if (ignoring && c == 'm') ignoring = false;
        if (has_width && !ignoring && offset >= width) {
            emit_off();
            return PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND, out.data(), static_cast<Py_ssize_t>(out.size()));
        }
    }
    emit_off();
    if (full_line && has_width && width > offset) out.insert(out.end(), static_cast<size_t>(width - offset), ' ');
    return PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND, out.data(), static_cast<Py_ssize_t>(out.size()));
}

}  // namespace orso
