#pragma once
// orso/native/decimal.h — exact decimal.Decimal <-> DRAKEN_DECIMAL128 slots.
//
// A slot holds the unscaled coefficient as a 16-byte __int128 (draken's DECIMAL128
// physical form) and the tag's parameter byte holds the scale (0..38) plus a negative-
// zero flag. That is enough to give back a Decimal with the same sign, digits and
// exponent as the one stored, so Decimal('1.50') stays Decimal('1.50') and
// Decimal('-0.00') stays Decimal('-0.00'). Values a slot cannot hold exactly (NaN,
// infinities, positive exponents like Decimal('1E+3'), more than 38 digits, a scale
// beyond 38) are kept as objects instead.
//
// Encoding reads str(Decimal), which the decimal spec makes exact; decoding builds the
// plain-notation text and calls Decimal(text), which is exact too.

#include <Python.h>

#include <cstdint>
#include <cstring>

namespace orso {

inline PyObject* g_decimal_type = nullptr;  // decimal.Decimal, set at module init

constexpr uint8_t DEC_SCALE_MASK = 0x3F;
constexpr uint8_t DEC_NEG_ZERO = 0x80;
constexpr int DEC_MAX_DIGITS = 38;

inline __int128 dec_pow10(int k) noexcept {
    __int128 r = 1;
    while (k-- > 0) r *= 10;
    return r;
}

// Decimal digits of |v| into buf, most significant first; "0" for zero. Returns count.
inline int dec_digits(__int128 v, char* buf) noexcept {
    unsigned __int128 u = v < 0 ? static_cast<unsigned __int128>(-(v + 1)) + 1u : static_cast<unsigned __int128>(v);
    char tmp[48];
    int n = 0;
    do {
        tmp[n++] = static_cast<char>('0' + static_cast<int>(u % 10u));
        u /= 10u;
    } while (u != 0u);
    for (int i = 0; i < n; ++i) buf[i] = tmp[n - 1 - i];
    return n;
}

// Parse the text of str(Decimal). False when the value has no exact slot form.
inline bool dec_from_text(const char* s, Py_ssize_t len, __int128& out, uint8_t& scale, bool& negzero) noexcept {
    Py_ssize_t i = 0;
    bool neg = false;
    if (i < len && (s[i] == '-' || s[i] == '+')) {
        neg = s[i] == '-';
        ++i;
    }
    if (i >= len || s[i] < '0' || s[i] > '9') return false;  // NaN, sNaN, Infinity
    char digits[128];
    int ndigits = 0;
    int frac = 0;
    bool seen_point = false;
    bool leading = true;
    for (; i < len; ++i) {
        const char c = s[i];
        if (c >= '0' && c <= '9') {
            if (seen_point) ++frac;
            if (leading && c == '0') continue;  // leading zeros carry no value
            leading = false;
            if (ndigits >= DEC_MAX_DIGITS) return false;
            digits[ndigits++] = c;
        } else if (c == '.' && !seen_point) {
            seen_point = true;
        } else {
            break;
        }
    }
    long exp = 0;
    if (i < len && (s[i] == 'E' || s[i] == 'e')) {
        ++i;
        bool eneg = false;
        if (i < len && (s[i] == '-' || s[i] == '+')) {
            eneg = s[i] == '-';
            ++i;
        }
        if (i >= len) return false;
        for (; i < len; ++i) {
            if (s[i] < '0' || s[i] > '9') return false;
            exp = exp * 10 + (s[i] - '0');
            if (exp > 100000) return false;
        }
        if (eneg) exp = -exp;
    }
    if (i != len) return false;
    const long exponent = exp - frac;
    if (exponent > 0 || -exponent > DEC_MAX_DIGITS) return false;
    __int128 v = 0;
    for (int k = 0; k < ndigits; ++k) v = v * 10 + (digits[k] - '0');
    negzero = neg && v == 0;
    out = neg ? -v : v;
    scale = static_cast<uint8_t>(-exponent);
    return true;
}

// Plain-notation text that Decimal() parses back to exactly this value. Returns length.
inline int dec_plain_text(__int128 v, uint8_t scale, bool negzero, char* buf) noexcept {
    char d[48];
    const int n = dec_digits(v, d);
    int k = 0;
    if (v < 0 || negzero) buf[k++] = '-';
    if (scale == 0) {
        std::memcpy(buf + k, d, static_cast<size_t>(n));
        return k + n;
    }
    if (n > scale) {
        std::memcpy(buf + k, d, static_cast<size_t>(n - scale));
        k += n - scale;
        buf[k++] = '.';
        std::memcpy(buf + k, d + (n - scale), scale);
        return k + scale;
    }
    buf[k++] = '0';
    buf[k++] = '.';
    for (int z = 0; z < scale - n; ++z) buf[k++] = '0';
    std::memcpy(buf + k, d, static_cast<size_t>(n));
    return k + n;
}

// len(str(Decimal)) without building it: the spec's to-scientific-string, which switches
// to exponent notation once the adjusted exponent drops below -6. v1 sized a Decimal as
// the msgpack string str(value), so this is what nbytes() needs.
inline int64_t dec_str_length(__int128 v, uint8_t scale, bool negzero) noexcept {
    char d[48];
    const int64_t n = dec_digits(v, d);
    const int64_t sign = (v < 0 || negzero) ? 1 : 0;
    const int64_t adjusted = -static_cast<int64_t>(scale) + n - 1;
    if (adjusted >= -6) {
        if (scale == 0) return sign + n;
        if (n > scale) return sign + n + 1;
        return sign + 2 + scale;
    }
    int64_t exp_digits = 0;
    for (int64_t a = -adjusted; a > 0; a /= 10) ++exp_digits;
    return sign + n + (n > 1 ? 1 : 0) + 1 /* E */ + 1 /* - */ + exp_digits;
}

// A new Decimal for the slot value, or nullptr with an exception set.
inline PyObject* dec_to_python(__int128 v, uint8_t param) {
    char buf[96];
    const int n = dec_plain_text(v, param & DEC_SCALE_MASK, (param & DEC_NEG_ZERO) != 0, buf);
    PyObject* text = PyUnicode_FromStringAndSize(buf, n);
    if (text == nullptr) return nullptr;
    PyObject* out = PyObject_CallOneArg(g_decimal_type, text);
    Py_DECREF(text);
    return out;
}

}  // namespace orso
