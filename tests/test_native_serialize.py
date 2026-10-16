"""
Phase 5 of the v2 rewrite (DESIGN.md §10): Row.as_bytes, Row.from_bytes and Row.as_json
written natively, byte-identical to v1.

v1's definitions are the oracle: ormsgpack.packb(tuple(row), option=OPT_SERIALIZE_NUMPY,
default=serialize) for the payload, orso.compute.compiled.from_bytes_cython for reading,
and json.dumps(row.as_dict, default=str, separators=(",", ":")) for JSON.
"""

import dataclasses
import datetime
import decimal
import enum
import json
import os
import random
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import numpy
from ormsgpack import OPT_SERIALIZE_NUMPY
from ormsgpack import packb

from orso.compute.compiled import from_bytes_cython
from orso.exceptions import DataError
from orso.row import Row
from orso.row import _serialize

UTC = datetime.timezone.utc


class Colour(enum.Enum):
    RED = "red"


class Level(enum.IntEnum):
    HIGH = 3


class Name(str):
    pass


@dataclasses.dataclass
class Point:
    x: int


VALUES = [
    None, True, False, 0, 1, 127, 128, 255, 256, 65535, 65536, 2**32 - 1, 2**32, 2**63 - 1, 2**63,
    2**64 - 1, -1, -32, -33, -128, -129, -32768, -32769, -(2**31), -(2**31) - 1, -(2**63),
    0.0, -0.0, 1.5, 1e300, float("inf"), float("-inf"), float("nan"), 1e16, 123456789.125,
    "", "a", "a" * 31, "a" * 32, "a" * 255, "a" * 256, "a" * 70000, "é", "日本", "🐻 bear",
    "quote\" backslash\\ tab\t newline\n bell\x07 del\x7f", Name("subclass"),
    b"", b"\x00\xff", b"x" * 300, b"y" * 70000,
    datetime.datetime(2024, 1, 2, 3, 4, 5), datetime.datetime(2024, 1, 2, 3, 4, 5, 6),
    datetime.datetime(1, 1, 1), datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
    datetime.datetime(2024, 1, 1, tzinfo=UTC), datetime.date(2024, 2, 29), datetime.date(1, 1, 1),
    datetime.time(0, 0), datetime.time(23, 59, 59, 1), datetime.timedelta(days=2, seconds=3),
    decimal.Decimal("1.50"), decimal.Decimal("-0"), decimal.Decimal("NaN"),
    [], (), {}, [1, "a", None, [2.5, {"k": (b"x",)}]], {"a": 1, "b": [True]}, list(range(16)),
    {str(i): i for i in range(16)}, Colour.RED, Level.HIGH, uuid.UUID(int=7), Point(1),
    numpy.int64(5), numpy.float32(1.5), numpy.bool_(True), numpy.array([1, 2]),
    numpy.datetime64("2024-01-01T00:00:00"), bytearray(b"ab"), memoryview(b"cd"),
]


def v1_payload(row):
    return packb(tuple(row), option=OPT_SERIALIZE_NUMPY, default=_serialize)


def v1_json(row):
    return json.dumps(row.as_dict, default=str, separators=(",", ":")).encode()


def random_rows(n, width=6, seed=5):
    rng = random.Random(seed)
    factory = Row.create_class([f"c{i}" for i in range(width)])
    return [factory([rng.choice(VALUES) for _ in range(width)]) for _ in range(n)]


@pytest.mark.parametrize("value", VALUES, ids=lambda v: f"{type(v).__name__}:{str(v)[:16]}")
def test_payload_is_v1_bytes(value):
    row = Row.create_class(["v"])([value])
    try:
        expected = v1_payload(row)
    except Exception as err:
        with pytest.raises(type(err)):
            row._packed()
        return
    assert row._packed() == expected


def test_payload_is_v1_bytes_for_random_rows():
    for row in random_rows(3000):
        try:
            expected = v1_payload(row)
        except TypeError:
            with pytest.raises(TypeError):
                row._packed()
            continue
        assert row._packed() == expected, row


def test_as_bytes_layout_and_round_trip():
    for row in random_rows(500, seed=8):
        try:
            payload = v1_payload(row)
        except TypeError:
            continue
        record = row.as_bytes
        assert record[:2] == b"\x10\x00"
        assert int.from_bytes(record[2:6], "big") == len(payload)
        assert record[14:] == payload
        native, v1 = Row.from_bytes(record), from_bytes_cython(record)
        # repr compares NaN as NaN, and the types must match too
        assert repr(tuple(native)) == repr(v1) and [type(x) for x in native] == [type(x) for x in v1]


def test_from_bytes_errors_match_v1():
    record = Row.create_class(["a"])([1]).as_bytes
    for bad in (b"", b"\x20" + record[1:], record[:-1], record + b"x"):
        with pytest.raises(DataError) as native:
            Row.from_bytes(bad)
        with pytest.raises(DataError) as v1:
            from_bytes_cython(bad)
        assert str(native.value) == str(v1.value)


def test_oversized_record_raises():
    from orso.row import MAXIMUM_RECORD_SIZE

    with pytest.raises(DataError):
        Row(["a" * MAXIMUM_RECORD_SIZE]).as_bytes


def test_as_json_is_v1_bytes_for_random_rows():
    for row in random_rows(3000, seed=9):
        try:
            expected = v1_json(row)
        except (TypeError, ValueError) as err:
            with pytest.raises(type(err)):
                row.as_json
            continue
        assert row.as_json == expected, row


@pytest.mark.parametrize(
    "value",
    [
        {1: "int key", 2.5: "float key", True: "bool", None: "none"},
        {float("nan"): 1},
        {(1, 2): "tuple key"},  # Python's json raises
        [[[[[]]]]],
    ],
    ids=["keys", "nan-key", "tuple-key", "nested"],
)
def test_as_json_edge_values(value):
    row = Row.create_class(["v"])([value])
    try:
        expected = v1_json(row)
    except TypeError:
        with pytest.raises(TypeError):
            row.as_json
        return
    assert row.as_json == expected


def test_as_json_circular_falls_back_to_python():
    loop = []
    loop.append(loop)
    row = Row.create_class(["v"])([loop])
    with pytest.raises(ValueError, match="Circular"):
        row.as_json


def test_as_json_duplicate_fields_falls_back():
    row = Row.create_class(["a", "a"])([1, 2])
    assert row.as_json == v1_json(row) == b'{"a":2}'


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()


# ---------------------------------------------------------------------------------
# display: the native trunc_printable against v1's, verbatim
# ---------------------------------------------------------------------------------


def _v1_character_width(symbol):
    if ord(symbol) < 128:
        return 1
    import unicodedata

    return 2 if unicodedata.east_asian_width(symbol) in ("F", "W") else 1


def _v1_trunc_printable(value, width=None, full_line: bool = True):
    simple = ("\n" not in value) and ("\r" not in value) and ("\033" not in value) and ("\001" not in value)
    if simple:
        if width is not None:
            if len(value) > width:
                value = value[:width]
            if full_line and len(value) < width:
                value = value + " " * (width - len(value))
        return value + "\001OFFm"
    offset = 0
    emit = []
    ignoring = False
    for char in value:
        if char == "\n":
            emit.append("\001CRLFm↵\001VARCHARm")
            offset += 1
            continue
        if char == "\r":
            continue
        emit.append(char)
        if char in ("\033", "\001"):
            ignoring = True
        if not ignoring:
            offset += _v1_character_width(char)
        if ignoring and char == "m":
            ignoring = False
        if width is not None and not ignoring and offset >= width:
            emit.append("\001OFFm")
            return "".join(emit)
    emit.append("\001OFFm")
    line = "".join(emit)
    if full_line and width is not None:
        return line + " " * (width - offset)
    return line


def test_trunc_printable_matches_v1():
    from orso import _native

    rng = random.Random(12)
    alphabet = list("abc xyz") + ["\n", "\r", "\033", "\001", "m", "[", "3", "1"] + list("日本語漢字") + ["é", "̀", "🐻", "ｱ", "\t"]
    for _ in range(20000):
        value = "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 40)))
        width = rng.choice([None, 0, 1, 5, 12, 30, 80, -3])
        full_line = rng.random() < 0.7
        assert _native.trunc_printable(value, width, full_line) == _v1_trunc_printable(value, width, full_line), (
            value, width, full_line,
        )
