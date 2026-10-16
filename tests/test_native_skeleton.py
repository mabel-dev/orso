"""
Phase 1 of the v2 rewrite (DESIGN.md §10): the native extension exists, builds with the
vendored draken headers, and the record layout arithmetic is what §4.1 says. No
behaviour lives here yet; these tests pin the skeleton so later phases build on
something that is known to compile and load.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso import _native as native  # noqa: E402


def test_module_identity():
    assert native.__name__ == "orso._native"
    # the commit of opteryx-core the draken headers were vendored from
    assert len(native.draken_source()) >= 8


def test_slot_formats_are_sixteen_bytes():
    report = native.abi_report()
    assert report["slot_bytes"] == 16
    assert report["string_slot_bytes"] == 16
    assert report["interval_slot_bytes"] == 16
    assert report["decimal128_bytes"] == 16
    # draken's frozen vector ABI, which the strided gather targets
    assert report["draken_vector_bytes"] == 40


@pytest.mark.parametrize(
    "nfields, stride",
    [(1, 32), (2, 48), (6, 112), (8, 144), (9, 176), (20, 368), (64, 1152)],
)
def test_record_stride(nfields, stride):
    assert native.record_stride(nfields) == stride
    # slots stay 16-byte aligned: the tag block is padded to a multiple of 16
    assert (stride - nfields * 16) % 16 == 0


def test_iso_timestamp_probe():
    # proves iso_datetime.h compiles and links under orso's flags; the real parsers
    # arrive in phase 4
    assert native.parse_timestamp_us("2024-01-01T00:00:00") == 1_704_067_200_000_000
    assert native.parse_timestamp_us("2024-01-01 12:34:56.789") == 1_704_112_496_789_000
    assert native.parse_timestamp_us("2024-01-01T00:00:00Z") == 1_704_067_200_000_000
    # an offset is honoured, not dropped (DESIGN.md §8 fix 11)
    assert native.parse_timestamp_us("2024-01-01T01:00:00+01:00") == 1_704_067_200_000_000
    assert native.parse_timestamp_us("not a date") is None


def test_ryu_probe():
    # proves the ryu C sources compile alongside the C++ unit
    for value in (0.1, 1.0, -2.5e300, 123456.789):
        assert float(native.format_double(value)) == value
