"""
Phase 6: hashing moved from the xxhash package to the vendored xxhash.h. mabel persists
CityHash64 values (which were always xxHash64), so they must not change.
"""

import os
import random
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

xxhash = pytest.importorskip("xxhash")

import orso
from orso import _native
from orso.cityhash import CityHash32
from orso.cityhash import CityHash64


def test_native_xxhash_matches_the_package():
    rng = random.Random(3)
    for _ in range(5000):
        data = bytes(rng.getrandbits(8) for _ in range(rng.randint(0, 300)))
        assert _native.xxh64(data) == xxhash.xxh64(data).intdigest()
        assert _native.xxh32(data) == xxhash.xxh32(data).intdigest()


@pytest.mark.parametrize(
    "value", [b"", b"abc", "text", "日本", 12345, 1.5, None, ("a", 1), bytearray(b"x")]
)
def test_cityhash_values_unchanged(value):
    data = value if isinstance(value, bytes) else str(value).encode()
    assert CityHash64(value) == xxhash.xxh64(data).intdigest()
    assert CityHash32(value) == xxhash.xxh32(data).intdigest()


def test_dataframe_hash_unchanged():
    df = orso.DataFrame(rows=[(1, "a"), (2, "b"), (3, None)], schema=["x", "y"])
    expected = 0
    for i, row in enumerate(df):
        expected = i ^ expected ^ xxhash.xxh64(str(row).encode()).intdigest()
    assert hash(df) == expected or hash(df) == hash(
        expected
    )  # Python folds __hash__ results to Py_ssize_t
