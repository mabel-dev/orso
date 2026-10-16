# These were always xxHash, not CityHash; the names are what mabel imports. v2 computes
# them natively (the vendored xxhash.h) instead of through the xxhash package, with the
# same values.

from orso._native import xxh32 as _xxh32
from orso._native import xxh64 as _xxh64


def CityHash32(data: bytes) -> int:
    if not isinstance(data, bytes):
        data = str(data).encode()
    return _xxh32(data)


def CityHash64(data: bytes) -> int:
    if not isinstance(data, bytes):
        data = str(data).encode()
    return _xxh64(data)
