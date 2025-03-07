def CityHash32(data: bytes) -> int:
    from xxhash import xxh32

    if not isinstance(data, bytes):
        data = str(data).encode()
    return xxh32(data).intdigest()


def CityHash64(data: bytes) -> int:
    from xxhash import xxh64

    if not isinstance(data, bytes):
        data = str(data).encode()
    return xxh64(data).intdigest()
