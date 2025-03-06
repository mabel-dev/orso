from opteryx.third_party.cyan4973.xxhash import hash_bytes

def CityHash32(data: bytes) -> int:
    if not isinstance(data, bytes):
        data = str(data).encode()
    return hash_bytes(data) & 0xFFFFFFFF

def CityHash64(data: bytes) -> int:
    if not isinstance(data, bytes):
        data = str(data).encode()
    return hash_bytes(data) & 0xFFFFFFFFFFFFFFFF