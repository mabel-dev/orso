import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.filters.cuckoo_filter import CuckooFilter

ITERATIONS: int = 50000


def random_string(width: int = 16):
    import random

    num_chars = ((width + 1) >> 1) << 3  # Convert length to number of bits
    rand_bytes = random.getrandbits(num_chars)  # Generate random bytes
    rand_hex = hex(rand_bytes)[
        2 : width + 2
    ]  # Convert to hex string and truncate to desired length
    return rand_hex


def test_cuckoo_filter():
    # first we populate the BloomFilter
    tokens = (random_string(48) for i in range(ITERATIONS))
    bf = CuckooFilter()
    for token in tokens:
        bf.add(token)

    # then we test. 100% shouldn't match (we use different string lengths)
    # but we're a probabilistic filter so expect some false positives
    tokens = (random_string(32) for i in range(ITERATIONS))
    collisions = 0
    for token in tokens:
        if token in bf:
            collisions += 1

    # should be about 0.006
    assert (ITERATIONS * 0.001) < collisions < (ITERATIONS * 0.01), collisions / ITERATIONS


def test_cuckoo_contains():
    bf = CuckooFilter()
    bf.add("test")
    assert "test" in bf
    assert "nonexistent" not in bf


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
