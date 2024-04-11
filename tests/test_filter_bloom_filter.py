import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.compute.bloom_filter import create_bloom_filter

ITERATIONS: int = 1000


def random_string(width: int = 16):
    import random

    num_chars = ((width + 1) >> 1) << 3  # Convert length to number of bits
    rand_bytes = random.getrandbits(num_chars)  # Generate random bytes
    rand_hex = hex(rand_bytes)[
        2 : width + 2
    ]  # Convert to hex string and truncate to desired length
    return rand_hex


def test_bloom_filter():
    # first we populate the BloomFilter
    tokens = (random_string(48) for i in range(ITERATIONS))
    bf = create_bloom_filter(19000, [])
    for token in tokens:
        bf.add(hash(token))

    # then we test. 100% shouldn't match (we use different string lengths)
    # but we're a probabilistic filter so expect some false positives
    tokens = (hash(random_string(32)) for i in range(ITERATIONS))
    collisions = 0
    for token in tokens:
        if bf.possibly_contains(hash(token)):
            collisions += 1

    # this is approximately 1% false positive rate, we're going to test between
    # 0.5% and 5% because this is probabilistic so are unlikely to actually get 1%
    assert (ITERATIONS * 0.005) < collisions < (ITERATIONS * 0.05), collisions / (ITERATIONS)


def test_bloom_contains():
    bf = create_bloom_filter(1000, [123])
    assert bf.possibly_contains(123)
    assert not bf.possibly_contains(888) and not bf.possibly_contains(321)


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
