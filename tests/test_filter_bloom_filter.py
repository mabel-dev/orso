import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.filters.bloom_filter import BloomFilter, _get_hash_count, _get_size

ITERATIONS: int = 50000


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
    bf = BloomFilter()
    for token in tokens:
        bf.add(token)

    # then we test. 100% shouldn't match (we use different string lengths)
    # but we're a probabilistic filter so expect some false positives
    # test a lot more cycles than we used to populate
    SCALE_FACTOR = 10
    tokens = (random_string(32) for i in range(ITERATIONS * SCALE_FACTOR))
    collisions = 0
    for token in tokens:
        if token in bf:
            collisions += 1

    # this is approximately 1% false positive rate, we're going to test between
    # 0.25 and 1.75 because this is probabilistic so are unlikely to actually get 1%
    assert (
        (ITERATIONS * SCALE_FACTOR * 0.025) < collisions < (ITERATIONS * SCALE_FACTOR * 0.175)
    ), collisions / (ITERATIONS * SCALE_FACTOR)


def test_bloom_contains():
    bf = BloomFilter()
    bf.add("test")
    assert "test" in bf
    assert "nonexistent" not in bf


def test_get_size():
    assert _get_size(50000, 0.01) == 479253, _get_size(50000, 0.01)
    assert _get_size(100000, 0.1) == 479253


def test_get_hash_count():
    assert _get_hash_count(479253, 50000) == 7, _get_hash_count(479253, 50000)
    assert _get_hash_count(479253, 100000) == 3


def test_init():
    bf = BloomFilter()
    assert bf.filter_size == 479253
    assert bf.hash_count == 7
    assert len(bf.hash_seeds) == 7
    assert bf.bits.size == 479253, bf.bits.size
    assert sum(bf.bits.array) == 0


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
