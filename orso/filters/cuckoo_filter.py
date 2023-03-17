import random

from orso.cityhash import CityHash32

SALTS = ("EPSOM", "TABLE")


class CuckooFilter:
    def __init__(self, capacity: int = 10000, bucket_size: int = 2, fingerprint_size: int = 2):
        self.capacity = capacity
        self.bucket_size = bucket_size
        self.fingerprint_size = fingerprint_size
        self.buckets = [[] for _ in range(capacity)]

    def __len__(self):
        return sum(len(bucket) for bucket in self.buckets)

    def add(self, item):
        item = str(item)

        hash1 = CityHash32(item + SALTS[0])
        fingerprint = hash1 & 0xFFFF
        bucket_index = hash1 % self.capacity

        # Try to insert fingerprint into first bucket
        if fingerprint not in self.buckets[bucket_index]:
            self.buckets[bucket_index].append(fingerprint)
            return True

        # Try to insert fingerprint into second bucket
        hash2 = CityHash32(item + "salt")
        fingerprint = hash2 & 0xFFFF
        bucket_index = hash2 % self.capacity

        if fingerprint not in self.buckets[bucket_index]:
            self.buckets[bucket_index].append(fingerprint)
            return True

        # If both buckets are full, perform eviction
        fingerprint = self.buckets[bucket_index][
            random.randint(0, len(self.buckets[bucket_index]) - 1)
        ]
        self.buckets[bucket_index].remove(fingerprint)

        # Try to insert evicted fingerprint
        return self.add(fingerprint)

    def __contains__(self, item):
        item = str(item)
        for salt in SALTS:
            hash = CityHash32(item + salt)
            fingerprint = hash & 0xFFFF
            bucket_index = hash % self.capacity
            if fingerprint in self.buckets[bucket_index]:
                return True
        return False


if __name__ == "__main__":  # pragma: no cover
    import os
    import sys

    sys.path.insert(1, os.path.join(sys.path[0], "../.."))

    ITERATIONS = 100000
    import time

    from opteryx.utils import random_string

    from orso.filters import BloomFilter

    t = time.monotonic_ns()
    tokens = (random_string(48) for i in range(ITERATIONS))
    bf = CuckooFilter()
    for token in tokens:
        bf.add(token)

    print((time.monotonic_ns() - t) / 1e9)

    # then we test. 100% shouldn't match (we use different string lengths)
    # but we're a probabilistic filter so expect some false positives
    # we're configured for a 1% false positive rate
    tokens = (random_string(32) for i in range(ITERATIONS))
    collisions = 0
    for token in tokens:
        if token in bf:
            collisions += 1

    print(ITERATIONS, collisions, (time.monotonic_ns() - t) / 1e9)

    t = time.monotonic_ns()
    tokens = (random_string(48) for i in range(ITERATIONS))
    bf = BloomFilter(10000)
    for token in tokens:
        bf.add(token)

    print((time.monotonic_ns() - t) / 1e9)

    # then we test. 100% shouldn't match (we use different string lengths)
    # but we're a probabilistic filter so expect some false positives
    # we're configured for a 1% false positive rate
    tokens = (random_string(32) for i in range(ITERATIONS))
    collisions = 0
    for token in tokens:
        if token in bf:
            collisions += 1

    print(ITERATIONS, collisions, (time.monotonic_ns() - t) / 1e9)
