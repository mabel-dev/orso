import os
import sys


sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pytest
import time

# Import the LRU cache decorator
from orso.tools import lru_cache_with_expiry
from orso.tools import random_string

@lru_cache_with_expiry(max_size=3, valid_for_seconds=1)
def sample_function(x, y):
    return random_string()

def test_cache_basic_functionality():
    # Initial call, should not be cached
    result1 = sample_function(1, 2)

    # Call with same arguments, should be cached
    result2 = sample_function(1, 2)
    assert result2 == result1

def test_cache_expiry():
    # Initial call, should not be cached
    result1 = sample_function(2, 3)

    # Sleep for longer than the cache validity period
    time.sleep(1)

    # Call with same arguments, cache should have expired
    result2 = sample_function(2, 3)
    assert result2 != result1

def test_cache_lru_eviction():
    # Fill the cache with three different items
    result1 = sample_function(1, 2)  # Should be cached
    result2 = sample_function(2, 3)  # Should be cached
    result3 = sample_function(3, 4)  # Should be cached

    # At this point, the cache is full. The next item should cause the oldest item to be evicted
    result4 = sample_function(4, 5)  # Should be cached, (1, 2) should be evicted

    # Accessing the first item should result in a cache miss and recalculation
    result1_again = sample_function(1, 2)

    assert result1_again != result1  # New result as the old one should have been evicted

    # Ensure the recently accessed items are still cached
    result3_again = sample_function(3, 4)
    assert result3_again == result3


def test_cache_lru_access_order():
    # Fill the cache with three different items
    result1 = sample_function(5, 6)  # Should be cached
    result2 = sample_function(6, 7)  # Should be cached
    result3 = sample_function(7, 8)  # Should be cached

    # Access the first item to make it recently used
    result1_again = sample_function(5, 6)
    assert result1_again == result1

    # Add a new item to the cache, causing the LRU item to be evicted
    result4 = sample_function(8, 9)  # Should be cached, (6, 7) should be evicted as it is now the LRU

    # Accessing the evicted item should result in a cache miss and recalculation
    result2_again = sample_function(6, 7)
    assert result2_again != result2  # New result as the old one should have been evicted

if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
