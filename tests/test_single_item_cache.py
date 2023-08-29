import os
import sys

from unittest.mock import Mock

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.tools import single_item_cache


def test_single_item_cache():
    # Mock function to record calls and return the argument
    mock_func = Mock(side_effect=lambda *args, **kwargs: args[0] if args else kwargs.get("arg"))
    cached_func = single_item_cache(mock_func)

    # Test 1: Basic functionality
    assert cached_func("test") == "test"
    assert mock_func.call_count == 1  # Function was called

    # Test 2: Cache hit
    assert cached_func("test") == "test"
    assert mock_func.call_count == 1  # Function was not called

    # Test 3: Cache miss, new argument
    assert cached_func("new test") == "new test"
    assert mock_func.call_count == 2  # Function was called

    # Test 4: Cache hit, same argument as before
    assert cached_func("new test") == "new test"
    assert mock_func.call_count == 2  # Function was not called

    # Test 5: Cache miss, old argument
    assert cached_func("test") == "test"
    assert mock_func.call_count == 3  # Function was called

    # Test 6: Complex arguments
    assert cached_func(("test",)) == ("test",)
    assert mock_func.call_count == 4  # Function was called

    # Test 7: Cache hit, complex arguments
    assert cached_func(("test",)) == ("test",)
    assert mock_func.call_count == 4  # Function was not called

    # Test 8: Keyword arguments
    assert cached_func(arg="test") == "test"
    assert mock_func.call_count == 5  # Function was called

    # Test 9: Cache hit, keyword arguments
    assert cached_func(arg="test") == "test"
    assert mock_func.call_count == 5  # Function was not called


def test_single_item_cache_with_same_arguments():
    counter = [0]

    @single_item_cache
    def foo(x):
        counter[0] += 1
        return x * 2

    # First call should increase counter
    assert foo(1) == 2
    assert counter[0] == 1

    # Second call with same argument should not increase counter (cache hit)
    assert foo(1) == 2
    assert counter[0] == 1


def test_single_item_cache_with_different_arguments():
    counter = [0]

    @single_item_cache
    def foo(x):
        counter[0] += 1
        return x * 2

    assert foo(1) == 2
    assert counter[0] == 1

    # Different argument, should increase counter (cache miss)
    assert foo(2) == 4
    assert counter[0] == 2


def test_single_item_cache_with_same_then_different_then_same_arguments():
    counter = [0]

    @single_item_cache
    def foo(x):
        counter[0] += 1
        return x * 2

    assert foo(1) == 2
    assert counter[0] == 1

    assert foo(2) == 4
    assert counter[0] == 2

    # Back to original argument, should increase counter (cache miss due to intermediate different call)
    assert foo(1) == 2
    assert counter[0] == 3


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
