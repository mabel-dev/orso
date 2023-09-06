import os
import sys
import time
from unittest.mock import Mock

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.tools import single_item_cache


def test_single_item_cache():
    mock_func = Mock(side_effect=lambda *args, **kwargs: args[0] if args else kwargs.get("arg"))
    cached_func = single_item_cache(mock_func)

    # Test 1: Basic functionality
    assert cached_func("test") == "test"
    assert mock_func.call_count == 1

    # Test 2: Cache hit
    assert cached_func("test") == "test"
    assert mock_func.call_count == 1

    # Test 3: Cache miss, new argument
    assert cached_func("new test") == "new test"
    assert mock_func.call_count == 2

    # Test 4: Cache hit, same argument as before
    assert cached_func("new test") == "new test"
    assert mock_func.call_count == 2

    # Test 5: Cache miss, old argument
    assert cached_func("test") == "test"
    assert mock_func.call_count == 3, mock_func.call_count

    # Test 6: Complex arguments
    assert cached_func(("test",)) == ("test",)
    assert mock_func.call_count == 4

    # Test 7: Cache hit, complex arguments
    assert cached_func(("test",)) == ("test",)
    assert mock_func.call_count == 4

    # Test 8: Keyword arguments
    assert cached_func(arg="test") == "test"
    assert mock_func.call_count == 5

    # Test 9: Cache hit, keyword arguments
    assert cached_func(arg="test") == "test"
    assert mock_func.call_count == 5


def test_single_item_cache_with_same_arguments():
    counter = [0]

    @single_item_cache()
    def foo(x):
        counter[0] += 1
        return x * 2

    assert foo(1) == 2
    assert counter[0] == 1

    assert foo(1) == 2
    assert counter[0] == 1


def test_single_item_cache_with_different_arguments():
    counter = [0]

    @single_item_cache()
    def foo(x):
        counter[0] += 1
        return x * 2

    assert foo(1) == 2
    assert counter[0] == 1

    assert foo(2) == 4
    assert counter[0] == 2


def test_single_item_cache_with_same_then_different_then_same_arguments():
    counter = [0]

    @single_item_cache()
    def foo(x):
        counter[0] += 1
        return x * 2

    assert foo(1) == 2
    assert counter[0] == 1

    assert foo(2) == 4
    assert counter[0] == 2

    assert foo(1) == 2
    assert counter[0] == 3


def test_single_item_cache_with_expiration():
    counter = [0]

    @single_item_cache(valid_for_seconds=1)
    def foo(x):
        counter[0] += 1
        return x * 2

    assert foo(1) == 2
    assert counter[0] == 1

    assert foo(1) == 2
    assert counter[0] == 1

    time.sleep(1.1)

    assert foo(1) == 2
    assert counter[0] == 2


if __name__ == "__main__":  # pragma: nocover
    from tests import run_tests

    test_single_item_cache()

    run_tests()
