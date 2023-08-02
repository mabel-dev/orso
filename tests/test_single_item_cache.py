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


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
