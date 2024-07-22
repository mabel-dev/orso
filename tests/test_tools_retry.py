import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pytest
from unittest.mock import Mock, patch
from orso.tools import retry  # assuming the decorator is saved in retry_decorator.py


def test_retry_success_on_first_try():
    mock_func = Mock(return_value="success")

    @retry(max_tries=3)
    def decorated_func():
        return mock_func()

    with patch("time.sleep", return_value=None) as mock_sleep:
        result = decorated_func()
        assert result == "success"
        mock_func.assert_called_once()
        mock_sleep.assert_not_called()


def test_retry_success_after_retries():
    mock_func = Mock(side_effect=[Exception("fail"), Exception("fail"), "success"])

    @retry(max_tries=3, backoff_seconds=1)
    def decorated_func():
        return mock_func()

    with patch("time.sleep", return_value=None) as mock_sleep:
        result = decorated_func()
        assert result == "success"
        assert mock_func.call_count == 3
        assert mock_sleep.call_count == 2


def test_retry_failure_after_max_retries():
    mock_func = Mock(side_effect=Exception("fail"))

    @retry(max_tries=3, backoff_seconds=1)
    def decorated_func():
        return mock_func()

    with patch("time.sleep", return_value=None) as mock_sleep:
        with pytest.raises(Exception):
            decorated_func()

        assert mock_func.call_count == 3
        assert mock_sleep.call_count == 2


def test_retry_with_exponential_backoff():
    mock_func = Mock(side_effect=[Exception("fail"), Exception("fail"), "success"])

    @retry(max_tries=3, backoff_seconds=1, exponential_backoff=True, max_backoff=4)
    def decorated_func():
        return mock_func()

    with patch("time.sleep", return_value=None) as mock_sleep:
        result = decorated_func()
        assert result == "success"
        assert mock_func.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)


def test_retry_with_jitter():
    mock_func = Mock(side_effect=[Exception("fail"), Exception("fail"), "success"])

    @retry(max_tries=3, backoff_seconds=1, jitter=True)
    def decorated_func():
        return mock_func()

    with (
        patch("time.sleep", return_value=None) as mock_sleep,
        patch("random.uniform", return_value=0.5) as mock_random,
    ):
        result = decorated_func()
        assert result == "success"
        assert mock_func.call_count == 3
        assert mock_sleep.call_count == 2
        mock_random.assert_called()


def test_retry_with_specific_exceptions():
    mock_func = Mock(side_effect=[ValueError("fail"), ValueError("fail"), "success"])

    @retry(max_tries=3, backoff_seconds=1, retry_exceptions=(ValueError,))
    def decorated_func():
        return mock_func()

    with patch("time.sleep", return_value=None) as mock_sleep:
        result = decorated_func()
        assert result == "success"
        assert mock_func.call_count == 3
        assert mock_sleep.call_count == 2


def test_retry_with_callback():
    mock_func = Mock(side_effect=[Exception("fail"), "success"])
    callback_func = Mock()

    @retry(max_tries=3, backoff_seconds=1, callback=callback_func)
    def decorated_func():
        return mock_func()

    with patch("time.sleep", return_value=None) as mock_sleep:
        result = decorated_func()
        assert result == "success"
        assert mock_func.call_count == 2
        assert mock_sleep.call_count == 1

        # Ensure callback was called once with correct arguments
        assert callback_func.call_count == 1
        called_exception, called_attempt = callback_func.call_args[0]
        assert isinstance(called_exception, Exception)
        assert called_exception.args == ("fail",)
        assert called_attempt == 1


def test_retry_no_retries_needed():
    mock_func = Mock(return_value="success")

    @retry(max_tries=3, backoff_seconds=1)
    def decorated_func():
        return mock_func()

    with patch("time.sleep", return_value=None) as mock_sleep:
        result = decorated_func()
        assert result == "success"
        assert mock_func.call_count == 1
        mock_sleep.assert_not_called()


def test_retry_with_multiple_exceptions():
    mock_func = Mock(side_effect=[ValueError("fail"), KeyError("fail"), "success"])

    @retry(max_tries=3, backoff_seconds=1, retry_exceptions=(ValueError, KeyError))
    def decorated_func():
        return mock_func()

    with patch("time.sleep", return_value=None) as mock_sleep:
        result = decorated_func()
        assert result == "success"
        assert mock_func.call_count == 3
        assert mock_sleep.call_count == 2


def test_retry_with_partial_success():
    mock_func = Mock(
        side_effect=[Exception("fail"), "partial_success", Exception("fail_again"), "success"]
    )

    @retry(max_tries=4, backoff_seconds=1)
    def decorated_func():
        result = mock_func()
        if result == "partial_success":
            raise Exception("retrying due to partial success")
        return result

    with patch("time.sleep", return_value=None) as mock_sleep:
        result = decorated_func()
        assert result == "success"
        assert mock_func.call_count == 4
        assert mock_sleep.call_count == 3


def test_retry_with_max_backoff():
    mock_func = Mock(side_effect=[Exception("fail"), Exception("fail"), "success"])

    @retry(max_tries=3, backoff_seconds=1, exponential_backoff=True, max_backoff=2)
    def decorated_func():
        return mock_func()

    with patch("time.sleep", return_value=None) as mock_sleep:
        result = decorated_func()
        assert result == "success"
        assert mock_func.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)


if __name__ == "__main__":  # pragma: nocover
    from tests import run_tests

    run_tests()
