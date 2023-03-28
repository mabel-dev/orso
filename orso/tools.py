# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from functools import wraps


def retry(
    max_tries: int = 3,
    backoff_seconds: int = 1,
    exponential_backoff: bool = False,
    max_backoff: int = 4,
):
    """
    max_tries: int=3, the number of times to execute the function
    backoff_seconds: int=1, the amount of time between retries
    exponential_backoff: bool=False, double the backoff period between retries
    max_backoff: int=4, the maximum backoff (not including the first)
    """

    def decorator_retry(func):
        @wraps(func)
        def wrapper_retry(*args, **kwargs):
            import time

            tries = 0
            this_delay = backoff_seconds
            while tries < max_tries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    tries += 1
                    if tries == max_tries:
                        print(
                            f"`{func.__name__}` failed with `{type(e).__name__}` error, attempt {tries} of {max_tries}. Aborting."
                        )
                        raise e
                    print(
                        f"`{func.__name__}` failed with `{type(e).__name__}` error, attempt {tries} of {max_tries}. Will retry in {this_delay} seconds."
                    )
                    time.sleep(this_delay)
                    if exponential_backoff:
                        this_delay *= 2
                        this_delay = min(this_delay, max_backoff)

        return wrapper_retry

    return decorator_retry


def timed(func):
    """
    Run and report the time for a function
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        import time

        start_time = time.monotonic_ns()
        result = func(*args, **kwargs)
        end_time = time.monotonic_ns()
        print(f"Function {func.__name__} took {(end_time - start_time)/1e9} seconds to run.")
        return result

    return wrapper


def log(func):
    """
    Log when a function starts and finishes.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        import datetime
        import uuid

        unique_marker = str(uuid.uuid4())

        print(f"{datetime.datetime.now()} - Executing {func.__name__} - {unique_marker}")
        result = func(*args, **kwargs)
        print(f"{datetime.datetime.now()} - Finished executing {func.__name__} - {unique_marker}")
        return result

    return wrapper


def repeat(number_of_times: int = None, capture_results: bool = False):
    """
    Repeat a function a given number of times
    """
    if not isinstance(number_of_times, int) or number_of_times < 0:
        raise ValueError("@repeat requires a 'number_of_times' to be set to a positive integer")

    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            results = []
            for _ in range(number_of_times):
                result = func(*args, **kwargs)
                if capture_results:
                    results.append(result)
            if capture_results:
                return results
            return None

        return wrapper

    return decorate


def monitor(func):
    """
    Add an execution counter and timer to a function
    """
    import time

    def report(self):
        stats = f"\nExecution Statistics for `{self.__name__}`\n  Count   : {self.count}\n"
        if self.count > 0:
            stats += f"  Average : {sum(self._run_times) / self.count} seconds\n"
            stats += f"  Slowest : {min(self._run_times)} seconds\n"
            stats += f"  Fastest : {max(self._run_times)} seconds\n"
        return stats

    @wraps(func)
    def wrapper(*args, **kwargs):
        wrapper.count += 1  # type:ignore
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        wrapper._run_times.append(time.monotonic() - start_time)  # type:ignore
        return result

    wrapper.count = 0  # type:ignore
    wrapper._run_times = []  # type:ignore
    wrapper.stats = lambda: report(wrapper)  # type:ignore
    return wrapper


def throttle(calls_per_second: float):
    import time

    if calls_per_second <= 0:
        raise ValueError("@throttle requires calls_per_second to be greater than zero")
    min_interval = 1.0 / float(calls_per_second)

    def decorate(func):
        last_time_called = 0.0

        @wraps(func)
        def rate_limited_function(*args, **kargs):
            elapsed = time.monotonic() - last_time_called
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            ret = func(*args, **kargs)
            last_time_called = time.monotonic()
            return ret

        return rate_limited_function

    return decorate


def islice(iterator, size):
    for i in range(size):
        yield next(iterator)
