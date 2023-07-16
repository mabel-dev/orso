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

import decimal
from functools import wraps
from random import getrandbits


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


def counter(func):
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
            nonlocal last_time_called
            elapsed = time.monotonic() - last_time_called
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            ret = func(*args, **kargs)
            last_time_called = time.monotonic()
            return ret

        return rate_limited_function

    return decorate


def monitor(time_limit=10, interval=1):
    import os
    import threading
    import time

    from orso.exceptions import MissingDependencyError

    try:
        import psutil
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error

    def decorator(func):
        stop_flag = False

        def wrapper(*args, **kwargs):
            nonlocal stop_flag
            stop_flag = False

            # Use a thread to monitor the resource usage
            def monitor():
                peak_cpu = 0
                peak_memory = 0
                cpu_tracker = []
                memory_tracker = []

                process = psutil.Process(os.getpid())
                while not stop_flag:
                    cpu_percent = process.cpu_percent(interval=interval)
                    memory_info = process.memory_info().rss

                    cpu_tracker.append(cpu_percent)
                    memory_tracker.append(memory_info)
                    if len(cpu_tracker) > time_limit:
                        cpu_tracker.pop(0)
                        memory_tracker.pop(0)

                    if cpu_percent > peak_cpu:
                        peak_cpu = cpu_percent
                    if memory_info > peak_memory:
                        peak_memory = memory_info

                print(f"Peak CPU usage: {peak_cpu:.2f}%")
                print(f"Peak memory usage: {peak_memory/1024/1024:.2f} MB")

            monitor_thread = threading.Thread(target=monitor)
            monitor_thread.start()

            try:
                start_time = time.monotonic_ns()
                result = func(*args, **kwargs)
                end_time = time.monotonic_ns()
                print(f"Execution time: {(end_time - start_time)/1e9:.6f} seconds")
                return result
            except Exception as e:
                print(f"Error raised: {type(e).__name__}")
                end_time = time.monotonic_ns()
                print(f"Execution time: {(end_time - start_time)/1e9:.6f} seconds")
                raise e
            finally:
                stop_flag = True
                monitor_thread.join()

        return wrapper

    return decorator


def islice(iterator, size):
    for i in range(size):
        yield next(iterator)


class DecimalFactory(decimal.Decimal):
    scale = 0
    precision = 0

    def __call__(self, value):
        context = decimal.Context(prec=self.precision)
        quantized_value = context.create_decimal(value).quantize(decimal.Decimal(10) ** -self.scale)
        return decimal.Decimal(quantized_value)

    def __str__(self):
        return f"Decimal({self.scale},{self.precision})"

    @classmethod
    def new_factory(cls, precision: int, scale: int):
        factory = DecimalFactory.__new__(cls)
        factory.scale = scale
        factory.precision = precision
        return factory


def arrow_type_map(parquet_type):
    import datetime

    from orso.exceptions import MissingDependencyError

    try:
        import pyarrow.lib as lib
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error

    if parquet_type.id == lib.Type_NA:
        return None
    if parquet_type.id == lib.Type_BOOL:
        return bool
    if parquet_type.id in {lib.Type_STRING, lib.Type_LARGE_STRING}:
        return str
    if parquet_type.id in {
        lib.Type_INT8,
        lib.Type_INT16,
        lib.Type_INT32,
        lib.Type_INT64,
        lib.Type_UINT8,
        lib.Type_UINT16,
        lib.Type_UINT32,
        lib.Type_UINT64,
    }:
        return int
    if parquet_type.id in {lib.Type_HALF_FLOAT, lib.Type_FLOAT, lib.Type_DOUBLE}:
        return float
    if parquet_type.id in {lib.Type_DECIMAL128, lib.Type_DECIMAL256}:
        return DecimalFactory.new_factory(parquet_type.precision, parquet_type.scale)
    if parquet_type.id in {lib.Type_DATE32}:
        return datetime.date
    if parquet_type.id in {lib.Type_DATE64, 18}:  # not sure what 18 maps to
        return datetime.datetime
    if parquet_type.id in {lib.Type_TIME32, lib.Type_TIME64}:
        return datetime.time
    if parquet_type.id in {lib.Type_INTERVAL_MONTH_DAY_NANO, lib.Type_DURATION}:
        return datetime.timedelta
    if parquet_type.id in {lib.Type_LIST, lib.Type_LARGE_LIST, lib.Type_FIXED_SIZE_LIST}:
        return list
    if parquet_type.id in {lib.Type_STRUCT, lib.Type_MAP}:
        return dict
    if parquet_type.id in {lib.Type_BINARY, lib.Type_LARGE_BINARY}:
        return bytes
    # _UNION_TYPES = {lib.Type_SPARSE_UNION, lib.Type_DENSE_UNION}
    raise ValueError(f"Unable to map parquet type {parquet_type} ({parquet_type.id})")


def random_int() -> int:
    """
    Select a random integer (32bit)
    """
    return getrandbits(32)


def random_string(width: int = 16):
    """
    I've not been able to find a faster way to generate short random strings.
    """
    num_chars = ((width + 1) >> 1) << 3  # Convert length to number of bits
    rand_bytes = getrandbits(num_chars)  # Generate random bytes
    # Convert to hex string and truncate to desired length, we clip the '0x'
    # the start of the hex string, and zero-fill the start of the string
    return ("000000" + hex(rand_bytes)[2:])[-width:]
