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

import datetime
import decimal
import logging
import os
import random
import threading
import time
import uuid
from collections import OrderedDict
from functools import wraps
from random import getrandbits
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union

import numpy

from orso.exceptions import MissingDependencyError

logger = logging.getLogger(__name__)


def retry(
    max_tries: int = 3,
    backoff_seconds: int = 1,
    exponential_backoff: bool = False,
    max_backoff: int = 4,
    retry_exceptions: Tuple[Type[Exception]] = (Exception,),
    jitter: bool = False,
    callback: Optional[Callable[[Exception, int], None]] = None,
) -> Callable:
    """
    Decorator to add retry logic with optional exponential backoff and jitter to a function.

    Parameters:
        max_tries: int
            The maximum number of attempts to execute the function.
        backoff_seconds: int
            The initial delay (in seconds) between retries.
        exponential_backoff: bool
            Whether to use exponential backoff for the delay between retries.
        max_backoff: int
            The maximum backoff time (in seconds) when using exponential backoff.
        retry_exceptions: Tuple[Type[Exception]]
            Tuple of exception types that should trigger a retry.
        jitter: bool
            Whether to add a small random delay to the backoff time.
        callback: Optional[Callable[[Exception, int], None]]
            A callback function to be called after each failure. It receives the exception and the attempt number.

    Returns:
        Callable: Wrapped function with retry logic.
    """

    def decorator_retry(func: Callable) -> Callable:
        @wraps(func)
        def wrapper_retry(*args, **kwargs):
            tries = 0
            this_delay = backoff_seconds

            while tries < max_tries:
                try:
                    return func(*args, **kwargs)
                except retry_exceptions as e:
                    tries += 1

                    if callback:
                        callback(e, tries)

                    if tries == max_tries:
                        logger.error(
                            f"`{func.__name__}` failed with `{type(e).__name__}` after {tries} attempts. Aborting."
                        )
                        raise e

                    logger.warning(
                        f"`{func.__name__}` failed with `{type(e).__name__}` error, attempt {tries} of {max_tries}. Retrying in {this_delay} seconds."
                    )
                    time.sleep(this_delay)

                    if exponential_backoff:
                        this_delay = min(this_delay * 2, max_backoff)
                    if jitter:
                        this_delay += random.uniform(0, 0.5)

        return wrapper_retry

    return decorator_retry


def timed(func: Callable) -> Callable:
    """
    Decorator to measure and report the execution time of a function.

    Parameters:
        func: Callable
            The function to be wrapped and timed.

    Returns:
        Callable: Wrapped function with timing logic.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        # Record the start time using a monotonic clock to avoid issues with system clock changes
        start_time = time.monotonic_ns()

        # Execute the function and store its result
        result = func(*args, **kwargs)

        # Record the end time
        end_time = time.monotonic_ns()

        # Calculate and print the elapsed time in seconds
        print(f"Function {func.__name__} took {(end_time - start_time) / 1e9} seconds to run.")

        return result

    return wrapper


def log(func: Callable) -> Callable:
    """
    Decorator to log when a function starts and finishes its execution.

    Parameters:
        func: Callable
            The function to be wrapped and logged.

    Returns:
        Callable: Wrapped function with logging logic.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        # Generate a unique marker to correlate start and finish logs
        unique_marker = str(uuid.uuid4())

        # Log the start time and function name
        print(f"{datetime.datetime.now()} - Executing {func.__name__} - {unique_marker}")

        # Execute the function and store its result
        result = func(*args, **kwargs)

        # Log the end time and function name
        print(f"{datetime.datetime.now()} - Finished executing {func.__name__} - {unique_marker}")

        return result

    return wrapper


def repeat(number_of_times: int = None, capture_results: bool = False) -> Callable:
    """
    Decorator to repeat the execution of a function a given number of times.

    Parameters:
        number_of_times: int, optional
            The number of times to repeat the function execution.
            Must be a positive integer.
        capture_results: bool, optional
            Whether to capture and return the results of each function execution.

    Returns:
        Callable: Wrapped function with repetition logic.

    Raises:
        ValueError: If 'number_of_times' is not a positive integer.
    """

    if not isinstance(number_of_times, int) or number_of_times < 0:
        raise ValueError("@repeat requires 'number_of_times' to be set to a positive integer")

    def decorate(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Union[None, List[Any]]:
            # Initialize an empty list to store function results if required
            results = []

            # Execute the function the specified number of times
            for _ in range(number_of_times):
                result = func(*args, **kwargs)

                # Append the result to the list if 'capture_results' is True
                if capture_results:
                    results.append(result)

            # Return the results list if 'capture_results' is True, else return None
            if capture_results:
                return results
            return None

        return wrapper

    return decorate


def counter(func: Callable) -> Callable:
    """
    Decorator to add an execution counter and timer to a function.

    Parameters:
        func: Callable
            The function to be wrapped with counter and timer.

    Returns:
        Callable: Wrapped function with counter and timer logic.
    """

    def report(self: Callable) -> str:
        """
        Generate and return a summary report of execution statistics.

        Parameters:
            self: Callable
                The function to report statistics on.

        Returns:
            str: The formatted report string.
        """
        stats = f"\nExecution Statistics for `{self.__name__}`\n  Count   : {self.count}\n"  # type:ignore
        if self.count > 0:  # type:ignore
            stats += f"  Average : {sum(self._run_times) / self.count} seconds\n"  # type:ignore
            stats += f"  Slowest : {min(self._run_times)} seconds\n"  # type:ignore
            stats += f"  Fastest : {max(self._run_times)} seconds\n"  # type:ignore
        return stats

    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        # Increment the counter for function execution
        wrapper.count += 1  # type:ignore

        # Record the starting time
        start_time = time.monotonic()

        # Execute the original function and store its result
        result = func(*args, **kwargs)

        # Record and store the elapsed time
        wrapper._run_times.append(time.monotonic() - start_time)  # type:ignore

        return result

    # Initialize counter and run_times attributes
    wrapper.count = 0  # type:ignore
    wrapper._run_times = []  # type:ignore

    # Attach the reporting function
    wrapper.stats = lambda: report(wrapper)  # type:ignore

    return wrapper


def throttle(calls_per_second: float) -> Callable:
    """
    Decorator to throttle the number of calls to a function per second.

    Parameters:
        calls_per_second: float
            The maximum number of allowed function calls per second.

    Returns:
        Callable: Wrapped function with rate-limiting logic.
    """

    # Validate the input rate
    if calls_per_second <= 0:
        raise ValueError("@throttle requires calls_per_second to be greater than zero")

    # Calculate the minimum interval between successive calls
    min_interval = 1.0 / float(calls_per_second)

    def decorate(func: Callable) -> Callable:
        """
        Internal decorator function.

        Parameters:
            func: Callable
                The function to be rate-limited.

        Returns:
            Callable: Wrapped function with rate-limiting logic.
        """

        last_time_called = 0.0

        @wraps(func)
        def rate_limited_function(*args, **kwargs) -> Any:
            """
            Wrapper function enforcing the rate-limiting.

            Parameters:
                *args: Any
                    Positional arguments passed to the wrapped function.
                **kwargs: Any
                    Keyword arguments passed to the wrapped function.

            Returns:
                Any: Result from the wrapped function.
            """

            nonlocal last_time_called

            # Calculate the elapsed and remaining time since the last call
            elapsed = time.monotonic() - last_time_called
            left_to_wait = min_interval - elapsed

            # Wait if the rate limit would be exceeded
            if left_to_wait > 0:
                time.sleep(left_to_wait)

            # Call the original function and store its result
            ret = func(*args, **kwargs)

            # Update the time of the last call
            last_time_called = time.monotonic()

            return ret

        return rate_limited_function

    return decorate


def monitor(time_limit: int = 10, interval: int = 1) -> Callable:
    """
    Monitors and reports CPU and memory usage of a function execution.

    Parameters:
        time_limit: int
            Time limit for monitoring CPU and memory usage, in seconds.
        interval: int
            Interval between consecutive readings, in seconds.

    Returns:
        Callable: Wrapped function with monitoring logic.
    """

    try:
        import psutil
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error

    def decorator(func: Callable) -> Callable:
        stop_flag = False

        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            nonlocal stop_flag
            stop_flag = False

            # Use a thread to monitor the resource usage
            def _monitor():
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
                print(f"Peak memory usage: {peak_memory / 1024 / 1024:.2f} MB")

            monitor_thread = threading.Thread(target=_monitor)
            monitor_thread.start()

            try:
                start_time = time.monotonic_ns()
                result = func(*args, **kwargs)
                end_time = time.monotonic_ns()
                print(f"Execution time: {(end_time - start_time) / 1e9:.6f} seconds")
                return result
            except Exception as e:
                print(f"Error raised: {type(e).__name__}")
                end_time = time.monotonic_ns()
                print(f"Execution time: {(end_time - start_time) / 1e9:.6f} seconds")
                raise e
            finally:
                stop_flag = True
                monitor_thread.join()

        return wrapper

    return decorator


def single_item_cache(
    func: Callable = None, *, valid_for_seconds: float = float("inf")
) -> Callable:
    """
    Single item cache decorator with optional expiration time.

    Parameters:
        func: Callable, optional
            The function to be decorated.
        valid_for_seconds: float, optional
            Number of seconds after which the cache expires.
    """
    if func is None:
        return lambda f: single_item_cache(f, valid_for_seconds=valid_for_seconds)

    cache = {"last_args": None, "last_kwargs": None, "last_result": None, "last_time": 0}

    @wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal cache
        current_time = time.time()

        if (
            cache["last_args"] == args
            and cache["last_kwargs"] == kwargs
            and current_time - cache["last_time"] <= valid_for_seconds
        ):
            return cache["last_result"]

        result = func(*args, **kwargs)
        cache["last_args"] = args
        cache["last_kwargs"] = kwargs
        cache["last_result"] = result
        cache["last_time"] = current_time

        return result

    return wrapper


def lru_cache_with_expiry(
    func: Callable = None, *, max_size: int = 5, valid_for_seconds: float = float("inf")
) -> Callable:
    """
    LRU cache decorator with optional expiration time and a fixed size.

    Parameters:
        func: Callable, optional
            The function to be decorated.
        maxsize: int, optional
            The maximum size of the cache.
        valid_for_seconds: float, optional
            Number of seconds after which the cache expires.
    """
    if func is None:
        return lambda f: lru_cache_with_expiry(
            f, max_size=max_size, valid_for_seconds=valid_for_seconds
        )

    cache: OrderedDict[Tuple[Any, ...], Tuple[float, Any]] = OrderedDict()

    @wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal cache
        current_time = time.time()
        key = (args, frozenset(kwargs.items()))

        # Remove expired items
        expired_keys = [
            k for k, (timestamp, _) in cache.items() if current_time - timestamp > valid_for_seconds
        ]
        for k in expired_keys:
            del cache[k]

        # Check if result is cached
        if key in cache:
            # Move the accessed item to the end to maintain LRU order
            cache.move_to_end(key)
            return cache[key][1]

        # Call the function and cache the result
        result = func(*args, **kwargs)
        cache[key] = (current_time, result)

        # Maintain the cache size
        if len(cache) > max_size:
            cache.popitem(last=False)  # Remove the first (least recently used) item

        return result

    return wrapper


def islice(iterator, size):
    for i in range(size):
        yield next(iterator)


class DecimalFactory(decimal.Decimal):
    """
    DecimalFactory class extending Python's built-in decimal.Decimal.
    It allows for custom precision and scale settings.
    """

    scale = 0  # Number of decimal places
    precision = 0  # Total number of significant digits

    def __call__(self, value: Union[float, int, str]):
        """
        Overridden call method to handle decimal conversions using custom scale and precision.

        Parameters:
            value: Union[float, int, str]
                The value to be converted to a custom decimal.

        Returns:
            decimal.Decimal: Customized decimal object.
        """
        context = decimal.Context(
            prec=self.precision,
            rounding=decimal.ROUND_HALF_EVEN,  # Adding proper rounding mode
        )

        if isinstance(value, str) and value.isdigit():
            value += "." + "0" * min(self.scale, 3)

        # Convert input to Decimal
        decimal_value = context.create_decimal(value)

        # Define the quantization factor safely
        # Limit scale to avoid InvalidOperation errors
        safe_scale = min(self.scale, 28)  # Python's decimal has max ~28 digits precision
        factor = decimal.Decimal("10") ** -safe_scale

        # Perform quantization with proper error handling
        try:
            quantized_value = decimal_value.quantize(factor, context=context)
            return quantized_value
        except decimal.InvalidOperation:
            # Fallback if quantization fails
            return decimal_value

    def __str__(self):
        """
        Overridden str method to provide a human-readable string representation.

        Returns:
            str: Description of the DecimalFactory object.
        """
        return f"Decimal({self.scale},{self.precision})"

    @classmethod
    def new_factory(cls, precision: int, scale: int):
        """
        Class method to create a new instance of DecimalFactory with custom precision and scale.

        Parameters:
            precision: int
                The total number of significant digits for the decimal.
            scale: int
                The number of digits after the decimal point.

        Returns:
            DecimalFactory: A new DecimalFactory object with custom precision and scale.
        """
        factory = DecimalFactory.__new__(cls)  # Create a new instance
        factory.scale = scale  # Set the scale
        factory.precision = precision  # Set the precision
        return factory


def arrow_type_map(parquet_type) -> Union[Type, None]:
    """
    Maps PyArrow types to corresponding Python types.

    Parameters:
        parquet_type: lib.DataType
            PyArrow DataType object.

    Returns:
        Type or None: Corresponding Python type for the PyArrow DataType or None if not recognized.

    Raises:
        ValueError: If the PyArrow DataType is not recognized.
    """

    try:
        import pyarrow.lib as lib
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error

    type_map = {
        lib.Type_NA: None,
        lib.Type_BOOL: bool,
        lib.Type_INT8: int,
        lib.Type_INT16: int,
        lib.Type_INT32: int,
        lib.Type_INT64: int,
        lib.Type_UINT8: int,
        lib.Type_UINT16: int,
        lib.Type_UINT32: int,
        lib.Type_UINT64: int,
        lib.Type_HALF_FLOAT: float,
        lib.Type_FLOAT: float,
        lib.Type_DOUBLE: float,
        lib.Type_STRING: str,
        lib.Type_LARGE_STRING: str,
        lib.Type_DATE32: datetime.date,
        lib.Type_DATE64: datetime.datetime,
        lib.Type_TIME32: datetime.time,
        lib.Type_TIME64: datetime.time,
        lib.Type_INTERVAL_MONTH_DAY_NANO: datetime.timedelta,
        lib.Type_DURATION: datetime.timedelta,
        lib.Type_LIST: list,
        lib.Type_LARGE_LIST: list,
        lib.Type_FIXED_SIZE_LIST: list,
        lib.Type_STRUCT: dict,
        lib.Type_MAP: dict,
        lib.Type_BINARY: bytes,
        lib.Type_LARGE_BINARY: bytes,
    }

    if parquet_type.id in type_map:
        return type_map[parquet_type.id]
    elif parquet_type.id in {lib.Type_DECIMAL128, lib.Type_DECIMAL256}:
        return DecimalFactory.new_factory(parquet_type.precision, parquet_type.scale)
    elif parquet_type.id == 18:  # not sure what 18 maps to
        return datetime.datetime

    raise ValueError(f"Unable to map parquet type {parquet_type} ({parquet_type.id})")


def random_int() -> int:
    """
    Selects a random 32-bit integer using the getrandbits method.

    Returns:
        int: Random 32-bit integer.
    """
    return getrandbits(32)


def random_string(width: int = 16) -> str:
    """
    Generates a random hexadecimal string of a specified width.

    Parameters:
        width: int, optional
            Length of the output string. Default is 16.

    Returns:
        str: Random hexadecimal string of the given length.

    Note:
        This function generates a random hex string by first converting the
        desired width into the number of random bits needed, and then
        formatting it as a hexadecimal string.
    """
    num_chars = ((width + 1) >> 1) << 3  # Convert length to number of bits
    rand_bytes = getrandbits(num_chars)  # Generate random bits
    # Convert to hex string, clip '0x' prefix, and zero-fill as needed
    return ("000000" + hex(rand_bytes)[2:])[-width:]


def parse_iso(value):
    # Date validation at speed is hard, dateutil is great but really slow, this is fast
    # but error-prone. It assumes it is a date or it really nothing like a date.
    # Making that assumption - and accepting the consequences - we can convert up to
    # three times faster than dateutil.
    #
    # valid formats (not exhaustive):
    #
    #   YYYY-MM-DD                 <- date
    #   YYYY-MM-DD HH:MM           <- date and time, no seconds
    #   YYYY-MM-DDTHH:MM           <- date and time, T separator
    #   YYYY-MM-DD HH:MM:SS        <- date and time with seconds
    #   YYYY-MM-DD HH:MM:SS.mmmm   <- date and time with milliseconds
    #

    # If the last character is a Z, we ignore it.
    # If we can't parse as a date we return None rather than error
    try:
        input_type = type(value)
        if input_type == str and value.isdigit():
            value = int(value)
            input_type = int

        if input_type == numpy.datetime64:
            # this can create dates rather than datetimes, so don't return yet
            value = value.astype(datetime.datetime)
            input_type = type(value)
            if input_type is int:
                value /= 1000000000

        if input_type in (int, numpy.int64, float, numpy.float64):
            return datetime.datetime.fromtimestamp(int(value), tz=datetime.timezone.utc).replace(
                tzinfo=None
            )

        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()

        if input_type == datetime.datetime:
            return value.replace(microsecond=0)
        if input_type == datetime.date:
            return datetime.datetime.combine(value, datetime.time.min)

        # if we're here, we're doing string parsing
        if input_type == str and 10 <= len(value) <= 33:
            if value[-1] == "Z":
                value = value[:-1]
            if "+" in value:
                value = value.split("+")[0]
                if not 10 <= len(value) <= 28:
                    return None
            val_len = len(value)
            if value[4] != "-" or value[7] != "-":
                return None
            if val_len == 10:
                # YYYY-MM-DD
                return datetime.datetime(
                    *map(int, [value[:4], value[5:7], value[8:10]])  # type:ignore
                )
            if val_len >= 16:
                if value[10] not in ("T", " ") and value[13] != ":":
                    return None
                if val_len >= 19 and value[16] == ":":
                    # YYYY-MM-DD HH:MM:SS
                    return datetime.datetime(
                        *map(  # type:ignore
                            int,
                            [
                                value[:4],  # YYYY
                                value[5:7],  # MM
                                value[8:10],  # DD
                                value[11:13],  # HH
                                value[14:16],  # MM
                                value[17:19],  # SS
                            ],
                        )
                    )
                if val_len == 16:
                    # YYYY-MM-DD HH:MM
                    return datetime.datetime(
                        *map(  # type:ignore
                            int,
                            [
                                value[:4],
                                value[5:7],
                                value[8:10],
                                value[11:13],
                                value[14:16],
                            ],
                        )
                    )
        return None
    except (ValueError, TypeError):
        return None
