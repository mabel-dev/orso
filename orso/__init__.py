from functools import wraps

from orso.version import __version__

from .dataframe import DataFrame
from .row import Row

__all__ = ["DataFrame", "Row", "__version__"]


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
    @wraps(func)
    def wrapper(*args, **kwargs):
        import datetime

        print(f"{datetime.datetime.now()} - Executing {func.__name__}")
        result = func(*args, **kwargs)
        print(f"{datetime.datetime.now()} - Finished executing {func.__name__}")
        return result

    return wrapper
