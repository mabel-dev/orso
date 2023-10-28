import datetime
import random

START_RANGE = datetime.datetime(1960, 1, 1)
END_RANGE = datetime.datetime(2100, 12, 31)


def generate_random_datetime(
    start: datetime.datetime = START_RANGE, end: datetime.datetime = END_RANGE
) -> datetime.datetime:
    """
    Generate a random datetime between two datetime objects.

    Parameters:
        start: datetime
            The start datetime.
        end: datetime
            The end datetime.

    Returns:
        datetime: A random datetime between the start and end.
    """
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + datetime.timedelta(seconds=random_seconds)
