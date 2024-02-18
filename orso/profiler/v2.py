import os
import sys
from copy import deepcopy
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import numpy

from orso.profiler.distogram import Distogram
from orso.profiler.distogram import load
from orso.profiler.distogram import merge
from orso.schema import FlatColumn
from orso.types import OrsoTypes

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


MOST_FREQUENT_VALUE_SIZE: int = 32
INFINITY = float("inf")
SIXTY_FOUR_BITS: int = 8
SIXTY_FOUR_BYTES: int = 64


def string_to_int64(value: str) -> int:
    """Convert the first 8 characters of a string to an integer representation.

    Parameters:
        value: str
            The string value to be converted.

    Returns:
        An integer representation of the first 8 characters of the string.
    """
    byte_value = value.ljust(SIXTY_FOUR_BITS)[:SIXTY_FOUR_BITS].encode("utf-8")
    int_value = int.from_bytes(byte_value, "big")
    return min(int_value, 9223372036854775807)


def int64_to_string(value: int) -> str:
    # Convert the integer back to 8 bytes using big-endian byte order
    if value >= 9223372036854775807:
        return None

    byte_value = value.to_bytes(SIXTY_FOUR_BITS, "big")

    # Decode the byte array back to a UTF-8 string
    # You might need to strip any padding characters that were added when encoding
    string_value = byte_value.decode("utf-8").rstrip("\x00")

    return string_value


def find_mfvs(data, top_n=MOST_FREQUENT_VALUE_SIZE):
    """
    Find the top N most frequent values (MFVs) in a NumPy array along with their counts.

    Parameters:
        data (np.ndarray): The input NumPy array containing numerical data.
        top_n (int): The number of top MFVs to return. Default is 32.

    Returns:
        top_values (np.ndarray): The top N most frequent values in the data.
        top_counts (np.ndarray): The counts of the top N most frequent values.
    """
    from collections import Counter

    counter = Counter(data)

    # Most common returns tuples of (value, count), so separate them
    top_items = counter.most_common(top_n)
    top_values, top_counts = zip(*top_items) if top_items else ([], [])

    return top_values, top_counts


def _to_unix_epoch(date):
    if date is None:
        return None
    # Not all platforms can handle negative timestamp()
    # https://bugs.python.org/issue29097
    return date.astype("datetime64[s]").astype("int64")


@dataclass
class ColumnProfile:
    name: str
    type: OrsoTypes
    count: int = 0
    missing: int = 0
    maximum: Optional[int] = None
    minimum: Optional[int] = None
    most_frequent_values: List[str] = field(default_factory=list)
    most_frequent_counts: List[int] = field(default_factory=list)
    bins: List[Tuple] = field(default_factory=list)

    def deep_copy(self):
        """Create a deep copy of the Profile instance."""
        return deepcopy(self)

    def __add__(self, profile: "ColumnProfile") -> "ColumnProfile":

        new_profile = self.deep_copy()
        new_profile.count += profile.count
        new_profile.missing += profile.missing
        new_profile.minimum = min([self.minimum or INFINITY, profile.minimum or INFINITY])
        if new_profile.minimum == INFINITY:
            new_profile.minimum = None
        new_profile.maximum = max([self.maximum or -INFINITY, profile.maximum or -INFINITY])
        if new_profile.maximum == -INFINITY:
            new_profile.maximum = None

        if self.most_frequent_values and profile.most_frequent_values:
            morsel1_map = dict(zip(self.most_frequent_values, self.most_frequent_counts))
            morsel2_map = dict(zip(profile.most_frequent_values, profile.most_frequent_counts))

            combined_map = {}
            for value in morsel1_map:
                if value in morsel2_map:  # Ensure the value is present in both morsels
                    combined_map[value] = morsel1_map[value] + morsel2_map[value]

            new_profile.most_frequent_values = list(combined_map.keys())
            new_profile.most_frequent_counts = list(combined_map.values())
        else:
            new_profile.most_frequent_values = []
            new_profile.most_frequent_counts = []

        if self.bins and profile.bins:
            my_dgram = load(self.bins, self.minimum, self.maximum)
            profile_dgram = load(profile.bins, profile.minimum, profile.maximum)
            if len(profile.bins) > len(self.bins):
                new_profile.bins = merge(profile_dgram, my_dgram).bins
            else:
                new_profile.bins = merge(my_dgram, profile_dgram).bins
        else:
            new_profile.bins = []

        return new_profile


class BaseProfiler:
    def __init__(self, column: FlatColumn):
        self.column = column
        self.profile = ColumnProfile(name=column.name, type=column.type)

    def __call__(self, column_data: List[Any]):
        raise NotImplementedError("Must be implemented by subclass.")


class ListStructProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):

        self.profile.count = len(column_data)
        self.profile.missing = sum(1 for val in column_data if val is None)


class DefaultProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):

        self.profile.count = len(column_data)
        self.profile.missing = sum(1 for val in column_data if val != val)


class BooleanProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):
        self.profile.count = len(column_data)

        column_data = [col for col in column_data if col is not None]
        self.profile.missing = self.profile.count - len(column_data)

        self.profile.most_frequent_values = ["True", "False"]
        self.profile.most_frequent_counts = [column_data.count(True), column_data.count(False)]


class NumericProfiler(BaseProfiler):

    def __call__(self, column_data: List[Any]):

        self.profile.count = len(column_data)
        column_data = numpy.array(column_data, copy=False)  # Ensure column_data is a NumPy array
        column_data = column_data[~numpy.isnan(column_data)]
        self.profile.missing = self.profile.count - len(column_data)
        # Compute min and max only if necessary
        if len(column_data) > 0:
            self.profile.minimum = int(numpy.min(column_data))
            self.profile.maximum = int(numpy.max(column_data))

            mf_values, mf_counts = find_mfvs(column_data, MOST_FREQUENT_VALUE_SIZE)
            self.profile.most_frequent_values = [f"{n:f}".rstrip("0").strip(".") for n in mf_values]
            self.profile.most_frequent_counts = mf_counts

            dgram = Distogram()
            dgram.bulkload(column_data)
            self.profile.bins = dgram.bins


class VarcharProfiler(BaseProfiler):

    def __call__(self, column_data: List[Any]):

        self.profile.count = len(column_data)
        column_data = [col[:SIXTY_FOUR_BYTES] for col in column_data if col is not None]
        self.profile.missing = self.profile.count - len(column_data)
        self.profile.minimum = string_to_int64(min(column_data))
        self.profile.maximum = string_to_int64(max(column_data))

        mf_values, mf_counts = find_mfvs(column_data, MOST_FREQUENT_VALUE_SIZE)
        self.profile.most_frequent_values = mf_values
        self.profile.most_frequent_counts = mf_counts


class DateProfiler(BaseProfiler):

    def __call__(self, column_data: List[Any]):

        self.profile.count = len(column_data)
        column_data = [col for col in column_data if col == col]
        self.profile.missing = self.profile.count - len(column_data)
        column_data = [_to_unix_epoch(i) for i in column_data]

        numeric_profiler = NumericProfiler(self.column)
        numeric_profiler(column_data)
        numeric_profile = numeric_profiler.profile

        self.profile.minimum = numeric_profile.minimum
        self.profile.maximum = numeric_profile.maximum
        self.profile.most_frequent_values = numeric_profile.most_frequent_values
        self.profile.most_frequent_counts = numeric_profile.most_frequent_counts
        self.profile.bins = numeric_profile.bins


def table_profiler(dataframe) -> List[Dict[str, Any]]:
    profiler_classes = {
        OrsoTypes.VARCHAR: VarcharProfiler,
        OrsoTypes.INTEGER: NumericProfiler,
        OrsoTypes.DOUBLE: NumericProfiler,
        OrsoTypes.DECIMAL: NumericProfiler,
        OrsoTypes.ARRAY: ListStructProfiler,
        OrsoTypes.STRUCT: ListStructProfiler,
        OrsoTypes.BOOLEAN: BooleanProfiler,
        OrsoTypes.DATE: DateProfiler,
        OrsoTypes.TIMESTAMP: DateProfiler,
    }

    profiles = {}

    for morsel in dataframe.to_batches(25000):
        for column in morsel.schema.columns:
            column_data = morsel.collect(column.name)
            if not column_data:
                continue

            profiler_class = profiler_classes.get(column.type, DefaultProfiler)
            profiler = profiler_class(column)
            profiler(column_data=column_data)
            if column.name in profiles:
                profiles[column.name] += profiler.profile
            else:
                profiles[column.name] = profiler.profile

    return [asdict(v) for v in profiles.values()]


if __name__ == "__main__":

    import os
    import sys
    import time

    import opteryx

    import orso

    df = opteryx.query("SELECT * FROM 'scratch/tweets.arrow'")
    print(df)
    t = time.monotonic_ns()
    pr = table_profiler(df)
    print((time.monotonic_ns() - t) / 1e9)
    print(orso.DataFrame(pr))
    print(orso.DataFrame(pr + pr))

    quit()

    import cProfile
    import pstats

    with cProfile.Profile(subcalls=False) as pr:

        prr = df.arrow()

        # stats = pstats.Stats(pr).strip_dirs().sort_stats("tottime")
        stats = pstats.Stats(pr).sort_stats("tottime")

        func_list = [
            (k, v)
            for k, v in stats.stats.items()
            if True  # "orso" in k[0]
            and "." in k[0]
            and (not "debugging.py" in k[0] and not "brace.py" in k[0])
        ]
        sorted_funcs = sorted(
            func_list, key=lambda x: x[1][2], reverse=True
        )  # Sorted by total time

        header = ["File", "Line", "Function", "Calls", "Total (ms)", "Cumulative (ms)"]
        divider = "-" * 110
        print(divider)
        print("{:<40} {:>5} {:<20} {:>10} {:>10} {:>12}".format(*header))
        print(divider)

        limit = 10
        for func, func_stats in sorted_funcs:
            file_name, line_number, function_name = func
            total_calls, _, total_time, cumulative_time, _ = func_stats
            file_name = file_name.split("opteryx")[-1]
            file_name = "..." + file_name[-37:] if len(file_name) > 40 else file_name
            function_name = function_name[:17] + "..." if len(function_name) > 20 else function_name
            row = [
                file_name,
                line_number,
                function_name,
                total_calls,
                f"{(total_time * 1000):.6f}",
                f"{(cumulative_time * 1000):.6f}",
            ]
            print("{:<40} {:>5} {:<20} {:>10} {:>10} {:>12}".format(*row))
            limit -= 1
            if limit == 0:
                break
        print(divider)

    quit()
    import json

    from opteryx.third_party import sqloxide

    parsed_statements = sqloxide.parse_sql(SQL, dialect="mysql")
    print(json.dumps(parsed_statements))
