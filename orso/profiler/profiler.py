import heapq
from copy import deepcopy
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import numpy

from orso.profiler import distogram
from orso.schema import FlatColumn
from orso.types import OrsoTypes

MOST_FREQUENT_VALUE_SIZE: int = 32
KVM_SIZE: int = 32
INFINITY = float("inf")
SIXTY_FOUR_BITS: int = 8
SIXTY_FOUR_BYTES: int = 64
DISTOGRAM_BIN_COUNT: int = 50
MAX_INT64: int = 9223372036854775807


def string_to_int64(value: str) -> int:
    """Convert the first 8 characters of a string to an integer representation.

    Parameters:
        value: str
            The string value to be converted.

    Returns:
        An integer representation of the first 8 characters of the string.
    """
    byte_value = (value + "\x00\x00\x00\x00")[:SIXTY_FOUR_BITS].encode("utf-8")
    int_value = int.from_bytes(byte_value, "big")
    return int_value if int_value <= MAX_INT64 else MAX_INT64


def int64_to_string(value: int) -> str:
    # Convert the integer back to 8 bytes using big-endian byte order
    if value >= MAX_INT64:
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


def get_kvm_hashes(data, size: int):  # slowest function
    from xxhash import xxh32

    min_hashes = []

    data = list(set(data))

    # Build a list with the hash values of the first 'size' elements or all elements if fewer.
    min_hashes = [-xxh32(str(element)).intdigest() for element in data[:size]]

    # Transform the list into a heap in-place.
    heapq.heapify(min_hashes)

    for element in data[size:]:
        hash_value = xxh32(str(element)).intdigest()

        # If the current hash is smaller than the largest in the heap
        if hash_value < -min_hashes[0]:
            heapq.heappushpop(min_hashes, -hash_value)

    # Convert back to positive values and sort before returning
    return sorted(-x for x in min_hashes)


def get_ordered_and_transitions(data) -> tuple:
    ordered = None
    transitions = 0
    last_value = data[0]
    for value in data[1:]:
        if value != last_value:
            transitions += 1
            if ordered is None:
                ordered = -1 if value < last_value else 1
            elif value > last_value and ordered == -1 or value < last_value and ordered == 1:
                ordered = 0
        last_value = value

    return (ordered, transitions)


@dataclass
class ColumnProfile:
    name: str
    type: OrsoTypes
    count: int = 0
    missing: int = 0
    maximum: Optional[int] = None
    minimum: Optional[int] = None
    order: int = None
    transitions: int = 0
    most_frequent_values: List[str] = field(default_factory=list)
    most_frequent_counts: List[int] = field(default_factory=list)
    histogram: List[Tuple] = field(default_factory=list)
    kmv_hashes: List[int] = field(default_factory=list)

    def deep_copy(self):
        """Create a deep copy of the Profile instance."""
        return deepcopy(self)

    def estimate_cardinality(self) -> int:
        """
        Estimates the cardinality (number of unique values) of the elements seen so far.
        """
        if not self.kmv_hashes:
            return 0
        # don't estimate if we know the number
        if len(self.kmv_hashes) < KVM_SIZE:
            return len(self.kmv_hashes)
        # Use the k-th smallest hash value (the last/largest in the sorted list) to estimate cardinality
        kth_min_value = self.kmv_hashes[-1]

        # Cardinality estimation formula
        return int((KVM_SIZE - 1) / (kth_min_value / 2**32))

    def estimate_values_at(self, point) -> int:
        if not hasattr(self, "distogram"):
            self.distogram = distogram.load(self.histogram, self.minimum, self.maximum)
        return distogram.bin_size(self.distogram, point)

    def estimate_values_below(self, point) -> int:
        if not hasattr(self, "distogram"):
            self.distogram = distogram.load(self.histogram, self.minimum, self.maximum)
        return distogram.count_at(self.distogram, point)

    def estimate_values_above(self, point) -> int:
        if not hasattr(self, "distogram"):
            self.distogram = distogram.load(self.histogram, self.minimum, self.maximum)
        return (self.count - self.missing) - distogram.count_at(self.distogram, point)

    def __add__(self, profile: "ColumnProfile") -> "ColumnProfile":
        new_profile = self.deep_copy()
        new_profile.count += profile.count
        new_profile.missing += profile.missing
        new_profile.transitions += profile.transitions + 1
        new_profile.order = 0 if new_profile.order == profile.order else new_profile.order
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

            new_profile.most_frequent_values = (
                [] if len(combined_map) == 0 else list(combined_map.keys())
            )
            new_profile.most_frequent_counts = (
                [] if len(combined_map) == 0 else list(combined_map.values())
            )
        else:
            new_profile.most_frequent_values = []
            new_profile.most_frequent_counts = []

        if self.histogram and profile.histogram:
            my_dgram = distogram.load(self.histogram, self.minimum, self.maximum)
            profile_dgram = distogram.load(profile.histogram, profile.minimum, profile.maximum)
            if len(profile.histogram) > len(self.histogram):
                new_profile.histogram = distogram.merge(profile_dgram, my_dgram).bins
            else:
                new_profile.histogram = distogram.merge(my_dgram, profile_dgram).bins
        else:
            new_profile.histogram = []

        if self.kmv_hashes and profile.kmv_hashes:
            new_profile.kmv_hashes = sorted(set(self.kmv_hashes + profile.kmv_hashes))[:KVM_SIZE]

        return new_profile


class TableProfile:
    def __init__(self):
        self._columns: List[ColumnProfile] = []
        self._column_names: List[str] = []

    def __add__(self, right_profile: "TableProfile") -> "TableProfile":
        new_profile = TableProfile()

        for column_name in self._column_names:
            left_column = self.column(column_name)
            right_column = right_profile.column(column_name)
            if not right_column:
                right_column = ColumnProfile(
                    column_name, left_column.type, left_column.count, left_column.count
                )
            new_profile.add_column(left_column + right_column, column_name)

        return new_profile

    def add_column(self, profile: ColumnProfile, name: str):
        self._columns.append(profile)
        self._column_names.append(name)

    def __iter__(self):
        """An iterator over columns"""
        return iter(self.column)

    def column(self, i: Union[int, str]) -> Union[ColumnProfile, None]:
        """Get a column by its name or index"""
        if isinstance(i, str):
            for name, column in zip(self._column_names, self._columns):
                if name == i:
                    return column
            return None

        if isinstance(i, int):
            return self._columns[i]

    def to_dicts(self) -> List[dict]:
        return [asdict(v) for v in self._columns]

    def to_arrow(self) -> "pyarrow.Table":
        import pyarrow

        return pyarrow.Table.from_pylist(self.to_dicts())

    def to_dataframe(self) -> "DataFrame":
        import orso

        return orso.DataFrame(self.to_dicts())

    @classmethod
    def from_dataframe(cls, table) -> "TableProfile":
        from orso.schema import FlatColumn
        from orso.schema import RelationSchema

        profile = cls()

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

        for morsel in table.to_batches(25000):
            if not isinstance(morsel.schema, RelationSchema):
                morsel._schema = RelationSchema(
                    name="morsel", columns=[FlatColumn(name=c) for c in morsel.schema]
                )

            for column in morsel.schema.columns:
                column_data = morsel.collect(column.name)
                if len(column_data) == 0:
                    continue

                profiler_class = profiler_classes.get(column.type, DefaultProfiler)
                profiler = profiler_class(column)
                profiler(column_data=column_data)
                if column.name in profiles:
                    profiles[column.name] += profiler.profile
                else:
                    profiles[column.name] = profiler.profile

        for name, summary in profiles.items():
            profile.add_column(summary, name)

        return profile


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

        if len(column_data) > 0:
            self.profile.most_frequent_values = ["True", "False"]
            self.profile.most_frequent_counts = [column_data.count(True), column_data.count(False)]


class NumericProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):
        self.profile.count = len(column_data)
        column_data = numpy.array(column_data, copy=False)  # Ensure column_data is a NumPy array
        if column_data.dtype.name == "object":
            column_data = column_data[~numpy.equal(column_data, -9223372036854775808)]
            column_data = [float(c) for c in column_data if c is not None]
        else:
            column_data = column_data[~numpy.isnan(column_data)]
        self.profile.missing = self.profile.count - len(column_data)
        # Compute min and max only if necessary
        if len(column_data) > 0:
            self.profile.minimum = int(numpy.min(column_data))
            self.profile.maximum = int(numpy.max(column_data))

            mf_values, mf_counts = find_mfvs(column_data, MOST_FREQUENT_VALUE_SIZE)
            self.profile.most_frequent_values = [f"{n:f}".rstrip("0").strip(".") for n in mf_values]
            self.profile.most_frequent_counts = mf_counts

            # Create a histogram of the data
            hist_counts, bin_edges = numpy.histogram(column_data, bins=DISTOGRAM_BIN_COUNT)
            self.profile.histogram = [
                (left_edge, count)
                for count, left_edge in zip(hist_counts, bin_edges[:-1])
                if count > 0
            ]

            # K-minimum value hashes, used for cardinality estimation
            self.profile.kmv_hashes = get_kvm_hashes(column_data, KVM_SIZE)
            self.profile.order, self.profile.transitions = get_ordered_and_transitions(column_data)


class VarcharProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):
        self.profile.count = len(column_data)
        column_data = [col for col in column_data if col is not None]
        if len(column_data) > 0:
            # K-minimum value hashes, used for cardinality estimation
            self.profile.kmv_hashes = get_kvm_hashes(column_data, KVM_SIZE)
            column_data = [col[:SIXTY_FOUR_BYTES] for col in column_data]
            self.profile.missing = self.profile.count - len(column_data)
            self.profile.minimum = string_to_int64(min(column_data))
            self.profile.maximum = string_to_int64(max(column_data))

            mf_values, mf_counts = find_mfvs(column_data, MOST_FREQUENT_VALUE_SIZE)
            self.profile.most_frequent_values = mf_values
            self.profile.most_frequent_counts = mf_counts
            self.profile.order, self.profile.transitions = get_ordered_and_transitions(column_data)


class DateProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):
        self.profile.count = len(column_data)
        if hasattr(column_data[0], "value"):
            column_data = numpy.array(
                [v.value for v in column_data if v is not None], dtype="int64"
            )
        else:
            column_data = numpy.array(column_data, dtype="datetime64[s]").astype("int64")
        column_data = column_data[~numpy.equal(column_data, -9223372036854775808)]
        self.profile.missing = self.profile.count - len(column_data)
        if len(column_data) > 0:
            numeric_profiler = NumericProfiler(self.column)
            numeric_profiler(column_data)
            numeric_profile = numeric_profiler.profile

            self.profile.minimum = numeric_profile.minimum
            self.profile.maximum = numeric_profile.maximum
            self.profile.most_frequent_values = numeric_profile.most_frequent_values
            self.profile.most_frequent_counts = numeric_profile.most_frequent_counts
            self.profile.histogram = numeric_profile.histogram
            self.profile.kmv_hashes = numeric_profile.kmv_hashes


def table_profiler(dataframe) -> List[Dict[str, Any]]:
    return TableProfile.from_dataframe(dataframe)
