import datetime
import functools

import numpy
import orjson

import orso
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import PYTHON_TO_ORSO_MAP
from orso.types import OrsoTypes

MAX_STRING_SIZE: int = 64
MAX_UNIQUE_COLLECTOR: int = 8


class DataProfile(orso.DataFrame):
    def __add__(self, data_profile):
        raise NotImplementedError("cannot add profiles")

    @classmethod
    def from_dataset(cls, dataset):
        rows = table_profiler(dataset)
        schema = RelationSchema(
            name="profile",
            columns=[
                FlatColumn(name="name", type=OrsoTypes.VARCHAR),
                FlatColumn(name="type", type=OrsoTypes.VARCHAR),
                FlatColumn(name="count", type=OrsoTypes.INTEGER),
                FlatColumn(name="missing", type=OrsoTypes.INTEGER),
                FlatColumn(name="most_frequent_values", type=OrsoTypes.ARRAY),
                FlatColumn(name="most_frequent_counts", type=OrsoTypes.ARRAY),
                FlatColumn(name="numeric_range", type=OrsoTypes.ARRAY),
                FlatColumn(name="varchar_range", type=OrsoTypes.ARRAY),
                FlatColumn(name="distogram_values", type=OrsoTypes.ARRAY),
                FlatColumn(name="distogram_counts", type=OrsoTypes.ARRAY),
            ],
        )
        profile = cls(rows=rows, schema=schema)
        return profile


UNIX_EPOCH = datetime.datetime(1970, 1, 1)


def _to_unix_epoch(date):
    if date is None:
        return None
    # Not all platforms can handle negative timestamp()
    # https://bugs.python.org/issue29097
    return (date - UNIX_EPOCH).total_seconds()


def table_profiler(dataframe):
    """
    Collect summary statistics about each column
    """
    from orso.profiler.distogram import Distogram  # type:ignore

    empty_profile = orjson.dumps(
        {
            "name": None,
            "type": [],
            "count": 0,
            "missing": 0,
            "most_frequent_values": None,
            "most_frequent_counts": None,
            "numeric_range": None,
            "varchar_range": None,
            "distogram_values": None,
            "distogram_counts": None,
        }
    )

    for morsel in dataframe.to_batches(10000):
        uncollected_columns = []
        profile_collector: dict = {}

        for column in morsel.column_names:
            column_data = morsel.collect(column)
            if len(column_data) == 0:
                continue

            profile = profile_collector.get(column, orjson.loads(empty_profile))
            _type: set = functools.reduce(
                lambda a, b: a if b is None else a.union({type(b).__name__}), column_data, set()
            )  # type:ignore

            profile["type"] = list(set(profile["type"]).union(_type))
            if len(profile["type"]) > 1:
                uncollected_columns.append(column)
            if len(_type) > 0:
                _type = _type.pop()
            else:
                _type = None
            profile["count"] += len(column_data)
            profile["missing"] += sum(1 for a in column_data if a is None)

            # interim save
            profile_collector[column] = profile

            # don't collect problematic columns
            if column in uncollected_columns:
                continue

            # don't collect columns we can't analyse
            if _type in {"list", "dict", "NoneType"}:
                continue

            # long strings are meaningless
            if _type == "str":
                column_data = [v for v in column_data if v is not None]

                max_len = functools.reduce(
                    lambda x, y: max(len(y), x),
                    column_data,
                    0,
                )
                if max_len > MAX_STRING_SIZE:
                    if column not in uncollected_columns:
                        uncollected_columns.append(column)
                    continue

                # collect the range values
                if len(column_data) > 0:
                    varchar_range_min = min(column_data)
                    varchar_range_max = max(column_data)

                    if profile["varchar_range"] is not None:
                        varchar_range_min = min(varchar_range_min, profile["varchar_range"][0])
                        varchar_range_max = max(varchar_range_max, profile["varchar_range"][1])

                    profile["varchar_range"] = (
                        varchar_range_min,
                        varchar_range_max,
                    )

            # convert TIMESTAMP into a NUMERIC (seconds after Unix Epoch)
            if _type == "datetime":
                column_data = (_to_unix_epoch(i) for i in column_data)

            if _type in {"bool", "datetime", "int", "float", "str"}:
                # remove empty values
                column_data = numpy.array([i for i in column_data if i not in (None, numpy.nan)])

            if _type == "bool":
                # we can make it easier to collect booleans
                counter = profile.get("counter")
                if counter is None:
                    counter = {"True": 0, "False": 0}
                trues = sum(column_data)
                counter["True"] += trues
                counter["False"] += column_data.size - trues
                profile["counter"] = counter

            if _type == "str" and profile.get("counter") != {}:
                # counter is used to collect and count unique values
                vals, counts = numpy.unique(column_data, return_counts=True)
                counter = {}
                if len(vals) <= MAX_UNIQUE_COLLECTOR:
                    counter = dict(zip(vals, counts))
                    for k, v in profile.get("counter", {}).items():
                        counter[k] = counter.pop(k, 0) + v
                    if len(counter) > MAX_UNIQUE_COLLECTOR:
                        counter = {}
                profile["counter"] = counter

            if _type in {"int", "float", "datetime"}:
                # populate the distogram, this is used for distribution statistics
                dgram = profile.get("dgram")
                if dgram is None:
                    dgram = Distogram()  # type:ignore
                dgram.bulkload(column_data)
                profile["dgram"] = dgram

            profile_collector[column] = profile

        buffer = []

        for column, profile in profile_collector.items():
            profile["name"] = column
            profile["type"] = ", ".join(
                [PYTHON_TO_ORSO_MAP.get(t, "OTHER") for t in profile["type"]]
            )

            if column not in uncollected_columns:
                dgram = profile.pop("dgram", None)
                if dgram:
                    # force numeric types to be the same
                    profile["numeric_range"] = (
                        numpy.double(dgram.min),
                        numpy.double(dgram.max),
                    )
                    profile["distogram_values"], profile["distogram_counts"] = zip(*dgram.bins)
                    profile["distogram_values"] = list(
                        numpy.array(profile["distogram_values"], numpy.double)
                    )

                counter = profile.pop("counter", None)
                if counter:
                    counts = list(counter.values())
                    if min(counts) != max(counts):
                        profile["most_frequent_values"] = [str(k) for k in counter.keys()]
                        profile["most_frequent_counts"] = counts

            # remove collectors
            profile.pop("dgram", None)
            profile.pop("counter", None)

            buffer.append(tuple(profile.values()))

        return buffer


if __name__ == "__main__":  # pragme: no cover
    # fmt:off
    cities_list:list = [
        {"name": "Tokyo", "population": 13929286, "country": "Japan", "founded": "1457", "area": 2191, "language": "Japanese"},
        {"name": "London", "population": 8982000, "country": "United Kingdom", "founded": "43 AD", "area": 1572, "language": "English"},
        {"name": "New York City", "population": 8399000, "country": "United States", "founded": "1624", "area": 468.9, "language": "English"},
        {"name": "Mumbai", "population": 18500000, "country": "India", "founded": "7th century BC", "area": 603.4, "language": "Hindi, English"},
        {"name": "Cape Town", "population": 433688, "country": "South Africa", "founded": "1652", "area": 400, "language": "Afrikaans, English"},
        {"name": "Paris", "population": 2148000, "country": "France", "founded": "3rd century BC", "area": 105.4, "language": "French"},
        {"name": "Beijing", "population": 21710000, "country": "China", "founded": "1045", "area": 16410.54, "language": "Mandarin"},
        {"name": "Rio de Janeiro", "population": 6747815, "country": "Brazil", "founded": "1 March 1565", "area": 1264, "language": "Portuguese"}
    ]
    import orso
    cities = orso.DataFrame(cities_list)
    # fmt:on

    import os
    import sys

    sys.path.insert(1, os.path.join(sys.path[0], "../.."))

    import opteryx

    df = opteryx.query("SELECT graduate_major, len(graduate_major) FROM $astronauts")
    print(df)

    print(table_profiler(df))
