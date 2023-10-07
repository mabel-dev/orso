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

import array
import decimal
from collections import defaultdict
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union


# Aggregation Functions
def min_agg(values: List[Any]) -> Any:
    return min(values)


def max_agg(values: List[Any]) -> Any:
    return max(values)


def count_agg(values: List[Any]) -> int:
    return len(values)


def avg_agg(values: List[decimal.Decimal]) -> decimal.Decimal:
    return decimal.Decimal(sum(values)) / decimal.Decimal(len(values))


def sum_agg(values: List[decimal.Decimal]) -> decimal.Decimal:
    return sum(values)


AGGREGATORS = {"MIN": min_agg, "MAX": max_agg, "COUNT": count_agg, "AVG": avg_agg, "SUM": sum_agg}


class TooManyGroups(Exception):
    pass


class GroupBy:
    """
    GroupBy does a lazy evaluation of the groups, the groups are calculated as part of
    calculating the aggregations. This was implemented like this so that generators
    can be aggregated - we have one opportunity to cycle of the records, and if the
    data is in a generator, there's a chance the dataset doesn't fit in memory.
    """

    def __init__(self, dictset, columns):
        self._dictset = dictset
        if isinstance(columns, (list, set, tuple)):
            self._columns = tuple(columns)
        else:
            self._columns = [columns]
        self._group_keys = {}

    def _map(self, collect_columns: Union[str, List[str]]) -> Dict[int, Dict[str, List[Any]]]:
        """
        Maps the dataset into groups based on given columns.

        Parameters:
            collect_columns: Union[str, List[str]]
                Columns used for aggregation.

        Returns:
            A dictionary containing groups and aggregated values.
        """
        collect_columns = (
            collect_columns if isinstance(collect_columns, list) else [collect_columns]
        )
        source_columns = self._dictset.column_names
        collect_column_indicies = [
            source_columns.index(target) if target in source_columns else -1
            for target in collect_columns
        ]
        group_column_indicies = array.array(
            "i",
            (source_columns.index(target) for target in self._columns),
        )

        for record in self._dictset:
            # Create a unique hash for each group
            group_key = hash(tuple(record[col] for col in group_column_indicies))

            if group_key not in self._group_keys:
                self._group_keys[group_key] = [
                    (source_columns[column], record[column]) for column in group_column_indicies
                ]

            for i, column in enumerate(collect_column_indicies):
                yield (group_key, collect_columns[i], "*" if column == -1 else record[column])

    def aggregate(self, aggregations: List[Tuple[str, Callable]]) -> "DataFrame":
        """
        Aggregates the grouped data based on the given aggregation functions.

        Parameters:
            aggregations: List[Tuple[str, Callable]]
                A list of tuples containing column name and aggregation function.

        Returns:
            A dictionary containing groups and aggregated values.
        """
        aggregated_data = {}
        column_value_map = defaultdict(lambda: defaultdict(list))

        if not isinstance(aggregations, list):
            aggregations = [aggregations]
        if not all(isinstance(agg, tuple) for agg in aggregations):  # pragma: no cover
            raise ValueError("`aggregate` expects a list of Tuples")

        # Collecting the values for each group and column
        for group_key, column, value in self._map([col for _, col in aggregations]):
            if value is not None:
                column_value_map[group_key][column].append(value)

        # Applying aggregation functions
        for group, column_values in column_value_map.items():
            aggregated_data[group] = {}
            for func, col in aggregations:
                aggregated_data[group][f"{func}({col})"] = AGGREGATORS[func](
                    column_values.get(col, [])
                )

        result_set = []
        for group, values in aggregated_data.items():
            results = {f"{func}({col})": values.get(f"{func}({col})") for func, col in aggregations}
            keys = self._group_keys[group]
            for key in keys:
                results[key[0]] = key[1]
            result_set.append(results)

        from orso.dataframe import DataFrame

        return DataFrame(result_set)

    def max(self, columns) -> "DataFrame":
        """
        Get the maximum value of a column, or set of columns, in each group.

        Parameters:
            columns: string or iterable
                The columns to collect the maximum value of for each group.

        Yields:
            Dictionary
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("MAX", column) for column in columns])

    def min(self, columns) -> "DataFrame":
        """
        Get the minimum value of a column, or set of columns, in each group.

        Parameters:
            columns: string or iterable
                The columns to collect the minimum value of for each group.

        Yields:
            Dictionary
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("MIN", column) for column in columns])

    def sum(self, columns) -> "DataFrame":
        """
        Get the sum of values in a column, or set of columns, in each group.

        Parameters:
            columns: string or iterable
                The columns to calculate the sum of for each group.

        Yields:
            Dictionary
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("SUM", column) for column in columns])

    def count(self) -> "DataFrame":
        """
        Count the number of items in each group.

        Yields:
            Dictionary
        """
        # COUNT is a little different, it doesn't have any fields to perform the
        # aggregation on.
        # This implementation could be improved by taking a copy of the
        # aggregate() function and removing the bits that aren't needed to just
        # count the values.
        return self.aggregate(
            (
                "COUNT",
                "*",
            )
        )

    def avg(self, columns) -> "DataFrame":
        """
        Calculate the average of the items in a group.
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("AVG", column) for column in columns])

    def groups(self) -> "DataFrame":
        """
        Return the set of groups - this is similar to a DISTINCT function
        """
        collector = defaultdict(dict)
        for record in self._map("*"):
            collector[record[0]] = 1

        from orso.dataframe import DataFrame

        return DataFrame(dict(self._group_keys[group]) for group in self._group_keys)
