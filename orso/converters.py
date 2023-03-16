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

import itertools
import typing

from orso import _tools
from orso.dataframe import DataFrame
from orso.exceptions import MissingDependencyError
from orso.row import Row


def to_arrow(dataset, size=None):
    try:
        import pyarrow
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error

    # Create a list of PyArrow arrays from the rows
    rows = dataset._rows
    arrays = [
        pyarrow.array(col) for col in zip(*(rows if size is None else itertools.islice(rows, size)))
    ]

    # Create a PyArrow table from the arrays and schema
    if arrays:
        table = pyarrow.Table.from_arrays(arrays, dataset.column_names)
    else:
        table = pyarrow.Table.from_arrays([[]] * len(dataset.column_names), dataset.column_names)

    return table


def from_arrow(tables, size=None):
    try:
        import pyarrow.lib as lib
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error

    def _type_convert(field_type):
        if field_type.id == lib.Type_BOOL:
            return bool
        if field_type.id == lib.Type_STRING:
            return str
        if field_type.id in {
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
        if field_type.id in {lib.Type_HALF_FLOAT, lib.Type_FLOAT, lib.Type_DOUBLE}:
            return float

    def _peek(iterable):
        iter1, iter2 = itertools.tee(iterable)
        try:
            first = next(iter1)
        except StopIteration:
            return None, iter([])
        else:
            return first, iter2

    if not isinstance(tables, (typing.Generator, list, tuple)):
        tables = [tables]

    if isinstance(tables, (list, tuple)):
        tables = iter(tables)

    first_table, all_tables = _peek(tables)
    if first_table is None:
        # return nothing back
        return [], {}

    schema = first_table.schema
    fields = {
        str(field.name): {"type": _type_convert(field.type), "nullable": field.nullable}
        for field in schema
    }

    # Create a generator of tuples from the columns
    row_factory = Row.create_class(fields)
    rows = (
        (row_factory(col[i].as_py() for col in [table.column(j) for j in schema.names]))
        for table in all_tables
        for i in range(table.num_rows)
    )

    # Limit the number of rows to 'size'
    if size:
        rows = _tools.islice(rows, size)

    return rows, fields


def to_pandas(dataset, size=None):
    try:
        import pandas
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error
    return pandas.DataFrame(r.as_dict for r in dataset.slice(0, size))


def from_pandas(pandas):
    raise NotImplementedError()


def to_polars(dataset, size=None):
    try:
        import polars
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error
    return polars.DataFrame(r.as_dict for r in dataset.slice(0, size))


def from_polars(polars):
    raise NotImplementedError()


def to_csv(dataset):
    raise NotImplementedError()


def from_csv(values):
    raise NotImplementedError()


def to_parquet(dataset):
    raise NotImplementedError()


def from_parquet(values):
    raise NotImplementedError()


def to_jsonl(dataset):
    raise NotImplementedError()


def from_jsonl(values):
    raise NotImplementedError()
