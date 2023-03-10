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

from orso.dataframe import DataFrame
from orso.exceptions import MissingDependencyError
from orso.row import Row


def to_arrow(dataset):
    try:
        import pyarrow
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error
    # Create a list of PyArrow arrays from the rows
    arrays = [pyarrow.array(col) for col in zip(*dataset._rows)]
    # Create a PyArrow table from the arrays and schema
    table = pyarrow.Table.from_arrays(arrays, dataset.column_names)

    return table


def from_arrow(tables):
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
    schema = first_table.schema
    fields = {
        str(field.name): {"type": _type_convert(field.type), "nullable": field.nullable}
        for field in schema
    }

    # Create a list of tuples from the columns
    row_factory = Row.create_class(fields)
    rows = (
        (row_factory(col[i].as_py() for col in [table.column(j) for j in schema.names]))
        for table in all_tables
        for i in range(table.num_rows)
    )

    return DataFrame(rows=rows, schema=fields)


def to_pandas(dataset):
    raise NotImplementedError()


def from_pandas(pandas):
    raise NotImplementedError()


def to_polars(dataset):
    raise NotImplementedError()


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
