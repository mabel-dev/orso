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

from orso.exceptions import MissingDependencyError
from orso.row import Row
from orso.schema import FlatColumn
from orso.schema import RelationSchema


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
    """
    Convert an Arrow table or an iterable of Arrow tables to a generator of
    Python objects.
    """
    if not isinstance(tables, (typing.Generator, list, tuple)):
        tables = [tables]

    if isinstance(tables, (list, tuple)):
        tables = iter(tables)

    # Extract schema information from the first table
    first_table = next(tables, None)
    if first_table is None:
        return [], {}

    arrow_schema = first_table.schema

    orso_schema = RelationSchema(
        name="arrow",
        columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
    )

    # Create a generator of tuples from the columns
    row_factory = Row.create_class(orso_schema)

    BATCH_SIZE: int = 10000
    if size:
        BATCH_SIZE = min(size, BATCH_SIZE)

    rows: typing.List[Row] = []
    for table in itertools.chain([first_table], tables):
        batches = table.to_batches(max_chunksize=BATCH_SIZE)
        for batch in batches:
            # column_data = [column.to_numpy(zero_copy_only=False) for column in batch.columns]
            column_data = [column for column in batch.columns]
            # column_data = [column.to_pandas().array for column in batch.columns]
            for i in range(len(batch)):
                row_data = (col[i].as_py() for col in column_data)
                rows.append(row_factory(row_data))  # type:ignore
            if size and len(rows) >= size:
                break

    # Limit the number of rows to 'size'
    if size:
        rows = itertools.islice(rows, size)  # type:ignore

    return rows, orso_schema


def to_pandas(dataset, size=None):
    """wrap the arrow function to convert to pandas"""
    return dataset.arrow(size).to_pandas()


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
