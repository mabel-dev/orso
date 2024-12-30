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

from orso.compute.compiled import process_table
from orso.exceptions import MissingDependencyError
from orso.row import Row
from orso.schema import FlatColumn
from orso.schema import RelationSchema


class _RowsIterator:
    """
    Iterator class for processing PyArrow tables lazily.

    Parameters:
        tables: Iterable of PyArrow tables to process.
        row_factory: Factory method to create Row instances.
        batch_size: Number of rows to process at a time.
        max_size: Maximum number of rows to return.
    """

    def __init__(
        self, tables: typing.Iterable, row_factory: typing.Callable, batch_size: int, max_size: int
    ):
        self.tables = itertools.chain(tables)
        self.row_factory = row_factory
        self.batch_size = batch_size
        self.max_size = max_size
        self.rows_processed = 0
        self.current_table = None
        self.current_rows = iter([])

    def __iter__(self):
        return self

    def __next__(self):
        if self.rows_processed >= self.max_size:
            raise StopIteration()

        row = next(self.current_rows, None)
        if row is not None:
            self.rows_processed += 1
            return row
        else:
            # Fetch the next table and process it
            self.current_table = next(self.tables, None)
            if self.current_table is None:
                raise StopIteration()

            self.current_rows = iter(
                process_table(self.current_table, self.row_factory, self.batch_size)
            )

            # Check if the new table has rows to process
            row = next(self.current_rows, None)
            if row is not None:
                self.rows_processed += 1
                return row
            else:
                raise StopIteration()


def to_arrow(dataset, size=None):
    try:
        import pyarrow
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error

    if size is not None and size >= 0:
        dataset = dataset.head(size)

    if dataset.rowcount == 0:
        arrays = [list() for _ in range(dataset.columncount)]
    else:
        arrays = list(zip(*dataset._rows))

    return pyarrow.Table.from_arrays(arrays, dataset.column_names)


def from_arrow(tables, size=None):
    """
    Convert an Arrow table or an iterable of Arrow tables to a generator of
    Python objects, handling each block one at a time.
    """
    if not isinstance(tables, (typing.Generator, list, tuple)):
        tables = [tables]

    if isinstance(tables, (list, tuple)):
        tables = iter(tables)

    BATCH_SIZE: int = 10_000
    if size:
        BATCH_SIZE = min(size, BATCH_SIZE)
    else:
        size = float("inf")

    # Extract schema information from the first table
    first_table = next(tables, None)
    if first_table is None:
        return iter([]), {}

    arrow_schema = first_table.schema
    orso_schema = RelationSchema(
        name="arrow",
        columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
    )
    row_factory = Row.create_class(orso_schema, tuples_only=True)

    # Create an bespoke lazy iterator instance
    rows_iterator = _RowsIterator(
        tables=itertools.chain([first_table], tables),
        row_factory=row_factory,
        batch_size=BATCH_SIZE,
        max_size=size,
    )

    return rows_iterator, orso_schema


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
