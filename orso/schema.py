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

import typing
from dataclasses import dataclass
from dataclasses import field
from enum import Enum

import numpy

from orso.exceptions import DataValidationError
from orso.tools import random_string
from orso.types import ORSO_TO_PYTHON_MAP
from orso.types import OrsoTypes


@dataclass
class FlatColumn:
    """
    This is a standard column type.
    Unlike the other column types, we don't store the values for this Column
    here, we read them from the underlying data structure (orso/pyarrow/velox).
    """

    name: str
    type: OrsoTypes
    description: typing.Optional[str] = None
    aliases: typing.Optional[typing.List[str]] = field(default_factory=list)
    nullable: bool = True
    expectations: typing.Optional[list] = field(default_factory=list)
    identity: str = field(default_factory=random_string)

    def __str__(self):
        return self.identity

    def materialize(self):
        raise TypeError("Cannot materialize FlatColumns")


@dataclass
class FunctionColumn(FlatColumn):
    """
    This is a virtual column, it's nominally a column where the value is
    derived from a function.
    """

    binding: typing.Optional[typing.Callable] = lambda: None
    configuration: typing.Tuple = field(default_factory=tuple)
    length: int = 1

    def materialize(self):
        """
        Turn this virtual column into a list
        """
        value = self.binding(*self.configuration)
        return numpy.array([value] * self.length)


@dataclass
class ConstantColumn(FlatColumn):
    """
    Rather than pass around columns of constant values, where we can we should
    replace them with this column type.

    note we don't implement anything here which deals with doing operations on
    two constant columns; whilst that would be a good optimization, the better
    way to do this is in the query optimizer, do operations on two constants
    while we're still working with a query plan.
    """

    length: int = 1
    value: typing.Any = None

    def materialize(self):
        """
        Turn this virtual column into a list
        """
        return numpy.array([self.value] * self.length)


@dataclass
class DictionaryColumn(FlatColumn):
    """
    If we know a column has a small amount of unique values AND is a large column
    AND we're going to perform an operation on the values, we should dictionary
    encode the column. This allows us to operate once on each unique value in the
    column, rather than each value in the column individually. At the cost of
    constructing and materializing the dictionary encoding.
    """

    values: typing.List[typing.Any] = field(default_factory=list)

    def __post_init__(self):
        """
        Perform the encoding for this column
        """
        values = numpy.asarray(self.values)
        self.dictionary, self.encoding = numpy.unique(values, return_inverse=True)

    def materialize(self):
        """
        Turn this virtual column into a list
        """
        return self.dictionary[self.encoding]


@dataclass
class RelationSchema:
    name: str
    aliases: typing.List[str] = field(default_factory=list)
    columns: typing.List[FlatColumn] = field(default_factory=list)

    @property
    def num_columns(self):
        return len(self.columns)

    def find_column(self, column_name: str):
        """find a column by name or alias"""
        for column in self.columns:
            if column.name == column_name:
                return column
            if column_name in column.aliases:
                return column
        return None

    def all_column_names(self):
        """return all the names for columns in this relation"""

        def _inner():
            for column in self.columns:
                yield column.name
                yield from column.aliases

        return list(_inner())

    def column(self, i):
        """get column by name or index"""
        if isinstance(i, int):
            return self.columns[i]
        else:
            return self.find_column(i)

    def to_dict(self):
        """Convert a Schema to a dictionary"""
        from dataclasses import asdict

        def _converter(obj):
            """handle enum serialization"""
            return {key: value.value if isinstance(value, Enum) else value for key, value in obj}

        return asdict(self, dict_factory=_converter)

    @classmethod
    def from_dict(cls, dic):
        """Create a Schema from a dictionary"""
        schema = RelationSchema(
            name=dic["name"],
            aliases=dic.get("aliases", []),
        )
        for column in dic["columns"]:
            schema.columns.append(FlatColumn(**column))
        return schema

    def validate(self, data: dict):
        """
        Perform schema validation against a dictionary formatted record
        """
        # If it's not dictionary-like, it's not valid
        if not isinstance(data, typing.MutableMapping):
            raise TypeError("Cannot validate non Dictionary-type value")

        for column in self.columns:
            # If the column is missing from the data, it's not valid
            if column.name not in data:
                raise DataValidationError(column=column, value=None, error="Column Missing")

            # Get the value for this field out of the dict
            value = data[column.name]

            # If the value is null, it's value if it's nullable
            if value is None:
                if not column.nullable:
                    raise DataValidationError(
                        column=column, value=value, error="None not acceptable"
                    )
            else:
                # finally, is it the right type
                if not isinstance(value, ORSO_TO_PYTHON_MAP.get(column.type)):
                    raise DataValidationError(column=column, value=value, error="Incorrect Type")

        return True
