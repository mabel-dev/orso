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
from dataclasses import _MISSING_TYPE
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from decimal import Decimal
from enum import Enum
from warnings import warn

import numpy

from orso.exceptions import ColumnDefinitionError
from orso.exceptions import DataValidationError
from orso.tools import arrow_type_map
from orso.tools import random_string
from orso.types import ORSO_TO_PYTHON_MAP
from orso.types import PYTHON_TO_ORSO_MAP
from orso.types import OrsoTypes


@dataclass(init=False)
class FlatColumn:
    """
    This is a standard column type.
    Unlike the other column types, we don't store the values for this Column
    here, we read them from the underlying data structure (orso/pyarrow/velox).
    """

    name: str
    type: OrsoTypes
    description: typing.Optional[str] = None
    aliases: typing.Optional[typing.List[str]] = field(default_factory=list)  # type: ignore
    nullable: bool = True
    expectations: typing.Optional[list] = field(default_factory=list)
    identity: str = field(default_factory=random_string)
    precision: typing.Optional[int] = None
    scale: typing.Optional[int] = None

    def __init__(self, **kwargs):
        attributes = {f.name: f for f in fields(self.__class__)}
        for attribute in attributes:
            if attribute in kwargs:
                setattr(self, attribute, kwargs[attribute])
            elif not isinstance(attributes[attribute].default, _MISSING_TYPE):
                setattr(self, attribute, attributes[attribute].default)
            elif not isinstance(attributes[attribute].default_factory, _MISSING_TYPE):
                setattr(self, attribute, attributes[attribute].default_factory())  # type:ignore
            else:
                raise ColumnDefinitionError(attribute)

        # map literals to OrsoTypes
        if self.type.__class__ is not OrsoTypes:
            type_name = str(self.type).upper()
            if type_name in OrsoTypes.__members__.keys():
                self.type = OrsoTypes[type_name]
            elif type_name == "LIST":
                warn("Column type LIST will be deprecated in a future version, use ARRAY instead.")
                self.type = OrsoTypes.ARRAY
            elif type_name == "NUMERIC":
                warn(
                    "Column type NUMERIC will be deprecated in a future version, use DECIMAL, DOUBLE or INTEGER instead. Mapped to DOUBLE, this may not be compatible with all values NUMERIC was compatible with."
                )
                self.type = OrsoTypes.DOUBLE
            elif self.type != 0:
                raise ValueError(f"Unknown column type {self.type} for column {self.name}")

    def __str__(self):
        return self.identity

    def materialize(self):
        raise TypeError("Cannot materialize FlatColumns")

    @classmethod
    def from_arrow(cls, arrow_field):
        """
        Help converting from from Arrow to Orso
        """
        native_type = arrow_type_map(arrow_field.type)
        scale = None
        precision = None
        if isinstance(native_type, Decimal):
            field_type = OrsoTypes.DECIMAL
            scale = native_type.scale  # type:ignore
            precision = native_type.precision  # type:ignore
        else:
            field_type = PYTHON_TO_ORSO_MAP.get(native_type)
        return cls(
            name=str(arrow_field.name),
            type=field_type,
            nullable=arrow_field.nullable,
            scale=scale,
            precision=precision,
        )


@dataclass(init=False)
class FunctionColumn(FlatColumn):
    """
    This is a virtual column, it's nominally a column where the value is
    derived from a function.
    """

    binding: typing.Optional[typing.Callable] = lambda: None
    configuration: typing.Tuple = field(default_factory=tuple)
    length: int = 1

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def materialize(self):
        """
        Turn this virtual column into a list
        """
        value = self.binding(*self.configuration)
        return numpy.array([value] * self.length)


@dataclass(init=False)
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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def materialize(self):
        """
        Turn this virtual column into a list
        """
        return numpy.array([self.value] * self.length)


@dataclass(init=False)
class DictionaryColumn(FlatColumn):
    """
    If we know a column has a small amount of unique values AND is a large column
    AND we're going to perform an operation on the values, we should dictionary
    encode the column. This allows us to operate once on each unique value in the
    column, rather than each value in the column individually. At the cost of
    constructing and materializing the dictionary encoding.
    """

    values: typing.List[typing.Any] = field(default_factory=list)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

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

    def __iter__(self):
        """Return an iterator over column names"""
        return iter([col.name for col in self.columns])

    def __add__(self, other):
        """When we add schemas together we combine the se of columns"""
        new_schema = RelationSchema(name=self.name, aliases=self.aliases, columns=[])

        # Create a new list to hold the merged columns
        new_columns = self.columns[:]

        # Keep track of the seen identities - preload with the current set
        seen_identities = [col.identity for col in self.columns]

        for column in other.columns:
            if column.identity not in seen_identities:
                seen_identities.append(column.identity)
                new_columns.append(column)

        # Assign the new list of columns to the new schema
        new_schema.columns = new_columns

        return new_schema

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

    @property
    def column_names(self):
        """return all the names for columns in this relation"""
        return [column.name for column in self.columns]

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
