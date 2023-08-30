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


from dataclasses import _MISSING_TYPE
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from decimal import Decimal
from enum import Enum
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import MutableMapping
from typing import Optional
from typing import Tuple
from typing import Union
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
    description: Optional[str] = None
    aliases: Optional[List[str]] = field(default_factory=list)  # type: ignore
    nullable: bool = True
    expectations: Optional[list] = field(default_factory=list)
    identity: str = field(default_factory=random_string)
    precision: Optional[int] = None
    scale: Optional[int] = None

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
            elif type_name == "STRING":
                raise ValueError(
                    f"Unknown column type '{self.type}' for column '{self.name}'. Did you mean 'VARCHAR'?"
                )
            elif self.type != 0:
                raise ValueError(f"Unknown column type '{self.type}' for column '{self.name}'.")

    def __str__(self):
        return self.identity

    def materialize(self):
        raise TypeError("Cannot materialize FlatColumns")

    @classmethod
    def from_arrow(cls, arrow_field) -> "FlatColumn":
        """
        Converts a PyArrow field to an Orso FlatColumn object.

        Parameters:
            arrow_field: Field
                PyArrow Field object to be converted.

        Returns:
            FlatColumn: A FlatColumn object containing the converted information.
        """
        # Fetch the native type mapping from Arrow to Python native types
        native_type = arrow_type_map(arrow_field.type)
        # Initialize variables to hold optional decimal properties
        scale: Optional[int] = None
        precision: Optional[int] = None
        # Check if the type is Decimal and populate scale and precision
        if isinstance(native_type, Decimal):
            field_type = OrsoTypes.DECIMAL
            scale = native_type.scale  # type:ignore
            precision = native_type.precision  # type:ignore
        else:
            # Fall back to the generic mapping
            field_type = PYTHON_TO_ORSO_MAP.get(native_type, None)
            if field_type is None:
                raise ValueError(f"Unsupported type: {native_type}")

        return FlatColumn(
            name=str(arrow_field.name),
            type=field_type,
            nullable=arrow_field.nullable,
            scale=scale,
            precision=precision,
        )

    def to_flatcolumn(self) -> "FlatColumn":
        """
        Convert any column type to a FlatColumn (e.g. after a FunctionColumn has been evaluated)
        """
        return FlatColumn(
            name=str(self.name),
            description=self.description,
            aliases=self.aliases,
            identity=self.identity,
            type=self.type,
            nullable=self.nullable,
            scale=self.scale,
            precision=self.precision,
        )


@dataclass(init=False)
class FunctionColumn(FlatColumn):
    """
    This is a virtual column, it's nominally a column where the value is
    derived from a function.
    """

    binding: Optional[Callable] = lambda: None
    configuration: Tuple = field(default_factory=tuple)
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
    value: Any = None

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

    values: List[Any] = field(default_factory=list)

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
    aliases: List[str] = field(default_factory=list)
    columns: List[FlatColumn] = field(default_factory=list)

    def __iter__(self):
        """Return an iterator over column names."""
        return iter([col.name for col in self.columns])

    def __add__(self, other: "RelationSchema") -> "RelationSchema":
        """
        When we add schemas together, we combine the set of columns.

        Parameters:
            other: RelationSchema
                The other schema to be added.

        Returns:
            RelationSchema: A new RelationSchema containing the combined columns.
        """
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
    def num_columns(self) -> int:
        """Returns the number of columns in the schema."""
        return len(self.columns)

    def find_column(self, column_name: str) -> Optional[FlatColumn]:
        """
        Find a column by name or alias.

        Parameters:
            column_name: str
                Name or alias of the column.

        Returns:
            Optional[FlatColumn]: The FlatColumn object, if found. None otherwise.
        """
        for column in self.columns:
            if column.name == column_name:
                return column
            if column_name in column.aliases:
                return column
        return None

    def all_column_names(self) -> List[str]:
        """
        Return all the names and aliases for columns in this relation.

        Returns:
            List[str]: List of all names and aliases.
        """

        def _inner():
            for column in self.columns:
                yield column.name
                yield from column.aliases

        return list(_inner())

    @property
    def column_names(self) -> List[str]:
        """Return the names for columns in this relation."""
        return [column.name for column in self.columns]

    def column(self, i: Union[int, str]) -> Optional[FlatColumn]:
        """
        Get column by name or index.

        Parameters:
            i: Union[int, str]
                Index or name of the column.

        Returns:
            Optional[FlatColumn]: The FlatColumn object, if found. None otherwise.
        """
        if isinstance(i, int):
            return self.columns[i]
        else:
            return self.find_column(i)

    def pop_column(self, column_name: str) -> Optional[FlatColumn]:
        """
        Remove a column by its name and return it.

        Parameters:
            column_name: str
                Name of the column to be removed.

        Returns:
            Optional[FlatColumn]: The removed column if found, otherwise None.
        """
        for idx, column in enumerate(self.columns):
            if column.name == column_name:
                return self.columns.pop(idx)
        return None

    def to_dict(self) -> Dict:
        """
        Convert the Schema to a dictionary.

        Returns:
            Dict: A dictionary representation of the schema.
        """

        def _converter(obj) -> Dict:
            """Handle enum serialization."""
            return {key: value.value if isinstance(value, Enum) else value for key, value in obj}

        return asdict(self, dict_factory=_converter)

    @classmethod
    def from_dict(cls, dic: Dict) -> "RelationSchema":
        """
        Create a Schema from a dictionary.

        Parameters:
            dic: Dict
                A dictionary to convert into a RelationSchema.

        Returns:
            RelationSchema: A new RelationSchema object.
        """
        schema = RelationSchema(name=dic["name"], aliases=dic.get("aliases", []))
        for column in dic["columns"]:
            schema.columns.append(FlatColumn(**column))
        return schema

    def validate(self, data: MutableMapping) -> bool:
        """
        Perform schema validation against a dictionary-formatted record.

        Parameters:
            data: MutableMapping
                A dictionary containing the data to validate against the schema.

        Returns:
            bool: True if the data is valid according to the schema.

        Raises:
            TypeError: If data is not dictionary-like.
            DataValidationError: If data validation fails.
        """
        if not isinstance(data, MutableMapping):
            raise TypeError("Cannot validate non Dictionary-type value")

        for column in self.columns:
            if column.name not in data:
                raise DataValidationError(column=column, value=None, error="Column Missing")

            value = data[column.name]

            if value is None:
                if not column.nullable:
                    raise DataValidationError(
                        column=column, value=value, error="None not acceptable"
                    )
            else:
                if not isinstance(value, ORSO_TO_PYTHON_MAP.get(column.type)):
                    raise DataValidationError(column=column, value=value, error="Incorrect Type")

        return True
