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

"""
Column Classes

This module defines several specialized column types, aimed at providing efficient
memory usage and computational speed for various SQL queries. The column types differ
in their internal data representation but expose a common interface for manipulation
and query operations.

Features:
    - All column types (except FlatColumn & FunctionColumn) use compressed internal
      representations to conserve memory.
    - The compressed data is exposed through a 'values' property. This allows direct
      operations on compressed data, which usually involves fewer items compared to the
      uncompressed list.
    - Each column type provides a 'materialize' method to expand the compressed data
      into its uncompressed form, facilitating query operations that require a full
      column of data.

Column Types:
    - SparseColumn: Handles sparse data by only storing non-default values.
    - DictionaryColumn: Uses a dictionary to encode a finite set of string or
      numerical values.
    - RLEColumn: Utilizes Run-Length Encoding for sequences of repeating elements.
    - ConstantColumn: Represents a column of constant values using a single value and a
      length attribute.
    - FlatColumn: Standard column type that stores data in an uncompressed form.
    - FunctionColumn: Used when the column is the result of a function, useful as a
      placeholder column type.

By leveraging these compressed representations, Orso aims to provide a data store that
is both memory-efficient and computationally fast. For most operations, manipulating
the compressed data directly, bypassing the need to materialize the full column,
leading to faster evaluation.

Example:
    sparse_col = SparseColumn(name='col1', type=OrsoTypes.INTEGER, values=[1, None, 3])
    sparse_col.values *= 2  # Perform operation directly on compressed data
    full_array = sparse_col.materialize()  # Get the full, uncompressed array when needed

"""

from collections import defaultdict
from dataclasses import _MISSING_TYPE
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from decimal import getcontext
from enum import Enum
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import MutableMapping
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union
from warnings import warn

import numpy
import orjson
from data_expectations import Expectation

from orso.exceptions import ColumnDefinitionError
from orso.exceptions import DataValidationError
from orso.exceptions import ExcessColumnsInDataError
from orso.tools import arrow_type_map
from orso.tools import random_string
from orso.types import ORSO_TO_PYTHON_MAP
from orso.types import PYTHON_TO_ORSO_MAP
from orso.types import OrsoTypes

_MISSING_VALUE: str = str()
DECIMAL_PRECISION: int = getcontext().prec


class ColumnDisposition(Enum):
    NAME = "name"
    AGE = "age"


class SchemaExpectation(Expectation):
    column = _MISSING_VALUE

    @classmethod
    def load(cls: Type["Expectation"], serialized: Union[Dict[str, Any], str]) -> "Expectation":
        """
        Loads a serialized Expectation and returns it as an instance.

        Parameters:
            serialized: Serialized Expectation as a dictionary or JSON string.

        Returns:
            An Expectation instance populated with the serialized data.
        """
        import json
        from copy import deepcopy

        if isinstance(serialized, str):
            serialized = dict(json.loads(serialized))
        serialized_copy: dict = deepcopy(serialized)
        if "expectation" not in serialized_copy:
            raise ValueError("Missing 'expectation' key in Expectation.")
        expectation = serialized_copy.pop("expectation")
        column = serialized_copy.pop("column", _MISSING_VALUE)
        ignore_nulls = serialized_copy.pop("ignore_nulls", True)
        config = serialized_copy
        return Expectation(
            expectation=expectation, column=column, ignore_nulls=ignore_nulls, config=config
        )


@dataclass(init=False)
class FlatColumn:
    """
    This is a standard column type.
    Unlike the other column types, we don't store the values for this Column
    here, we read them from the underlying data structure (orso/pyarrow/velox).
    """

    name: str
    default: Optional[Any] = None
    type: OrsoTypes = OrsoTypes._MISSING_TYPE
    element_type: Optional[OrsoTypes] = None
    description: Optional[str] = None
    disposition: Optional[ColumnDisposition] = None
    aliases: Optional[List[str]] = field(default_factory=list)  # type: ignore
    nullable: bool = True
    expectations: Optional[Expectation] = field(default_factory=list)
    identity: str = field(default_factory=random_string)
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    origin: Optional[List[str]] = field(default_factory=list)
    highest_value: Optional[Any] = None
    lowest_value: Optional[Any] = None
    null_count: Optional[int] = None

    def __init__(self, **kwargs):
        attributes = {f.name: f for f in fields(self.__class__)}
        for attribute in attributes:
            if attribute in kwargs:
                value = kwargs[attribute]
                # Special handling for 'expectations'
                if attribute == "expectations" and value:
                    value = [
                        (
                            v
                            if isinstance(v, Expectation)
                            else (
                                SchemaExpectation.load(v).update({"column": kwargs["name"]})
                                if v.get("column", _MISSING_VALUE) == _MISSING_VALUE
                                else v
                            )
                        )
                        for v in value
                    ]

                setattr(self, attribute, value)
            elif not isinstance(attributes[attribute].default, _MISSING_TYPE):
                setattr(self, attribute, attributes[attribute].default)
            elif not isinstance(attributes[attribute].default_factory, _MISSING_TYPE):
                setattr(self, attribute, attributes[attribute].default_factory())  # type:ignore
            else:
                raise ColumnDefinitionError(attribute)

        # map literals to OrsoTypes
        if self.type.__class__ is not OrsoTypes:
            self.type, _length, _precision, _scale, _element_type = OrsoTypes.from_name(self.type)
            if self.type == OrsoTypes._MISSING_TYPE:
                warn(f"Column '{self.name}' type not recognized.")
            if isinstance(self.type, OrsoTypes):
                if self.element_type is None:
                    self.element_type = _element_type
                if self.precision is None:
                    self.precision = _precision
                if self.scale is None:
                    self.scale = _scale
                if self.length is None:
                    self.length = _length

        # if we have a default value, parse it to the correct type and fail if we can't
        if self.default:
            try:
                self.default = self.type.parse(self.default)
            except Exception:
                raise ValueError(
                    f"Column '{self.name}' default value not compatible with '{self.type}'."
                )

        # validate decimal properties
        if self.type == OrsoTypes.DECIMAL and self.precision is None:
            from decimal import getcontext

            self.precision = getcontext().prec
        if self.type == OrsoTypes.DECIMAL and self.scale is None:
            self.scale = int(0.75 * self.precision)

    def __str__(self):
        return self.identity

    def materialize(self):
        raise TypeError("Cannot materialize FlatColumns")

    @classmethod
    def from_arrow(cls, arrow_field, mappable_as_binary: bool = False) -> "FlatColumn":
        """
        Converts a PyArrow field to an Orso FlatColumn object.

        Parameters:
            arrow_field: Field
                PyArrow Field object to be converted.

        Returns:
            FlatColumn: A FlatColumn object containing the converted information.
        """
        from orso.tools import DecimalFactory

        # Fetch the native type mapping from Arrow to Python native types
        native_type = arrow_type_map(arrow_field.type)
        element_type = None
        # Initialize variables to hold optional decimal properties
        scale: Optional[int] = None
        precision: Optional[int] = None
        # Check if the type is Decimal and populate scale and precision
        if isinstance(native_type, DecimalFactory):
            field_type = OrsoTypes.DECIMAL
            scale = native_type.scale  # type:ignore
            precision = native_type.precision  # type:ignore
        elif mappable_as_binary and native_type == dict:
            field_type = OrsoTypes.BLOB
        elif native_type == list:
            field_type = OrsoTypes.ARRAY
            element_type = PYTHON_TO_ORSO_MAP.get(arrow_type_map(arrow_field.type.value_type))
        else:
            # Fall back to the generic mapping
            field_type = PYTHON_TO_ORSO_MAP.get(native_type, None)
            if field_type is None:
                field_type = OrsoTypes.VARCHAR

        return FlatColumn(
            name=str(arrow_field.name),
            type=field_type,
            element_type=element_type,
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
            default=self.default,
            description=self.description,
            aliases=self.aliases,
            identity=self.identity,
            type=self.type,
            element_type=self.element_type,
            nullable=self.nullable,
            scale=self.scale,
            precision=self.precision,
            lowest_value=self.lowest_value,
            highest_value=self.highest_value,
            null_count=self.null_count,
        )

    @property
    def all_names(self):
        """simplify collection of all of the names for this column"""
        if self.aliases is not None:
            return self.aliases + [self.name]
        return [self.name]

    @property
    def arrow_field(self):
        import pyarrow

        # fmt: off
        type_map: dict = {
            OrsoTypes.BOOLEAN: pyarrow.bool_(),
            OrsoTypes.BLOB: pyarrow.binary(),
            OrsoTypes.DATE: pyarrow.date64(),
            OrsoTypes.TIMESTAMP: pyarrow.timestamp("us"),
            OrsoTypes.TIME: pyarrow.time32("ms"),
            OrsoTypes.INTERVAL: pyarrow.month_day_nano_interval(),
            OrsoTypes.STRUCT: pyarrow.binary(),  # convert structs to JSON strings/BSONs
            OrsoTypes.DECIMAL: pyarrow.decimal128(self.precision or DECIMAL_PRECISION, self.scale or 10),
            OrsoTypes.DOUBLE: pyarrow.float64(),
            OrsoTypes.INTEGER: pyarrow.int64(),
            OrsoTypes.ARRAY: pyarrow.list_(pyarrow.string()),
            OrsoTypes.VARCHAR: pyarrow.string(),
            OrsoTypes.JSONB: pyarrow.binary(),
            OrsoTypes.NULL: pyarrow.null(),
        }
        # fmt: on

        if self.type == OrsoTypes.ARRAY:
            return pyarrow.field(
                name=self.name,
                type=pyarrow.list_(type_map.get(self.element_type, pyarrow.string())),
            )

        return pyarrow.field(name=self.name, type=type_map.get(self.type, pyarrow.string()))

    def to_json(self) -> str:
        def default_serializer(o):
            if isinstance(o, OrsoTypes):
                return str(o)
            if isinstance(o, Expectation):
                return o.__dict__
            raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

        return orjson.dumps(asdict(self), default=default_serializer)

    @classmethod
    def from_json(cls, json_str: str) -> "FlatColumn":
        def custom_decoder(dct):
            if "type" in dct:
                dct["type"] = OrsoTypes.__members__.get(dct["type"], OrsoTypes._MISSING_TYPE)
            if "expectations" in dct and isinstance(dct["expectations"], list):
                dct["expectations"] = [
                    SchemaExpectation.load(v) if isinstance(v, dict) else v
                    for v in dct["expectations"]
                ]
            return dct

        data = orjson.loads(json_str)
        return cls(**data)


@dataclass(init=False)
class FunctionColumn(FlatColumn):
    """
    This is a virtual column, it's nominally a column where the value is
    derived from a function.
    """

    binding: Optional[Callable] = lambda: None
    configuration: Tuple = field(default_factory=tuple)
    length: int = 1

    @property
    def values(self):
        raise TypeError("FunctionColumn has no 'values', do you mean 'materialize'?")

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

    Note: We don't implement anything here which deals with doing operations on
    two constant columns; whilst that would be a good optimization, the better
    way to do this is in the query optimizer, do operations on two constants
    while we're still working with a query plan.
    """

    length: int = 1
    value: Any = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.values = numpy.array([self.value])

    def materialize(self):
        """
        Turn this virtual column into a list.
        When performing element-wise operations, use value_array for broadcasting.
        """
        return numpy.full(self.length, self.values)


@dataclass(init=False)
class SparseColumn(FlatColumn):
    """
    This is a column type optimized for sparse data.
    Only the non-default values and their indices are stored.
    """

    values: numpy.ndarray = None
    default_value: Any = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        (self.indices,) = numpy.where(numpy.array(self.values) != self.default_value)
        self.values = numpy.array(self.values)[self.indices]
        self.total_length = len(kwargs.get("values", []))  # Store the total length

    def materialize(self):
        """
        Materialize the sparse column into a full numpy array.
        """
        materialized = numpy.full(
            self.total_length, self.default_value
        )  # Initialize with default values
        materialized[self.indices] = self.values
        return materialized


@dataclass(init=False)
class RLEColumn(FlatColumn):
    """
    This is a column type optimized for sequences of repeated values.
    """

    values: numpy.ndarray = None
    lengths: List[int] = field(default_factory=list)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        run_values = []
        run_lengths = []

        if len(self.values) == 0:
            self.values = numpy.array([])
            return

        prev_value = self.values[0]
        run_length = 1

        for value in self.values[1:]:
            if value == prev_value:
                run_length += 1
            else:
                run_values.append(prev_value)
                run_lengths.append(run_length)

                prev_value = value
                run_length = 1

        run_values.append(prev_value)
        run_lengths.append(run_length)

        self.values = numpy.array(run_values)
        self.lengths = run_lengths

    def materialize(self):
        """
        Turn this compressed column back into its original form.
        """
        materialized = []
        for value, length in zip(self.values, self.lengths):
            materialized.extend([value] * length)
        return numpy.array(materialized)


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
        self.values, self.encoding = numpy.unique(values, return_inverse=True)

    def materialize(self):
        """
        Turn this virtual column into a list
        """
        return self.values[self.encoding]


@dataclass
class RelationSchema:
    name: str
    aliases: List[str] = field(default_factory=list)
    columns: List[FlatColumn] = field(default_factory=list)
    primary_key: Optional[str] = None

    row_count_metric: Optional[int] = None
    """Statistic of the number of rows in the relation."""
    row_count_estimate: Optional[int] = None
    """Estimate of the number of rows in the relation."""
    data_size_metric: Optional[int] = None
    """Statistic of the size of the data in the relation."""
    data_size_estimate: Optional[int] = None
    """Estimate of the size of the data in the relation."""

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

    def find_column(self, column_name: str, case_insensitive: bool = False) -> Optional[FlatColumn]:
        """
        Find a column by name or alias.

        Parameters:
            column_name: str
                Name or alias of the column.

        Returns:
            Optional[FlatColumn]: The FlatColumn object, if found. None otherwise.
        """
        if case_insensitive:
            for column in self.columns:
                if column_name.lower() in [c.lower() for c in column.all_names]:
                    return column
        else:
            for column in self.columns:
                if column_name in column.all_names:
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
                yield from column.all_names

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
    def from_dict(cls, dic: dict) -> "RelationSchema":
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
            if isinstance(column, dict):
                schema.columns.append(FlatColumn(**column))
            if isinstance(column, str):
                schema.columns.append(FlatColumn(name=column))
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

        # Check if all fields in 'data' are in the schema
        extra_fields = set(data.keys()) - set(column.name for column in self.columns)
        if extra_fields:
            raise ExcessColumnsInDataError(columns=extra_fields)

        errors = defaultdict(list)

        for column in self.columns:
            if column.name not in data:
                errors["Column in Schema Not Found in Record"].append(column.name)

            else:
                value = data[column.name]

                if value is None:
                    if not column.nullable:
                        errors["Column not Nullable"].append(column.name)
                elif column.type != OrsoTypes._MISSING_TYPE and not isinstance(
                    value, ORSO_TO_PYTHON_MAP[column.type]
                ):
                    errors["Incorrect Type"].append(
                        (
                            column.name,
                            value,
                            column.type,
                        )
                    )

        if errors:
            raise DataValidationError(errors=errors)
        return True

    def to_json(self):
        return {
            "name": self.name,
            "aliases": self.aliases,
            "primary_key": self.primary_key,
            "columns": [col.to_json() for col in self.columns],
        }


def convert_arrow_schema_to_orso_schema(arrow_schema):
    return RelationSchema(
        name="arrow",
        columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
    )


def convert_orso_schema_to_arrow_schema(orso_schema, use_identities: bool = False):
    from pyarrow import field
    from pyarrow import schema

    if not use_identities:
        return schema([col.arrow_field for col in orso_schema.columns])

    return schema(
        [field(name=col.identity, type=col.arrow_field.type) for col in orso_schema.columns]
    )
