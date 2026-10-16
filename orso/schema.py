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

import json
from collections import defaultdict
from collections.abc import MutableMapping
from dataclasses import MISSING as _MISSING
from dataclasses import _MISSING_TYPE
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from dataclasses import fields as dataclass_fields
from decimal import getcontext
from enum import Enum
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union
from warnings import warn

from data_expectations import Expectation

from orso._native import TrackedSlot
from orso._native import schema_epoch
from orso.exceptions import ColumnDefinitionError
from orso.exceptions import DataValidationError
from orso.exceptions import ExcessColumnsInDataError
from orso.exceptions import MissingDependencyError
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


_ARROW_STATIC_TYPE_MAP: Optional[dict] = None


def _arrow_static_type_map() -> dict:
    """
    Orso type -> PyArrow type for every type whose Arrow representation is fixed.

    Built once and reused. This used to be rebuilt for every column on every call,
    which meant a frame with N columns constructed N dicts of PyArrow type objects
    each time it was converted.
    """
    global _ARROW_STATIC_TYPE_MAP
    if _ARROW_STATIC_TYPE_MAP is None:
        import pyarrow

        # fmt: off
        _ARROW_STATIC_TYPE_MAP = {
            OrsoTypes.BOOLEAN: pyarrow.bool_(),
            OrsoTypes.BLOB: pyarrow.binary(),
            OrsoTypes.DATE: pyarrow.date32(),  # v2: was date64 (DESIGN.md §8 fix 10)
            OrsoTypes.TIMESTAMP: pyarrow.timestamp("us"),
            OrsoTypes.TIME: pyarrow.time32("ms"),
            OrsoTypes.INTERVAL: pyarrow.month_day_nano_interval(),
            OrsoTypes.DOUBLE: pyarrow.float64(),
            OrsoTypes.INTEGER: pyarrow.int64(),
            OrsoTypes.ARRAY: pyarrow.list_(pyarrow.string()),
            OrsoTypes.VARCHAR: pyarrow.string(),
            OrsoTypes.JSONB: pyarrow.binary(),
            OrsoTypes.NULL: pyarrow.null(),
        }
        # fmt: on
    return _ARROW_STATIC_TYPE_MAP


# ---------------------------------------------------------------------------------
# v2 storage (DESIGN.md §4.10, phase 4)
#
# FlatColumn and RelationSchema keep their dataclass metadata — __dataclass_fields__,
# the generated __eq__ and __repr__ — because consumers build on it: the PyPI opteryx
# planner declares `@dataclass(init=False) class ExpressionColumn(FlatColumn)`, which only
# inherits FlatColumn's fields if FlatColumn is still a dataclass, and to_dict()/to_json()
# use asdict(). What changes is underneath: every field is a hidden __slots__ member
# behind an orso._native.TrackedSlot descriptor, construction is hand-written instead of
# a loop over dataclasses.fields(), and writes to the fields that cached lookups depend
# on bump the schema epoch so RelationSchema can cache safely.
# ---------------------------------------------------------------------------------

_COLUMN_FIELDS = (
    "name", "default", "type", "element_type", "description", "disposition", "aliases",
    "nullable", "expectations", "identity", "length", "precision", "scale", "origin",
    "highest_value", "lowest_value", "null_count", "fields",
)
# writes to these invalidate schema caches (lookups, names, Arrow schemas)
_COLUMN_CACHE_FIELDS = frozenset(
    ("name", "aliases", "type", "element_type", "nullable", "precision", "scale", "fields", "length")
)
_COLUMN_SLOTS = tuple("_" + f for f in _COLUMN_FIELDS)
_NOTSET = object()


def _numpy():
    """numpy for the columns whose materialize() returns arrays (v2: optional)."""
    try:
        import numpy
    except ImportError as import_error:  # pragma: no cover
        raise MissingDependencyError(import_error.name) from import_error
    return numpy


def _install_tracked_slots(cls, field_names, bump_fields):
    """Replace each field's class attribute with a TrackedSlot over its hidden slot."""
    defaults = {f.name: f.default for f in dataclass_fields(cls)}
    for name in field_names:
        member = cls.__dict__["_" + name]
        default = defaults.get(name, _MISSING)
        if isinstance(default, _MISSING_TYPE):
            setattr(cls, name, TrackedSlot(member, name, bump=name in bump_fields))
        else:
            setattr(cls, name, TrackedSlot(member, name, default, bump=name in bump_fields))


_FIELD_SPECS: Dict[type, tuple] = {}


def _field_specs(cls) -> tuple:
    """(name, default, default_factory) for every dataclass field of a FlatColumn subclass."""
    specs = _FIELD_SPECS.get(cls)
    if specs is None:
        specs = tuple((f.name, f.default, f.default_factory) for f in dataclass_fields(cls))
        _FIELD_SPECS[cls] = specs
    return specs


def _load_expectations(value, name):
    return [
        (
            v
            if isinstance(v, Expectation)
            else (
                SchemaExpectation.load(v).update({"column": name})
                if v.get("column", _MISSING_VALUE) == _MISSING_VALUE
                else v
            )
        )
        for v in value
    ]


@dataclass(init=False)
class FlatColumn:
    """
    This is a standard column type.
    Unlike the other column types, we don't store the values for this Column
    here, we read them from the underlying data structure (orso/pyarrow/velox).
    """

    __slots__ = _COLUMN_SLOTS + ("_arrow_cache", "__weakref__")

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
    fields: Optional[List["FlatColumn"]] = None

    def __init__(self, **kwargs):
        if type(self) is not FlatColumn:
            self._init_generic(kwargs)
            return
        # The plain class: write the hidden slots directly. A new column is in no schema
        # yet, so nothing needs invalidating; unknown keywords are ignored, as in v1.
        try:
            name = kwargs["name"]
        except KeyError:
            raise ColumnDefinitionError("name") from None
        get = kwargs.get
        self._name = name
        self._default = get("default")
        column_type = get("type", OrsoTypes._MISSING_TYPE)
        self._element_type = get("element_type")
        self._description = get("description")
        self._disposition = get("disposition")
        value = get("aliases", _NOTSET)
        self._aliases = [] if value is _NOTSET else value
        self._nullable = get("nullable", True)
        value = get("expectations", _NOTSET)
        self._expectations = [] if value is _NOTSET else (_load_expectations(value, name) if value else value)
        value = get("identity", _NOTSET)
        self._identity = random_string() if value is _NOTSET else value
        self._length = get("length")
        self._precision = get("precision")
        self._scale = get("scale")
        value = get("origin", _NOTSET)
        self._origin = [] if value is _NOTSET else value
        self._highest_value = get("highest_value")
        self._lowest_value = get("lowest_value")
        self._null_count = get("null_count")
        value = get("fields")
        self._fields = [v if isinstance(v, FlatColumn) else FlatColumn(**v) for v in value] if value else value
        self._arrow_cache = None
        if column_type.__class__ is OrsoTypes and column_type is not OrsoTypes.DECIMAL and not self._default:
            self._type = column_type  # the common case: nothing left to derive
            return
        self._type = column_type
        self._finish(plain=True)

    def _init_generic(self, kwargs):
        """Subclasses (orso's own and consumers' dataclass subclasses): every dataclass
        field, the subclass's included, through ordinary attribute assignment, so a field a
        subclass re-declares (ConstantColumn.length) lands where reads will find it."""
        self._arrow_cache = None
        for fname, default, factory in _field_specs(type(self)):
            if fname in kwargs:
                value = kwargs[fname]
                if fname == "expectations" and value:
                    value = _load_expectations(value, kwargs["name"])
                elif fname == "fields" and value:
                    value = [v if isinstance(v, FlatColumn) else FlatColumn(**v) for v in value]
                setattr(self, fname, value)
            elif not isinstance(default, _MISSING_TYPE):
                setattr(self, fname, default)
            elif not isinstance(factory, _MISSING_TYPE):
                setattr(self, fname, factory())
            else:
                raise ColumnDefinitionError(fname)
        self._finish()

    def _finish(self, plain: bool = False):
        """Derive what v1 derived after assignment. A plain FlatColumn writes its hidden
        slots so building one never invalidates schema caches; subclasses go through
        their attributes, which is where re-declared fields live."""
        p = "_" if plain else ""
        column_type = getattr(self, p + "type")
        # map literals to OrsoTypes
        if column_type.__class__ is not OrsoTypes:
            column_type, _length, _precision, _scale, _element_type = OrsoTypes.from_name(column_type)
            setattr(self, p + "type", column_type)
            if column_type == OrsoTypes._MISSING_TYPE:
                warn(f"Column '{self.name}' type not recognized.")
            if isinstance(column_type, OrsoTypes):
                if getattr(self, p + "element_type") is None:
                    setattr(self, p + "element_type", _element_type)
                if getattr(self, p + "precision") is None:
                    setattr(self, p + "precision", _precision)
                if getattr(self, p + "scale") is None:
                    setattr(self, p + "scale", _scale)
                if getattr(self, p + "length") is None:
                    setattr(self, p + "length", _length)

        # if we have a default value, parse it to the correct type and fail if we can't
        default = getattr(self, p + "default")
        if default:
            try:
                setattr(self, p + "default", column_type.parse(default))
            except Exception:
                raise ValueError(
                    f"Column '{self.name}' default value not compatible with '{column_type}'."
                )

        # validate decimal properties
        if column_type == OrsoTypes.DECIMAL and getattr(self, p + "precision") is None:
            from decimal import getcontext

            setattr(self, p + "precision", getcontext().prec)
        if column_type == OrsoTypes.DECIMAL and getattr(self, p + "scale") is None:
            setattr(self, p + "scale", int(0.75 * getattr(self, p + "precision")))

    # copy, deepcopy and pickle carry the fields, never the caches
    def __getstate__(self):
        state = {}
        for slot in _COLUMN_SLOTS:
            try:
                state[slot] = object.__getattribute__(self, slot)
            except AttributeError:
                pass
        extra = getattr(self, "__dict__", None)
        if extra:
            state.update(extra)
        return state

    def __setstate__(self, state):
        for key, value in state.items():
            object.__setattr__(self, key, value)
        object.__setattr__(self, "_arrow_cache", None)

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

        # v2: a dictionary-encoded column is described by its value type (v1 raised)
        arrow_type = arrow_field.type
        while getattr(arrow_type, "value_type", None) is not None and hasattr(arrow_type, "index_type"):
            arrow_type = arrow_type.value_type

        # Fetch the native type mapping from Arrow to Python native types
        native_type = arrow_type_map(arrow_type)
        element_type = None
        struct_fields: Optional[List["FlatColumn"]] = None
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
        elif native_type == dict:
            field_type = OrsoTypes.STRUCT
            if getattr(arrow_type, "num_fields", 0):
                struct_fields = [cls.from_arrow(child) for child in arrow_type]
        elif native_type == list:
            field_type = OrsoTypes.ARRAY
            value_type = getattr(arrow_type, "value_type", None)  # a map has (key, value) pairs instead
            if value_type is not None:
                element_type = PYTHON_TO_ORSO_MAP.get(arrow_type_map(value_type))
        else:
            # Fall back to the generic mapping
            field_type = PYTHON_TO_ORSO_MAP.get(native_type, None)
            if field_type is None:
                field_type = OrsoTypes.VARCHAR

        # v1 also wrote element_type, scale and precision onto the shared OrsoTypes member
        # here, so every column of a type saw the parameters of the last one converted
        # (DESIGN.md §8 fix 2). The parameters belong to this column only.
        return FlatColumn(
            name=str(arrow_field.name),
            type=field_type,
            element_type=element_type,
            nullable=arrow_field.nullable,
            scale=scale,
            precision=precision,
            fields=struct_fields,
        )

    def to_flatcolumn(self) -> "FlatColumn":
        col = object.__new__(FlatColumn)

        col.name = str(self.name)
        col.default = self.default
        col.description = self.description
        # v1 left these unset, so the copy lost length and disposition and had no
        # expectations attribute at all
        col.disposition = self.disposition
        col.expectations = self.expectations
        col.length = self.length
        col.aliases = self.aliases
        col.identity = self.identity
        col.type = self.type
        col.element_type = self.element_type
        col.nullable = self.nullable
        col.scale = self.scale
        col.precision = self.precision
        col.lowest_value = self.lowest_value
        col.highest_value = self.highest_value
        col.null_count = self.null_count
        col.origin = self.origin
        col.fields = (
            [field.to_flatcolumn() for field in self.fields]
            if getattr(self, "fields", None)
            else None
        )
        col._arrow_cache = None

        return col

    @property
    def all_names(self):
        """simplify collection of all of the names for this column"""
        if self.aliases is not None:
            return self.aliases + [self.name]
        return [self.name]

    @property
    def arrow_field(self):
        # cached per column; any write that changes the Arrow type bumps the epoch
        try:
            cached = self._arrow_cache
        except AttributeError:
            cached = None
        epoch = schema_epoch()
        if cached is not None and cached[0] == epoch:
            return cached[1]
        result = self._build_arrow_field()
        self._arrow_cache = (epoch, result)
        return result

    def _build_arrow_field(self):
        import pyarrow

        # DECIMAL is the only type whose Arrow type depends on the column (precision
        # and scale), so it is resolved per call; everything else comes from the shared
        # cache built once per process rather than rebuilt for every column.
        type_map = _arrow_static_type_map()

        def _resolve(orso_type):
            if orso_type == OrsoTypes.DECIMAL:
                # v1 used `self.scale or 10`, so scale 0 became 10: DECIMAL(10,0) exported
                # as decimal128(10, 10), which cannot hold 1
                return pyarrow.decimal128(
                    self.precision or DECIMAL_PRECISION, 10 if self.scale is None else self.scale
                )
            return type_map.get(orso_type, pyarrow.string())

        if self.type == OrsoTypes.STRUCT:
            if self.fields:
                struct_type = pyarrow.struct([field.arrow_field for field in self.fields])
                return pyarrow.field(name=self.name, type=struct_type, nullable=self.nullable)
            return pyarrow.field(name=self.name, type=pyarrow.binary(), nullable=self.nullable)

        if self.type == OrsoTypes.ARRAY:
            return pyarrow.field(
                name=self.name,
                type=pyarrow.list_(_resolve(self.element_type)),
            )

        return pyarrow.field(name=self.name, type=_resolve(self.type))

    def to_json(self) -> str:
        def default_serializer(o):
            if isinstance(o, OrsoTypes):
                return str(o)
            if isinstance(o, Expectation):
                return o.__dict__
            raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

        return json.dumps(asdict(self), default=default_serializer)

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

        data = json.loads(json_str)
        return cls(**data)


_install_tracked_slots(FlatColumn, _COLUMN_FIELDS, _COLUMN_CACHE_FIELDS)

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
        numpy = _numpy()
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
        numpy = _numpy()
        self.values = numpy.array([self.value])

    def materialize(self):
        """
        Turn this virtual column into a list.
        When performing element-wise operations, use value_array for broadcasting.
        """
        numpy = _numpy()
        return numpy.full(self.length, self.values)


@dataclass(init=False)
class SparseColumn(FlatColumn):
    """
    This is a column type optimized for sparse data.
    Only the non-default values and their indices are stored.
    """

    values: "numpy.ndarray" = None
    default_value: Any = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        numpy = _numpy()
        (self.indices,) = numpy.where(numpy.array(self.values) != self.default_value)
        self.values = numpy.array(self.values)[self.indices]
        self.total_length = len(kwargs.get("values", []))  # Store the total length

    def materialize(self):
        """
        Materialize the sparse column into a full numpy array.
        """
        numpy = _numpy()
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

    values: "numpy.ndarray" = None
    lengths: List[int] = field(default_factory=list)

    def __init__(self, **kwargs):
        numpy = _numpy()
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
        numpy = _numpy()
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

        numpy = _numpy()
        values = numpy.asarray(self.values)
        self.values, self.encoding = numpy.unique(values, return_inverse=True)

    def materialize(self):
        """
        Turn this virtual column into a list
        """
        return self.values[self.encoding]


_SCHEMA_FIELDS = (
    "name", "aliases", "columns", "primary_key", "row_count_metric", "row_count_estimate",
    "data_size_metric", "data_size_estimate",
)
_SCHEMA_SLOTS = tuple("_" + f for f in _SCHEMA_FIELDS)


class _SchemaCache:
    """
    What a RelationSchema derives from its columns, valid while the schema epoch is
    unchanged and the columns list is the same object at the same length. Built on first
    use after any change; the expensive parts (case-folded names, the Arrow schema, the
    validation plan) only when asked for.
    """

    __slots__ = ("epoch", "columns", "ncols", "exact", "names", "all_names", "lower", "arrow", "plan")

    def __init__(self, epoch, columns):
        self.epoch = epoch
        self.columns = columns
        self.ncols = len(columns)
        exact = {}
        names = []
        all_names = []
        for column in columns:
            name = column.name
            names.append(name)
            aliases = column.aliases
            if aliases:
                for alias in aliases:
                    all_names.append(alias)
                    if alias not in exact:
                        exact[alias] = column
            all_names.append(name)
            if name not in exact:
                exact[name] = column
        self.exact = exact
        self.names = tuple(names)
        self.all_names = tuple(all_names)
        self.lower = None
        self.arrow = None
        self.plan = None  # None = not built yet, False = not usable

    def lower_map(self):
        lower = self.lower
        if lower is None:
            lower = {}
            for column in self.columns:
                for candidate in column.all_names:
                    key = candidate.lower()
                    if key not in lower:
                        lower[key] = column
            self.lower = lower
        return lower

    def validation_plan(self):
        """A native check for RelationSchema.validate's success path, or False when this
        schema needs v1's own checks (NULL-typed columns, unmapped types, non-str names)."""
        plan = self.plan
        if plan is None:
            plan = False
            specs = []
            for column in self.columns:
                if column.type == OrsoTypes.NULL or type(column.name) is not str:
                    break
                if column.type == OrsoTypes._MISSING_TYPE:
                    expected = None
                else:
                    expected = ORSO_TO_PYTHON_MAP.get(column.type, _NOTSET)
                    if expected is _NOTSET or expected is None:
                        break
                specs.append((column.name, column.nullable, expected, False))
            else:
                from orso._native import Plan
                from orso.row import Row

                try:
                    plan = Plan(Row.create_class(list(self.names)), specs, None, None)
                except (TypeError, ValueError):
                    plan = False
            self.plan = plan
        return plan


@dataclass(init=False)
class RelationSchema:
    __slots__ = _SCHEMA_SLOTS + ("_schema_cache", "__weakref__")

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

    def __init__(
        self,
        name: str,
        aliases: List[str] = _NOTSET,
        columns: List[FlatColumn] = _NOTSET,
        primary_key: Optional[str] = None,
        row_count_metric: Optional[int] = None,
        row_count_estimate: Optional[int] = None,
        data_size_metric: Optional[int] = None,
        data_size_estimate: Optional[int] = None,
    ):
        # the dataclass signature, written out: positional or keyword, list factories
        self._name = name
        self._aliases = [] if aliases is _NOTSET else aliases
        self._columns = [] if columns is _NOTSET else columns
        self._primary_key = primary_key
        self._row_count_metric = row_count_metric
        self._row_count_estimate = row_count_estimate
        self._data_size_metric = data_size_metric
        self._data_size_estimate = data_size_estimate
        self._schema_cache = None

    def __getstate__(self):
        state = {}
        for slot in _SCHEMA_SLOTS:
            try:
                state[slot] = object.__getattribute__(self, slot)
            except AttributeError:
                pass
        extra = getattr(self, "__dict__", None)
        if extra:
            state.update(extra)
        return state

    def __setstate__(self, state):
        for key, value in state.items():
            object.__setattr__(self, key, value)
        object.__setattr__(self, "_schema_cache", None)

    def _cache(self) -> _SchemaCache:
        columns = self.columns
        try:
            cache = self._schema_cache
        except AttributeError:
            cache = None
        epoch = schema_epoch()
        if (
            cache is not None
            and cache.epoch == epoch
            and cache.columns is columns
            and cache.ncols == len(columns)
        ):
            return cache
        cache = _SchemaCache(epoch, columns)
        self._schema_cache = cache
        return cache

    def __iter__(self):
        """Return an iterator over column names."""
        return iter(self._cache().names)

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
        cache = self._cache()
        if case_insensitive:
            return cache.lower_map().get(column_name.lower())
        try:
            return cache.exact.get(column_name)
        except TypeError:  # an unhashable name: v1's scan, which compares with ==
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
        return list(self._cache().all_names)

    @property
    def column_names(self) -> List[str]:
        """Return the names for columns in this relation."""
        return list(self._cache().names)

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
        # v2: a record that passes is confirmed natively (exact keys, nullability,
        # isinstance); anything else takes v1's path below, which raises v1's errors
        plan = self._cache().validation_plan()
        if plan and plan.check(data):
            return True
        return self._validate_slow(data)

    def _validate_slow(self, data: MutableMapping) -> bool:
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

    def arrow_schema(self):
        """The pyarrow schema of these columns (cached until a column changes)."""
        cache = self._cache()
        arrow = cache.arrow
        if arrow is None:
            from pyarrow import schema

            arrow = cache.arrow = schema([col.arrow_field for col in cache.columns])
        return arrow

    def to_json(self):
        return {
            "name": self.name,
            "aliases": self.aliases,
            "primary_key": self.primary_key,
            "columns": [col.to_json() for col in self.columns],
        }


_install_tracked_slots(RelationSchema, _SCHEMA_FIELDS, ())


def convert_arrow_schema_to_orso_schema(arrow_schema):
    return RelationSchema(
        name="arrow",
        columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
    )


def convert_orso_schema_to_arrow_schema(orso_schema, use_identities: bool = False):
    from pyarrow import field
    from pyarrow import schema

    if not use_identities:
        if isinstance(orso_schema, RelationSchema):
            return orso_schema.arrow_schema()
        return schema([col.arrow_field for col in orso_schema.columns])

    return schema(
        [
            field(name=col.identity, type=col.arrow_field.type, nullable=col.arrow_field.nullable)
            for col in orso_schema.columns
        ]
    )
