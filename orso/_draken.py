"""
The draken bridge (DESIGN.md §4.8, phase 7): DataFrame <-> draken Morsel.

draken is optional and never imported by `import orso`. The first call here imports it,
which loads draken_native with RTLD_GLOBAL, and binds the native bridge to that library
(orso._native.draken_bind). Without draken, both directions raise MissingDependencyError.

Columns move natively, one gather or scatter per column, wherever a draken vector can
hold the values exactly; otherwise that column goes through draken's own Python
constructors (to draken) or its to_pylist() values (from draken).
"""

from typing import List
from typing import Optional

from orso._native import draken_bind
from orso._native import draken_bound
from orso.exceptions import MissingDependencyError
from orso.types import OrsoTypes

_draken = None


def draken():
    """The draken package, imported and bound on first use."""
    global _draken
    if _draken is None:
        try:
            import draken as module
        except ImportError as err:
            raise MissingDependencyError("draken") from err
        if not draken_bound():
            draken_bind(module.draken_native.__file__)
        _draken = module
    return _draken


def _types():
    return draken().draken_native.DrakenType


# OrsoTypes -> the draken type to export a column as (DrakenType values, buffers.h).
# Types not listed are inferred from the values themselves.
def _target(column) -> tuple:
    T = _types()
    kind = getattr(column, "type", None)
    table = {
        OrsoTypes.BOOLEAN: T.BOOL,
        OrsoTypes.INTEGER: T.INT64,
        OrsoTypes.DOUBLE: T.FLOAT64,
        OrsoTypes.VARCHAR: T.VARCHAR,
        OrsoTypes.BLOB: T.VARBINARY,
        OrsoTypes.DATE: T.DATE32,
        OrsoTypes.TIMESTAMP: T.TIMESTAMP64,
        OrsoTypes.TIME: T.TIME64,
        OrsoTypes.INTERVAL: T.INTERVAL,
        OrsoTypes.DECIMAL: T.DECIMAL128,
    }
    target = table.get(kind)
    if target is None:
        return (0, 0, -1)
    if kind == OrsoTypes.DECIMAL:
        precision = column.precision or 0
        scale = column.scale if column.scale is not None else -1
        return (int(target.value), int(precision), int(scale))
    return (int(target.value), 0, -1)


# the dtype names draken's vector_from_sequence understands, for columns that cannot go
# natively (nested values, or values the store keeps as Python objects)
_SEQUENCE_DTYPE = {
    OrsoTypes.BOOLEAN: "BOOLEAN",
    OrsoTypes.INTEGER: "INTEGER",
    OrsoTypes.DOUBLE: "DOUBLE",
    OrsoTypes.VARCHAR: "VARCHAR",
    OrsoTypes.DATE: "DATE",
    OrsoTypes.TIMESTAMP: "TIMESTAMP",
    OrsoTypes.TIME: "TIME",
    OrsoTypes.ARRAY: "ARRAY",
}

_PYTHON_DTYPE = {bool: "BOOLEAN", int: "INTEGER", float: "DOUBLE", str: "VARCHAR", list: "ARRAY", tuple: "ARRAY"}


def _fallback_vector(name, column, values):
    """A draken Vector built by draken from Python values."""
    module = draken()
    kind = getattr(column, "type", None)
    dtype = _SEQUENCE_DTYPE.get(kind)
    if dtype is None:
        seen = {type(v) for v in values if v is not None}
        dtype = _PYTHON_DTYPE.get(seen.pop()) if len(seen) == 1 else None
    if dtype is None:
        raise TypeError(
            f"orso: column '{name}' holds values draken cannot put in one vector "
            f"({', '.join(sorted({type(v).__name__ for v in values if v is not None})) or 'none'})"
        )
    try:
        return module.vector_from_sequence(values, dtype)
    except (TypeError, ValueError) as err:
        raise TypeError(f"orso: draken could not build column '{name}' as {dtype}: {err}") from err


def to_draken(dataset, size: Optional[int] = None):
    """A draken Morsel of the frame's rows (the first `size` when given)."""
    module = draken()
    from orso.converters import _native_frame_for

    dataset.materialize()
    total = len(dataset)
    limit = min(size, total) if size is not None and size >= 0 else total
    names = list(dataset.column_names)
    columns = _schema_columns(dataset, names)

    frame = _native_frame_for(dataset, limit)
    if frame is None:  # rows of uneven width: build every column from values
        rows = dataset._rows[:limit]
        values = [[row[j] for row in rows] for j in range(len(names))]
        vectors = [_fallback_vector(n, c, v) for n, c, v in zip(names, columns, values)]
        return module.Morsel.from_vectors(names, vectors)

    exported = frame.export_draken([_target(c) for c in columns], limit)
    vectors = []
    for j, ((kind, vector), name, column) in enumerate(zip(exported, names, columns)):
        if kind == 0:
            vectors.append(vector)
        elif kind == 1:
            vectors.append(module.draken_native.vector_null_from_length(limit))
        else:
            vectors.append(_fallback_vector(name, column, frame.column_values(j, limit)))
    return module.Morsel.from_vectors(names, vectors)


def _schema_columns(dataset, names) -> List:
    from orso.schema import RelationSchema

    schema = dataset._schema
    if isinstance(schema, RelationSchema):
        return list(schema.columns)
    return [None] * len(names)


# ----- from draken --------------------------------------------------------------------


def _handle(vector):
    """The raw draken_native.Vector behind a draken.Vector (or the handle itself)."""
    return getattr(vector, "_nb", vector)


_UNITS = {"s": 0, "ms": 1, "us": 2, "ns": 3}


def _enum_int(value) -> int:
    """A draken logical-type field as its ordinal: enums, unit names ("us") or ints."""
    if value is None:
        return 0
    if isinstance(value, str):
        return _UNITS[value]
    return int(getattr(value, "value", value))


def _column_for(name: str, handle):
    from orso.schema import FlatColumn

    T = _types()
    t = handle.type
    simple = {
        T.BOOL: OrsoTypes.BOOLEAN,
        T.INT8: OrsoTypes.INTEGER,
        T.INT16: OrsoTypes.INTEGER,
        T.INT32: OrsoTypes.INTEGER,
        T.INT64: OrsoTypes.INTEGER,
        T.UINT8: OrsoTypes.INTEGER,
        T.UINT16: OrsoTypes.INTEGER,
        T.UINT32: OrsoTypes.INTEGER,
        T.UINT64: OrsoTypes.INTEGER,
        T.FLOAT32: OrsoTypes.DOUBLE,
        T.FLOAT64: OrsoTypes.DOUBLE,
        T.VARCHAR: OrsoTypes.VARCHAR,
        T.NVARCHAR: OrsoTypes.VARCHAR,
        T.VARIANT: OrsoTypes.VARCHAR,
        T.VARBINARY: OrsoTypes.BLOB,
        T.DATE32: OrsoTypes.DATE,
        T.TIMESTAMP64: OrsoTypes.TIMESTAMP,
        T.TIME32: OrsoTypes.TIME,
        T.TIME64: OrsoTypes.TIME,
        T.INTERVAL: OrsoTypes.INTERVAL,
        T.ARRAY: OrsoTypes.ARRAY,
        T.NULL: OrsoTypes.NULL,
    }
    if t in (T.DECIMAL, T.DECIMAL128):
        return FlatColumn(
            name=name,
            type=OrsoTypes.DECIMAL,
            precision=int(handle.logical_type_precision or 38),
            scale=int(handle.logical_type_scale or 0),
        )
    kind = simple.get(t)
    if kind is None or (t == T.UINT32 and _enum_int(handle.logical_type_kind) != 0):
        return FlatColumn(name=name)  # UUID, fp16 vectors, IPv4: typed by their values
    return FlatColumn(name=name, type=kind)


def _morsels(source):
    module = draken()
    if isinstance(source, module.Morsel):
        return [source]
    morsels = list(source)
    if not all(isinstance(m, module.Morsel) for m in morsels):
        raise TypeError("orso: from_draken takes a draken Morsel or a list of them")
    return morsels


def from_draken(cls, source):
    """A DataFrame of the rows of a draken Morsel, or of a list of Morsels with the same
    columns, imported into the native record store."""
    from orso._native import Frame
    from orso.schema import RelationSchema

    morsels = _morsels(source)
    if not morsels:
        return cls(rows=[], schema=[])
    first = morsels[0]
    names = [n.decode() if isinstance(n, bytes) else n for n in first.column_names]
    handles = [_handle(first.column(n)) for n in names]
    schema = RelationSchema(name="draken", columns=[_column_for(n, h) for n, h in zip(names, handles)])
    df = cls(rows=[], schema=schema)
    frame = Frame(df._get_plan())
    for morsel in morsels:
        these = [n.decode() if isinstance(n, bytes) else n for n in morsel.column_names]
        if these != names:
            raise ValueError("orso: from_draken needs every Morsel to have the same columns")
        sources = []
        for n in names:
            h = _handle(morsel.column(n))
            sources.append(
                (h, _enum_int(h.logical_type_kind), _enum_int(h.logical_type_unit), int(h.logical_type_scale or 0))
            )
        frame.import_draken(sources, morsel.num_rows)
    df._frame = frame
    df._py_rows = None
    df._cursor = frame.cursor()
    return df
