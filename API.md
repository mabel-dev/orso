# orso API contract

This is the public surface a rewrite of orso must preserve. The implementation
underneath (row storage, Cython, serialisation internals) is free to change; the
names, signatures, return types, exceptions, laziness rules and byte formats listed
here are not.

It was compiled from `orso` 0.0.246 by introspection, cross-checked against the two
real consumers (mabel, and the PyPI `opteryx` 0.26.x checkout) and the test suite.
`opteryx-core` does not use orso at all; it carries its own DataFrame and schema types.

Each item carries a tier:

| tier | meaning |
|---|---|
| **1** | called by a consumer today; breaking it breaks a product |
| **2** | public and tested, no external caller found; keep, but it is not on the hot path |
| **3** | public but dead (NotImplemented, or nothing calls or tests it); drop or keep, free choice |

Where the current behaviour is a wart that consumers work around, it is called out
under *Known warts*. A rewrite may fix those; it must not silently change anything else.

---

## 1. Package

```python
import orso
orso.DataFrame        # tier 1
orso.Row              # tier 1
orso.__version__      # tier 1  (str, "0.0.246")
orso.__all__ == ["DataFrame", "Row", "__version__"]
```

Submodules that consumers import directly: `orso.dataframe`, `orso.schema`,
`orso.types`, `orso.tools`, `orso.logging`, `orso.cityhash`, `orso.faker`,
`orso.converters`, `orso.exceptions`.

Hard dependencies today: `numpy`, `pyarrow`, `ormsgpack`, `xxhash`, `data-expectations`,
`pandas` (listed, barely used). A rewrite can shed any of these as long as `pyarrow`
interop still works when pyarrow is installed, and its absence raises
`MissingDependencyError` rather than `ImportError` at call time.

---

## 2. `orso.DataFrame` (tier 1)

A container of rows that share one schema. Rows are `Row` objects (tuple subclass)
or, when constructed from `rows=`, whatever the caller passed in (plain tuples are
accepted and must keep working for everything except `nbytes()`, see warts).

### Construction

```python
DataFrame(dictionaries: Iterable[dict] | None = None,
          *, rows: Iterable[tuple] | None = None,
          schema: RelationSchema | list[str] | tuple[str] | None = None)
```

- `DataFrame(dicts)`: schema is the key order of the **first** dict; later dicts are
  read by key (`dict.get`), missing keys become `None`, extra keys are ignored, no type
  enforcement. Column names are `str(key)`.
- `DataFrame(rows=..., schema=...)`: `rows` may be a list, tuple, generator or any
  iterable; a generator makes the frame **lazy**. `rows=None` means empty.
  `schema` may be a `RelationSchema` or a list/tuple of names (coerced with `str`).
- `DataFrame(schema=s)` with no rows is the writer idiom (mabel: `DataFrame(rows=[], schema=self.schema)`).
- Passing `dictionaries` together with `rows` or `schema` raises `ValueError`.
- `DataFrame.from_arrow(tables)` (classmethod): accepts one `pyarrow.Table`, a list or
  tuple of Tables, or a generator of Tables. Returns a **lazy** frame whose schema is a
  `RelationSchema` named `"arrow"` built with `FlatColumn.from_arrow` per field.
  Zero tables (or `None` first table) gives an empty frame with an empty schema.
  In v2 a Table, RecordBatch or list of them is imported eagerly into a native store frame
  (no Python objects until rows are read); a generator of tables is still read lazily.

### Attributes and properties

| name | type | notes |
|---|---|---|
| `column_names` | `tuple[str, ...]` | from the list, or `FlatColumn.name` in order; cached per frame |
| `columncount` | `int` | |
| `rowcount` | `int` | materialises |
| `shape` | `(rowcount, columncount)` | materialises |
| `schema` | `RelationSchema \| list[str]` | **returns exactly what was passed in**, not normalised; opteryx reads `.schema.columns` and mabel checks `isinstance(schema, RelationSchema)` |
| `description` | `list[tuple]` | DB-API 2.0 7-tuples `(name, type_code, None, None, precision, scale, null_ok)`; `type_code` is the `OrsoTypes` value string, `"DECIMAL(p,s)"` for decimals, `"ARRAY<T>"` for typed arrays, `None` for list-of-names schemas |
| `arraysize` | `int`, default 100 | DB-API; default size for `fetchmany()` |

### Materialisation rules

Frames built from a generator (including everything from `from_arrow`) are lazy.
`materialize()` turns `_rows` into a list exactly once and is idempotent. These call it
implicitly: `len()`, `rowcount`, `shape`, `nbytes()`, `row()`, `slice()`, `head()`,
`tail()`, `collect()`, `__getitem__`, `arrow()`, `to_batches()`, `display()`,
`markdown()`, `str()`. These do **not**: `__iter__`, `query()`, `select()`, `filter()`,
`take()`, `distinct()`, `fetch*()`, `group_by()`. Iterating a lazy frame consumes it.

### Appending

```python
df.append(entry: dict | tuple | list) -> None
```

- Schema is a `RelationSchema`: `entry` must be a mapping. It is validated (same rules as
  `RelationSchema.validate`, same exceptions). Values for `JSONB` columns that are
  `dict`/`list`/`tuple`/`set` are serialised to JSON `bytes` before storage; `bytes` are
  stored as-is. The row is stored in **schema column order** regardless of dict key
  order. Tuples are rejected (`TypeError` from validate).
- Schema is a list of names: `entry` may be a dict (fields pulled by name, missing →
  `None`) or a tuple/list (stored positionally). v1 did no length check; v2 raises
  `ValueError` when a row appended to an empty frame has the wrong number of values,
  because the record store has a fixed width. Appending to a frame that was given Python
  rows keeps v1's behaviour.
- After `append`, the fetch cursor is invalid: any `fetchone/fetchmany/fetchall` raises
  `Exception("Cannot use fetchone and append on the same DataFrame")`.
- Appending does not size the row; `nbytes()` picks it up later.

### Reading rows

| call | contract |
|---|---|
| `for row in df` | yields rows in order; does not materialise |
| `len(df)` | row count (materialises) |
| `df.row(i)` | i-th row, negative indexes allowed (materialises) |
| `df[cols]` | alias for `collect(cols)` |
| `df.collect(columns, limit=None)` | `columns` is an int, a name, or a list of either; returns one list for a single column, a tuple of lists for several; `limit` caps rows; `IndexError` on bad index, `ValueError` on unknown name (from `tuple.index`) |
| `df.fetchone()` | next row or `None` when exhausted |
| `df.fetchmany(size=None)` | up to `size` rows (`arraysize` when `None`); `[]` when exhausted |
| `df.fetchall()` | remaining rows as a list; `[]` on a second call |
| `df.to_batches(batch_size=1000)` | generator of `DataFrame`s sharing the schema object |
| `df.nbytes()` | approximate serialised size: sum over rows of `len(msgpack(row)) + 14`; cached per row and incrementally per frame so polling it during a build stays linear |

The three `fetch*` methods share one cursor that starts at row 0 on construction.

### Deriving frames (all return a new `DataFrame` with the same `schema` object)

| call | contract |
|---|---|
| `slice(offset=0, length=None)` | list slice; negative offset counts from the end; `length=0` → empty; materialises |
| `head(size=5)` / `tail(size=5)` | `slice(0, size)` / last `size` rows |
| `query(predicate)` | rows where `predicate(row)` is truthy; eager |
| `select(attributes)` | column projection by **name**; result schema is the list of names given (not a `RelationSchema`); lazy |
| `filter(mask)` | rows where the parallel boolean iterable is truthy; lazy |
| `take(indexes)` | rows whose position is in `indexes`; lazy |
| `distinct()` | first occurrence of each distinct row (by `hash(row)`); eager |
| `df1 + df2` | concatenation; `ValueError("Schemas must be identical to add DataFrames")` unless `schema == other.schema` |
| `group_by(columns)` | returns `GroupBy`, see §7 |

### Conversion

| call | contract |
|---|---|
| `arrow(size=None)` | `pyarrow.Table`. With a `RelationSchema` the Arrow schema is `convert_orso_schema_to_arrow_schema(schema)` (types in §5.4), **unless** any `STRUCT` column has no `fields`, in which case types are inferred by pyarrow. With a list schema, types are inferred. `size` caps rows; `size=0` gives an empty table with the right columns. Materialises. |
| `pandas(size=None)` | `arrow(size).to_pandas()` (tier 2) |
| `polars(size=None)` | `polars.DataFrame` built from `row.as_dict` (tier 2) |
| `to_draken(size=None)` | **v2, new.** A draken `Morsel` (draken ships in opteryx-core; `MissingDependencyError` without it). Columns go natively where a draken vector holds them exactly: with a `RelationSchema` the vector type follows §5.4's column types (BOOLEAN→BOOL, INTEGER→INT64, DOUBLE→FLOAT64, VARCHAR→VARCHAR, BLOB→VARBINARY, DATE→DATE32, TIMESTAMP→TIMESTAMP64[us], TIME→TIME64[us], INTERVAL→INTERVAL, DECIMAL→DECIMAL128(p or 38, s)); otherwise it is inferred from the values (int and float mixed → FLOAT64, all null → NULL). Nested values and objects go through draken's `vector_from_sequence`; a column draken cannot type raises `TypeError` naming it. Vectors own copies of the data. |
| `DataFrame.from_draken(morsels)` (classmethod) | **v2, new.** A store frame of one draken `Morsel` or a list of Morsels with the same columns; `RelationSchema` named `"draken"` from the vector types. Values are what `from_arrow(morsel.to_arrow())` gives (naive timestamps, `Decimal`, `bytes` for VARBINARY), except types with no orso slot (arrays, UUIDs, fp16 vectors, IPv4, intervals with months, UINT64 above 2^63-1), which take draken's own `to_pylist()` values. |

### Rendering

| call | contract |
|---|---|
| `display(limit=5, display_width=True, max_column_width=32, colorize=True, show_types=True) -> str` | ASCII table followed by `"\n[ {n} rows x {m} columns ]"`; `display_width=True` uses the terminal width, an int is a character budget, `False` means unlimited |
| `markdown(limit=5, max_column_width=30) -> str` | pipe table with a `#` index column; `limit <= 0` means **all rows** (opteryx `--format markdown` passes `-1`) |
| `str(df)` | 10-row top-and-tail ASCII table plus the `[ rows x columns ]` footer; in a notebook, renders an HTML table via IPython and returns `""` |
| `repr(df)` | `"<orso.dataframe>"` outside notebooks |

### Hashing and equality

- `hash(df)`: content hash, order-sensitive, equal for equal content (used by tests, not
  by consumers). The exact algorithm (`xxh64(str(row))` folded with the row index) is
  **not** a contract; equality of hashes for equal frames is.
- No `__eq__` is defined; identity equality. Keep it that way.

### Private attributes consumers touch

The PyPI opteryx cursor reads `df._rows` and `df._schema` directly. A rewrite must keep
a `_rows` attribute that is a list after `materialize()` and a `_schema` attribute that
is the schema object as passed in.

---

## 3. `orso.Row` (tier 1)

A `tuple` subclass. Rows compare, hash, index, slice, unpack and copy exactly like
tuples. In v2 rows have no `__dict__` (`__slots__ = ()` on every subclass); v1's
`create_class` subclasses accidentally carried one, which `nbytes()` and `as_map` used
as a cache. Nothing outside orso set attributes on rows.

```python
Row.create_class(schema: RelationSchema | tuple[str] | list[str], tuples_only=False) -> type[Row]
```

Returns a `Row` subclass bound to a field list. The result is cached per `(fields,
tuples_only)` and the same class object comes back for the same input (consumers rely
on `isinstance(row, Row)`, never on the subclass identity). `tuples_only=True` is a
promise that the factory is only ever given sequences, never dicts.

Instances are made by calling the subclass:

- with a sequence: stored positionally, no length check
- with a dict: fields pulled by name, missing → `None`, extra ignored
- `Row(...)` on the base class is allowed (fields are `None`) and only `as_bytes`
  works meaningfully on it

| member | kind | contract |
|---|---|---|
| `as_dict` | **property** | `{field: value}` in field order |
| `as_map` | property | `tuple((field, value), ...)` (v1 cached it per row; v2 rows have nowhere to cache) |
| `as_json` | property | `bytes`; `json.dumps(as_dict, default=str, separators=(",", ":"))` → compact, no spaces, non-JSON values stringified |
| `as_bytes` | property | the record format in §3.1; raises `DataError` if the payload exceeds 16 MiB |
| `from_bytes(data)` | classmethod | inverse of `as_bytes`; `DataError("Data malformed")` on a bad header, `DataError("Data malformed - incorrect length")` on a length mismatch |
| `nbytes()` | method | `len(msgpack payload) + 14`; cached on the instance; must **not** raise on oversize rows |
| `get(name, default=None)` | method | value by field name or `default` |
| `keys()` | method | the field tuple |
| `values` | property | `tuple(self)` |

`as_dict`, `as_map`, `as_json` and `as_bytes` are properties, not methods. Note that
mabel's own row-like objects expose `as_dict()` as a method; that is mabel's class, not
orso's, and the two must not be confused.

### 3.1 Record byte format (tier 2: tested, no consumer found, keep byte-identical)

```
offset  size  field
0       2     header prefix  0x10 0x00   (high nibble of byte 0 is a version; from_bytes only checks it is 0x1)
2       4     payload length, big-endian unsigned
6       8     timestamp, time.time_ns(), big-endian unsigned
14      n     payload: msgpack (ormsgpack) of tuple(row)
```

Payload encoding rules: `numpy.datetime64` → `["__datetime__", seconds]` (NaT → nil),
`datetime.datetime` → `["__datetime__", timestamp()]`, `numpy.ndarray` → list, numpy
scalars natively, anything else unknown → `str(value)`. Decoding turns any 2-element
list whose first item is `"__datetime__"` back into a naive local `datetime`.
`MAXIMUM_RECORD_SIZE = 16 * 1024 * 1024` is importable from `orso.row`.

### Module-level extras in `orso.row`

`extract_columns(table: list[dict], columns: list[str]) -> tuple[list, ...]` (tier 2,
tested). `HEADER_PREFIX`, `HEADER_SIZE`, `RECORD_HEADER_SIZE` constants (tier 2).

---

## 4. `orso.schema`

### 4.1 `RelationSchema` (tier 1, dataclass with `__eq__`)

```python
RelationSchema(name: str, aliases: list[str] = [], columns: list[FlatColumn] = [],
               primary_key: str | None = None,
               row_count_metric: int | None = None, row_count_estimate: int | None = None,
               data_size_metric: int | None = None, data_size_estimate: int | None = None)
```

All fields are public, mutable attributes; opteryx assigns `schema.columns = [...]`
and reads `row_count_metric`/`row_count_estimate` freely.

| member | contract |
|---|---|
| `iter(schema)` | yields column **names** |
| `schema + other` | new schema with `self.name`/`aliases`, columns of both, de-duplicated by `identity`, left wins |
| `num_columns` (property) | `len(columns)` |
| `column_names` (property) | `[col.name, ...]` as a list |
| `all_column_names()` | names and aliases flattened, in column order |
| `find_column(name, case_insensitive=False)` | first column whose `name` **or an alias** matches; `None` if none |
| `column(i)` | by int index or by `find_column` |
| `pop_column(name)` | removes and returns the first column with that exact `name`, else `None` |
| `validate(record)` | see below |
| `to_dict()` | `dataclasses.asdict` with enum values flattened to their `.value` |
| `from_dict(d)` | needs `name` and `columns`; a column given as a `str` becomes an untyped `FlatColumn(name=...)`, a dict is `FlatColumn(**dict)` |
| `to_json()` | **returns a dict**, not a string, with keys `name`, `aliases`, `primary_key`, `columns` (each `FlatColumn.to_json()` string) |

`validate(record)`:

1. `TypeError("Cannot validate non Dictionary-type value")` if `record` is not a `MutableMapping`.
2. `ExcessColumnsInDataError(columns=set_of_extra_names)` if the record has keys not in the schema.
3. Otherwise collects errors into a `dict[str, list]` under exactly these keys:
   `"Column in Schema Not Found in Record"` (missing key), `"Column not Nullable"` (value is
   `None` and `nullable=False`), `"Incorrect Type"` (entries are `(name, value, type)` where
   `isinstance(value, ORSO_TO_PYTHON_MAP[type])` fails; columns with the missing type are
   not type-checked). Raises `DataValidationError(errors=...)` if any; returns `True` otherwise.
   Note `bool` passes an `INTEGER` check (it is an `int`), and `JSONB` expects `bytes`.

### 4.2 `FlatColumn` (tier 1, dataclass with `__eq__` over all fields)

v2: still a dataclass in metadata (`dataclasses.fields`, `asdict`, the generated `__eq__`
and `__repr__`, and subclassing with `@dataclass(init=False)` all work as in v1), but
fields live in `__slots__`, so instances have no `__dict__`. `to_flatcolumn()` now keeps
every field (v1 lost `length`, `disposition` and `expectations`).

Keyword-only construction. Every field is a public attribute.

| field | default | notes |
|---|---|---|
| `name` | required (`ColumnDefinitionError("name")` if missing) | |
| `type` | `OrsoTypes._MISSING_TYPE` | accepts an `OrsoTypes` or any string the grammar in §5.2 accepts; strings fill `length`, `precision`, `scale`, `element_type` when those are `None`; an unrecognised name warns and leaves the missing type |
| `element_type` | `None` | for `ARRAY` |
| `description`, `disposition`, `aliases` (`[]`), `expectations` (`[]`), `origin` (`[]`) | | `expectations` dicts are loaded via `SchemaExpectation.load` and stamped with the column name |
| `nullable` | `True` | |
| `identity` | `random_string(16)` | a fresh random id per column unless supplied; opteryx keys dictionaries on it and relies on it being unique and stable for the column's lifetime |
| `default` | `None` | parsed with `type.parse` at construction; `ValueError` if it cannot be |
| `length`, `precision`, `scale` | `None` | `DECIMAL` with no precision gets `decimal.getcontext().prec` (28) and scale `int(0.75 * precision)` |
| `highest_value`, `lowest_value`, `null_count` | `None` | statistics, read and written by opteryx |
| `fields` | `None` | child `FlatColumn`s for `STRUCT`; dicts are promoted |

| member | contract |
|---|---|
| `str(col)` | the `identity` |
| `all_names` (property) | `aliases + [name]` |
| `arrow_field` (property) | `pyarrow.Field(name, type)` using §5.4; `nullable` is only passed through for `STRUCT` |
| `from_arrow(field, mappable_as_binary=False)` (classmethod) | name, type via §5.5, `nullable`, decimal precision/scale, `element_type` for lists, child `fields` for structs; `mappable_as_binary=True` maps structs to `BLOB` instead of `STRUCT` |
| `to_flatcolumn()` | shallow copy as a plain `FlatColumn` (used to strip subclasses), keeping `identity` |
| `to_json()` | JSON **string** of `asdict(self)` with `OrsoTypes` as their names and expectations as dicts |
| `from_json(s)` | inverse; unknown type names become the missing type |
| `materialize()` | raises `TypeError` on a plain `FlatColumn` |

Subclasses (all tier 1 for opteryx except where noted): `FunctionColumn` (`binding`,
`configuration`, `length`, `materialize()` → numpy array of the bound value repeated),
`ConstantColumn` (`value`, `length`, `values`, `materialize()` → `numpy.full`),
`SparseColumn`, `RLEColumn`, `DictionaryColumn` (tier 2, tests only). Each subclass
compares equal only to its own kind.

`ColumnDisposition` enum: `NAME = "name"`, `AGE = "age"` (tier 1, used by the faker and opteryx).

`SchemaExpectation` wraps `data_expectations.Expectation` with a `load()` that accepts a
dict or JSON string (tier 2, mabel uses the `expectations` list on columns).

### 4.3 Module functions

| function | tier | contract |
|---|---|---|
| `convert_orso_schema_to_arrow_schema(schema, use_identities=False)` | 1 | `pyarrow.schema([col.arrow_field ...])`; with `use_identities` the field names are the column identities |
| `convert_arrow_schema_to_orso_schema(arrow_schema)` | 2 | `RelationSchema(name="arrow", columns=[FlatColumn.from_arrow(f) ...])` |
| `DECIMAL_PRECISION` | 2 | `decimal.getcontext().prec` at import |

---

## 5. `orso.types`

### 5.1 `OrsoTypes` (tier 1, `str` + `Enum`)

Members: `ARRAY, BLOB, BOOLEAN, DATE, DECIMAL, DOUBLE, INTEGER, INTERVAL, JSONB, NULL,
STRUCT, TIME, TIMESTAMP, VARCHAR, VECTOR, _MISSING_TYPE`. Values are the member names
except `_MISSING_TYPE`, whose value is `"0"`. `str(t)` is the value, so `str(OrsoTypes.INTEGER) == "INTEGER"`;
`OrsoTypes.INTEGER == "INTEGER"` is `True` (str enum). `OrsoTypes["VARCHAR"]` and
`OrsoTypes("VARCHAR")` both work. `OrsoTypes.__members__` is iterated by consumers.

| member | contract |
|---|---|
| `is_numeric()` | `INTEGER, DOUBLE, DECIMAL` |
| `is_temporal()` | `DATE, TIME, TIMESTAMP` (not `INTERVAL`) |
| `is_large_object()` | `VARCHAR, BLOB` |
| `is_complex()` | `ARRAY, STRUCT, VECTOR, JSONB` |
| `python_type` (property) | §5.3 |
| `numpy_dtype` (property) | `ARRAY/DECIMAL/STRUCT/TIME/NULL/JSONB/VECTOR → O`, `BLOB → S`, `BOOLEAN → ?`, `DATE → datetime64[D]`, `DOUBLE → float64`, `INTEGER → int64`, `INTERVAL → m`, `TIMESTAMP → datetime64[us]`, `VARCHAR → U` |
| `metadata` (property) | documentation dict with `description`, `example`, optional `min`/`max`/`notes` |
| `parse(value, **kwargs)` | `None` → `None`; else the per-type parser in §5.3 with `length`, `precision`, `scale`, `element_type` kwargs |
| `from_name(name)` (static) | `(type, length, precision, scale, element_type)` per §5.2 |

### 5.2 Type-name grammar (`from_name`, `get_orso_type`, `FlatColumn(type="...")`)

Case-insensitive. Accepted forms and what they yield:

| input | result |
|---|---|
| any member name, e.g. `"integer"` | that member |
| `None`, `"0"`, `0`, `"VARIANT"`, `"MISSING"` | `_MISSING_TYPE` (returned as the literal `0` by `from_name`; `FlatColumn` then warns) |
| `"VARCHAR[n]"`, `"BLOB[n]"`, `"VARBINARY[n]"`, `"VECTOR[n]"` | the type with `length=n` |
| `"DECIMAL(p,s)"` | `DECIMAL` with precision and scale; `ValueError` unless `0 <= s <= p <= 38` |
| `"ARRAY<T>"` | `ARRAY` with `element_type=T`; `T` must be a member and not `ARRAY/LIST/NUMERIC/BSON/STRING/DECIMAL/VECTOR` |
| `"ARRAY"` | `ARRAY<VARCHAR>` with a warning |
| `"LIST"` | `ARRAY` with a deprecation warning |
| `"NUMERIC"` | `DOUBLE` with a deprecation warning |
| `"BSON"` | `JSONB` with a deprecation warning |
| `"STRING"` | `ValueError` suggesting `VARCHAR` |
| anything else | `ValueError("Unknown column type ...")` |

`get_orso_type(s)` (tier 1) returns the member only, with the parsed parameters stashed
on it (see warts).

### 5.3 Python mapping and parsers

`ORSO_TO_PYTHON_MAP` (tier 1, imported by opteryx as `PYTHON_TO_ORSO_MAP`'s inverse):

| type | python | parser (`parse`) |
|---|---|---|
| `BOOLEAN` | `bool` | upper-cased str/bytes in `("TRUE","ON","YES","1","1.0","T","Y")`; anything else is `str()`-ed first |
| `BLOB` | `bytes` | dict/list/tuple/set → JSON bytes; bytes as-is; else `str(x).encode()`; truncated to `length` |
| `DATE` | `datetime.date` | `parse_iso(x).date()`, `ValueError("Invalid date.")` |
| `TIMESTAMP` | `datetime.datetime` | `parse_iso(x)`, `ValueError("Invalid timestamp.")` |
| `TIME` | `datetime.time` | `parse_iso(x).time()` |
| `INTERVAL` | `datetime.timedelta` | `timedelta(x)` |
| `STRUCT` | `dict` | same as `BLOB` |
| `DECIMAL` | `decimal.Decimal` | float → `format(v, ".99g")`, int → `str`, bytes → utf-8, `.as_py()`/`.item()` unwrapped, stripped; result is a `DecimalFactory` with precision (default 38) and scale (default 21) |
| `DOUBLE` | `float` | `float(x)` |
| `INTEGER` | `int` | `int(x)` |
| `ARRAY` | `list` | non-sequence input is JSON-decoded; elements parsed with `element_type.parse` when given |
| `VARCHAR` | `str` | `BLOB` parser then utf-8 decode |
| `JSONB` | `bytes` | same as `BLOB` |
| `NULL` | `None` | `None` |
| `VECTOR` | `list` | JSON-decode if needed, `float()` each element |

`PYTHON_TO_ORSO_MAP` is the inverse minus `JSONB`, plus `tuple → ARRAY`, `set → ARRAY`.
The parser functions are importable by name (`parse_boolean`, `parse_decimal`, ...).
`find_compatible_type(types, default=VARCHAR)` (tier 1): empty → `NULL`; all the same →
that type; all numeric, all temporal, all large-object or all complex → the widest of
that category (`BOOLEAN < INTEGER < DOUBLE < DECIMAL`, `DATE < TIMESTAMP`, `BLOB < VARCHAR`, `VECTOR < ARRAY`);
any mix drawn from `BLOB/STRUCT/JSONB/VARCHAR` → `BLOB`; any other mix containing
`BLOB` → `BLOB`; otherwise `default`.

### 5.4 orso → Arrow (used by `arrow_field`, `arrow()`)

| orso | arrow |
|---|---|
| `BOOLEAN` | `bool_` |
| `BLOB`, `JSONB` | `binary` |
| `DATE` | `date32` (v1: `date64`; changed in v2, DESIGN.md §8 fix 10) |
| `TIMESTAMP` | `timestamp("us")` |
| `TIME` | `time32("ms")` |
| `INTERVAL` | `month_day_nano_interval` |
| `DOUBLE` | `float64` |
| `INTEGER` | `int64` |
| `DECIMAL` | `decimal128(precision or 28, scale or 10)` |
| `VARCHAR` | `string` |
| `ARRAY` | `list_(<element_type mapped>)`, `list_(string)` with no element type |
| `STRUCT` with `fields` | `struct([...child arrow_fields])` |
| `STRUCT` without fields | `binary` |
| `NULL` | `null` |
| `_MISSING_TYPE`, `VECTOR`, anything else | `string` |

### 5.5 Arrow → orso (used by `FlatColumn.from_arrow`, `tools.arrow_type_map`)

`NA → None`, `BOOL → bool`, all `INT*/UINT*` → `int`, `HALF_FLOAT/FLOAT/DOUBLE → float`,
`STRING/LARGE_STRING/STRING_VIEW → str`, `DATE32 → date`, `DATE64 → date` (v1: `datetime`),
`TIME32/TIME64 → time`, `INTERVAL_MONTH_DAY_NANO/DURATION → timedelta`, id 18 (timestamp)
→ `datetime`, `LIST/LARGE_LIST/FIXED_SIZE_LIST → list`, `STRUCT/MAP → dict`,
`BINARY/LARGE_BINARY → bytes`, `DECIMAL128/256 → DecimalFactory(precision, scale)`,
anything else `ValueError`. The Python type then goes through `PYTHON_TO_ORSO_MAP`;
unmapped (including `None`) becomes `VARCHAR`.

---

## 6. `orso.converters` (module functions)

| function | tier | contract |
|---|---|---|
| `to_arrow(dataset, size=None)` | 1 | what `DataFrame.arrow` calls |
| `from_arrow(tables, size=None)` | 1 | returns `(rows_iterator, RelationSchema)`; `size` caps the rows yielded |
| `to_pandas`, `to_polars` | 2 | |
| `from_pandas`, `from_polars`, `to_csv`, `from_csv`, `to_parquet`, `from_parquet`, `to_jsonl`, `from_jsonl` | 3 | all raise `NotImplementedError`; free to remove |

---

## 7. `orso.group_by` (tier 1, both consumers call `group_by`)

`DataFrame.group_by(columns)` returns a `GroupBy(dataset, columns)` where `columns` is a
name or list of names. Methods, each returning a `DataFrame` whose schema is a list of
names:

| call | output columns |
|---|---|
| `count()` | group columns + `"COUNT(*)"` |
| `min(cols)`, `max(cols)`, `sum(cols)`, `avg(cols)` | group columns + `"MIN(col)"` etc. per column |
| `aggregate([(col, fn), ...])` | group columns + `"FN(col)"` with `fn` a name in `AGGREGATORS` (`MIN, MAX, COUNT, AVG, SUM`) or a callable |
| `groups()` | the distinct group keys |

Evaluation is single-pass over the source rows so generator-backed frames work.
`TooManyGroups` is importable but nothing raises it today. `AGGREGATORS`, `min_agg`,
`max_agg`, `count_agg`, `avg_agg`, `sum_agg` are importable.

---

## 8. `orso.display` (tier 1 via `DataFrame.display`/`markdown`/`str`; mabel imports `ascii_table`)

```python
ascii_table(table, limit=5, display_width=True, max_column_width=30, colorize=True,
            top_and_tail=True, show_types=False, return_row_count=False)
markdown(table, limit=5, max_column_width=30)   # generator of lines
html_table(dictset, limit=5)                     # tier 2
colorizer(record, can_colorize=True)             # tier 2
COLORS                                           # tier 2
```

`ascii_table` takes any `DataFrame`; with `return_row_count=True` it returns
`(text, rows_shown)`. Column widths are measured over the rows shown, not the whole
frame. Exact glyphs, colour codes and type-formatting rules are implementation detail
except that the output must stay a valid, aligned box-drawing table that the existing
display tests accept.

---

## 9. `orso.tools`

| name | tier | contract |
|---|---|---|
| `random_string(width=16)` | 1 | lowercase hex string of exactly `width` characters (v1 could return fewer), called for every `FlatColumn` identity and all over opteryx |
| `random_int()` | 1 | random unsigned 32-bit int |
| `single_item_cache(func=None, *, valid_for_seconds=inf)` | 1 | memoises the most recent `(args, kwargs)` result; usable bare or with arguments; stacks under `@property` |
| `lru_cache_with_expiry(func=None, *, max_size=5, valid_for_seconds=inf)` | 2 | |
| `retry(max_tries=3, backoff_seconds=1, exponential_backoff=False, max_backoff=4, retry_exceptions=(Exception,), jitter=False, callback=None)` | 2 | decorator |
| `parse_iso(value)` | 1 (behind every temporal `parse`); **v2 applies offsets (result is naive UTC), refuses (`None`) an offset it cannot apply such as hours-only `+01`, and keeps fractional seconds and a `datetime`'s microseconds; v1 dropped all of these. Matches draken's parser and opteryx-core's `CAST`** | fast ISO-8601 parser, not a full one: `YYYY-MM-DD`, `YYYY-MM-DD[ T]HH:MM[:SS[.ffffff]]`, optional trailing `Z` or `+offset` (dropped, no conversion); digit strings and numbers are epoch seconds (UTC, tz stripped); numpy `datetime64`, pandas timestamps, `date` and `datetime` pass through with microseconds zeroed; returns naive `datetime` or `None` |
| `DecimalFactory` | 2 | `Decimal` subclass carrying `precision`/`scale`, `new_factory(precision, scale)` |
| `arrow_type_map(arrow_type)` | 2 | §5.5 |
| `timed`, `log`, `counter`, `throttle`, `monitor`, `repeat`, `islice` | 3 | decorators and helpers nothing imports |

---

## 10. `orso.logging` (tier 1, mabel)

`get_logger() -> logging.Logger`, `set_log_name(name)`, `LEVELS` enum, `LEVELS_TO_STRING`.
The logger adds an `ALERT` level and a Google Cloud formatter. Behaviour beyond "returns
a configured stdlib logger" is not relied on.

## 11. `orso.cityhash` (tier 1, mabel)

`CityHash32(data) -> int`, `CityHash64(data) -> int`. Despite the names these are
**xxhash** (`xxh32`/`xxh64` int digests), not CityHash. Non-bytes input is
`str(data).encode()` first, so mabel can pass strings. mabel persists the 64-bit values
as hashes of records, so the digests must stay xxh64-identical.

## 12. `orso.faker` (tier 1, opteryx `$fake` dataset)

`generate_fake_data(schema: RelationSchema, size=100) -> DataFrame`,
`generate_random_row(schema) -> tuple`. Honour `type`, `nullable`, `disposition`
(`NAME`/`AGE` steer the generator) and `length`/`precision`/`scale`.

## 13. `orso.exceptions`

```
DataError(Exception)
  DataValidationError(errors: dict)          .errors
  ExcessColumnsInDataError(columns)          .columns
  ColumnDefinitionError(attribute)           .attribute
MissingDependencyError(Exception)(dependency) .dependency
```

Messages are free text; the class hierarchy and constructor arguments are the contract.

## 14. `orso.compute` (removed in v2)

v1's Cython modules (`compiled.pyx`, `column_encodings.pyx`) were implementation, and
only orso's own tests imported them. v2 deleted the package: their work is native
(`orso._native`), and the RLE/dictionary encoders, which nothing used, are gone. The
`RLEColumn`, `DictionaryColumn` and `SparseColumn` classes remain (pure Python + numpy).

---

## Known warts (consumers work around these; a rewrite may fix them)

1. **Untyped columns become VARCHAR in `arrow()`.** With a `RelationSchema` whose columns
   have the missing type, `arrow_field` maps them to `string`, so the first non-string
   value makes `arrow()` fail. mabel detects this and hands orso a list of names instead
   (`blob_writer.py`). Inferring types for untyped columns would let that workaround go.
2. **`OrsoTypes` members are mutated globally.** `FlatColumn.from_arrow` and
   `get_orso_type` set `_element_type`, `_scale`, `_precision`, `_length` on the enum
   singleton, and `parse()`/`metadata` read them. Two columns of the same type with
   different parameters race on shared state. The parameters belong on the column.
3. **`from_arrow(size=n)` converts the whole table** before honouring the limit.
4. **`DataFrame.nbytes()` raises `AttributeError`** when the frame holds plain tuples
   rather than `Row`s (every `DataFrame(rows=[...tuples...])`). Size any tuple.
5. **Two record-size caps:** `orso.row` said 16 MiB, `orso.compute.compiled` 8 MiB; only 16 was
   enforced. v2 has one, 16 MiB.
6. **`RelationSchema.to_json()` returns a dict** while `FlatColumn.to_json()` returns a
   string. Keep both as they are; renaming is an API change.
7. **`select()` returns a list-of-names schema** even when the source had a
   `RelationSchema`, so type information is lost on projection.
8. **Every `DataFrame()` with a `RelationSchema` rebuilds its append-validation plan**,
   which is why `head()`/`slice()`/`to_batches()` cost microseconds each. The plan
   belongs on the schema, or lazily on first `append`.
9. **`find_column` builds a list per column per lookup**; opteryx's binder calls it in
   loops.

## Behaviour the benchmark pins

`benchmarks/cases.py` exercises every tier-1 path above and `benchmarks/baseline_0.0.246.json`
is the number to beat. A rewrite is done when the full `tests/` suite passes unchanged
(it is the executable form of this document) and every benchmark case is faster than
the baseline by more than the 5% noise floor.
