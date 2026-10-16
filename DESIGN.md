# orso v2 internals

> Status: DRAFT for Justin to react to. API.md is the contract this must meet. This is a
> V2, not a clean-room: bugs in v1 get fixed here, and the "known warts" in API.md are
> work items, not behaviour to preserve.
>
> Decided 2026-10-16: **row-major storage**. draken is the column-major store; orso is the
> row-major one, and it adopts draken's memory model so the two can hand data across
> without a format in between.
>
> Decided 2026-10-16: **fixed-stride records of 16-byte slots with a per-field tag**
> (§4.1), draken's slot formats verbatim.

## 1. What v2 is

A C++ library with a thin nanobind binding, in the shape of draken, behind the existing
orso Python API. Rows go in and rows come out; what happens in between never touches a
Python object except at the two edges (ingest and readback), which is the rule draken's
binding doc sets: *marshal at the edge, once; no `object` mid-pipeline*.

The measured problem with v1 is not the data structure. A list of Python tuples is the
fastest thing to hand back to a Python caller (7 ns/row to iterate). The cost is every
per-row and per-value operation written in Python around it: dict validation on append,
`to_pylist` + `zip` + factory on `from_arrow`, msgpack for a size estimate, dataclass
introspection per column, regex per type name, string formatting per cell in `display`.
v2 moves all of that below the Python line and replaces the tuple list with a compact
native row store that decodes to tuples on demand.

## 2. Memory model: draken's, row-major

draken's model, read across rather than down:

| draken | orso v2 |
|---|---|
| `draken_malloc` / `draken_free`, the one process-wide allocator (`core/alloc.h`) | same; every buffer the store owns is `draken_malloc`'d, so a buffer can be handed to draken without a copy of the allocator contract |
| `AppendBuffer<T>` (`core/append_buffer.h`), growth without zero-fill | the record buffer and the byte arena |
| 16-byte German string slot (`core/string_slot.h`): short inline, long = length + prefix + arena offset | the slot format for `VARCHAR`, `BLOB`, `JSONB`; a row's string fields point into the frame's arena exactly as a draken string vector's slots point into its arena |
| `DrakenIntervalSlot` (`core/interval_slot.h`), 16 bytes | the `INTERVAL` slot |
| `DRAKEN_DECIMAL128`: 16-byte `__int128` unscaled, scale out-of-band | the `DECIMAL` slot, scale in the field tag |
| `DrakenType` tag values (`core/buffers.h`, frozen ABI) | the per-field type tags; orso does not invent a second type vocabulary |
| physical / logical split (`logical_type.h`) | physical tag per field; logical parameters on the `FlatColumn` / `TypeSpec`, never on a shared enum |
| `VectorOwner`: RAII buffers, views share the owner | `Store` owns, `Frame` views share it through `shared_ptr` |
| `draken_vector_own_raw[_with_arena]` (`vectors/_vector_bridge.h`) | the hand-off: a strided gather of one field's slots is a dense draken data buffer; for strings, the gathered slots plus a copy of the arena is a draken string vector |

So the row store is, field by field, a draken column with a stride. That is the whole
reason to adopt the slot formats: turning rows into draken vectors is a strided `memcpy`
per field, not a re-encoding, and the reverse is a scatter. Arrow stays the public
interchange format (that is what the API promises and what mabel writes), and the
draken hand-off is an additional, optional edge for processes that have draken loaded.

## 3. Layout

```
orso/
  native/                      C++20 headers + one nanobind translation unit
    draken/                    vendored, synced by `make sync-draken`, source commit recorded
      alloc.h  append_buffer.h  string_slot.h  interval_slot.h  buffers.h (types + structs only)
      iso_datetime.h  decimal_text.h  value_format.hpp  ryu/  arrow_c_data_interface.h
      _vector_bridge.h           (declarations only; resolved at runtime, see §4.8)
    record.h                   record layout: tags + 16-byte slots, stride arithmetic
    store.h                    Store: records, arena, object table, row count, size estimate
    frame.h                    Frame: a window over a Store, row cache, projection
    encode.h                   PyObject -> slot      (the only unboxing site)
    decode.h                   slot -> PyObject      (the only boxing site)
    nested.h                   ARRAY / STRUCT / JSONB payloads inside the arena
    validate.h                 compiled validation plan for a RelationSchema
    arrow_export.h             Store window -> ArrowArray/ArrowSchema per column
    arrow_import.h             ArrowArray per column -> records
    draken_bridge.h            Store window <-> DrakenVector (optional, runtime-resolved)
    serialize.h                as_json / as_bytes (msgpack) / from_bytes
    render.h                   ascii / markdown rendering from slots
    row.h                      Row: CPython tuple subclass created from C
    native.cpp                 nanobind module `orso._native`
  dataframe.py                 thin: argument handling, laziness, cursor; delegates to Frame
  row.py                       Row class + create_class cache; instances come from C
  schema.py                    RelationSchema / FlatColumn: slotted Python, native plan cached
  types.py                     OrsoTypes enum (unchanged surface); parsers call native
  converters.py, display.py, group_by.py, tools.py, logging/, cityhash/, faker/
                               stay Python; not on the hot path or one-line delegations
  compute/                     DELETED
```

Vendoring, not depending: mabel installs orso alone, so orso cannot import draken at
runtime, and the rugo wheel that bundles draken is far too big to pull in for a dozen
headers. `make sync-draken` copies them from `../opteryx-core/draken` and writes the
source commit into `native/draken/SOURCE`, the same single-source discipline
`build_common.py` uses for rugo. The ISO, decimal and value-format headers matter most:
orso and opteryx must not disagree about what a timestamp string means.

## 4. The native data model

### 4.1 Record

A frame with `n` fields stores every row as one fixed-stride record:

```
record  = [ tags: n × uint16, padded to 16 bytes ][ slots: n × 16 bytes ]
stride  = round_up(2n, 16) + 16n
tag     = low byte: DrakenType (DRAKEN_NULL for a null value)
          high byte: per-value parameter (decimal scale; timestamp unit if ever needed)
```

Fixed stride means row `i` is at `records + i * stride`, so there is no offsets index,
slicing is arithmetic, `to_batches` is arithmetic, and a column gather is a strided
read. For a six-field row that is 16 + 96 = 112 bytes plus long-string payload, against
roughly 300 bytes for the equivalent Python tuple and its objects.

Slot contents by tag:

| orso type | tag | slot (16 bytes) |
|---|---|---|
| `NULL` / missing value | `DRAKEN_NULL` | unused |
| `BOOLEAN` | `DRAKEN_BOOL` | byte 0 |
| `INTEGER` | `DRAKEN_INT64` | int64 |
| `DOUBLE` | `DRAKEN_FLOAT64` | double |
| `DATE` | `DRAKEN_DATE32` | int32 days since epoch |
| `TIMESTAMP` | `DRAKEN_TIMESTAMP64` | int64 µs, naive UTC |
| `TIME` | `DRAKEN_TIME64` | int64 µs since midnight |
| `INTERVAL` | `DRAKEN_INTERVAL` | `DrakenIntervalSlot` (months, µs) |
| `DECIMAL` | `DRAKEN_DECIMAL128` | `__int128` unscaled; scale in the tag's high byte |
| `VARCHAR` | `DRAKEN_VARCHAR` | string slot: ≤ 12 bytes inline, else length + prefix + arena offset |
| `BLOB`, `JSONB`, `STRUCT` without fields | `DRAKEN_VARBINARY` | string slot over bytes (JSON bytes for the last two) |
| `ARRAY<T>`, `VECTOR` | `DRAKEN_ARRAY` | length + arena offset of a nested slot sequence (§4.4) |
| `STRUCT` with fields | `DRAKEN_ARRAY` with struct marker in the high byte | arena offset of a nested record |
| anything else | `DRAKEN_VARIANT` (object) | `PyObject*` in bytes 0..7, index into the object table in 8..15 |

Every field carries its own tag whether or not the frame has a `RelationSchema`. With a
schema the tag is what the plan allowed; without one it is whatever the value was. One
layout, one decode path, and heterogeneous untyped frames need no column promotion
logic: the Arrow exporter looks at the tags.

### 4.2 Store

```cpp
struct Store {
    uint32_t               nfields;
    uint32_t               stride;
    AppendBuffer<uint8_t>  records;     // rows × stride
    AppendBuffer<uint8_t>  arena;       // long strings, bytes, nested payloads; draken_malloc'd
    AppendBuffer<PyObject*> objects;    // DRAKEN_VARIANT values, incref'd; decref'd on destroy
    uint64_t               rows;
    uint64_t               size_estimate;   // see nbytes, §4.6
    std::mutex             mu;              // append / cache fill on free-threaded builds
};
```

The arena is append-only and shared by every record; a slice never copies it. Objects
live in a side table so the store can implement `tp_traverse` and the GC can see them.

### 4.3 Frame

```cpp
struct Frame {
    std::shared_ptr<Store>  store;
    uint64_t                offset, length;   // window, fixed at creation
    const uint32_t*         projection;       // field index map for select(), or null
    PyObject**              row_cache;        // length entries, lazily allocated, lazily filled
    PyObject*               row_type;         // the Row subclass for this field list
    ValidationPlan*         plan;             // borrowed from the schema object; null for list schemas
};
```

`slice`, `head`, `tail`, `to_batches`, `filter`, `take`, `query` and `select` are new
`Frame`s over the same `Store` (`filter`/`take`/`query` materialise an index list as a
gather window). A child's `length` is fixed when it is made, so appending to the parent
does not change what the child sees.

A row is decoded by walking its record once, left to right, into a freshly allocated
`Row` tuple. Decoded rows are kept in `row_cache`, so the second pass is one pointer per
row, the same cost as v1's list. `materialize()` drains any pending source iterator into
the store and does not decode. `_rows`, which the PyPI opteryx cursor reads, becomes a
property returning a sequence proxy over the frame (indexable, iterable, `len()`).

### 4.4 Nested values

`ARRAY<T>`, `VECTOR`, and `STRUCT` with fields are encoded into the arena with the same
slot grammar, recursively:

```
sequence = [ count: uint32 ][ tags: count × uint16, padded ][ slots: count × 16 ]
record   = tags + slots as in §4.1, with the child FlatColumn order
```

Nested string slots point into the same arena. `JSONB`, and `STRUCT` without declared
fields, are JSON bytes written by a native writer (v1 did the same through `json.dumps`).
Decoding a nested value recurses through the same decoder.

### 4.5 Encode: the only unboxing site

`encode.h` turns one `PyObject*` into one tagged slot. It is a switch on the *expected*
type (from the plan) or on the value's type (untyped), with exact-type fast paths first
(`PyLong_CheckExact`, `PyFloat_CheckExact`, `PyUnicode_CheckExact` using the cached UTF-8
of compact strings, `PyDateTime_CheckExact` through the C-API macros) and the correct
slow path second (`__index__`, subclasses, numpy scalars through the buffer protocol,
`Decimal` through its text form into `decimal_text.h`). Strings of 12 bytes or fewer
never touch the arena.

`append(dict)` against a `RelationSchema` is therefore: reserve one record, then for each
column in the compiled plan `PyDict_GetItem` with the interned name, null check against
`nullable`, `encode` into slot `j`. No intermediate tuple, no `isinstance` chain, no
second pass. A value the plan does not accept raises the same `DataValidationError`
v1 raised, with the same error keys.

### 4.6 Size estimate

mabel polls `nbytes()` to decide when to flush a blob, so the number must mean what it
meant: roughly the msgpack size of the rows plus 14 per row. The slot layout is 2 to 8
times larger than msgpack for small integers, so the store does not report its own byte
count. Instead `encode` adds the msgpack width of each value (1 to 9 bytes for an
integer by magnitude, length + header for strings, and so on) to `Store.size_estimate`
as it goes. It costs a few instructions per value and keeps blob sizes where they are.
`nbytes()` is then O(1), exact to v1's estimate, and no longer serialises anything.

### 4.7 Decode: the only boxing site

`decode.h` is the inverse switch. Strings become `PyUnicode_FromStringAndSize`
(`PyBytes` for binary), timestamps `PyDateTime_FromDateAndTime` from
`civil_from_days`, decimals `Decimal(text)` through the cached class, intervals
`timedelta` plus a months component carried the way v1 carried it, nested values
recurse, objects are an incref. The datetime C-API is the slowest thing here; decode
never goes through `fromtimestamp`.

### 4.8 Arrow in both directions, and draken

Export is one pass over the window's records with one Arrow builder per field appended
in lockstep, so each record is read once. Fixed-width fields are a strided copy; string
fields become offsets + data from inline bytes or the arena; nested fields recurse. The
Arrow type per field comes from the `RelationSchema` when there is one, otherwise from
the tags (one non-null tag type → that type; mixed `INT64`/`FLOAT64` → `float64`;
anything else → fall back to building that column through pyarrow's own inference from
decoded objects, which is what v1 did for every column of an untyped frame). The result
is handed over through the C Data Interface with a release callback; buffers are
`draken_malloc`'d copies, as in draken's exporter, so the store can keep accepting
appends after export. Cost is roughly `rows × fields` slot reads: a few milliseconds
for 100k × 6, against 10 ms today through `from_arrays` and 56 ms for the reverse.

Import takes pyarrow's buffers through `__arrow_c_array__` per column (pyarrow 14+) and
scatters them into records: one pass over rows, fields inner. Strings over 12 bytes are
copied into the arena once. `from_arrow(size=n)` imports `n` rows and stops; chunked
tables import chunk by chunk into the same store.

The draken bridge is the same two loops with a different sink and source: a gather of
field `j` into a `draken_malloc`'d buffer, plus a validity bitmap from the tags, plus
for strings a copy of the arena, handed to `draken_vector_own_raw_with_arena`; and the
reverse scatter from a `DrakenVector`'s `data[selection[i]]`. It compiles into the same
extension but resolves the bridge symbols at runtime (`dlsym`, the way draken's own
`__init__` loads with `RTLD_GLOBAL`), so `import orso` works without draken and
`Frame.to_draken()` / `Frame.from_draken()` raise `MissingDependencyError` when it is
not loaded. This is the optional edge; nothing in API.md depends on it.

### 4.9 Row

`Row` stays a `tuple` subclass; CPython reads tuple items directly, so there is no lazy
tuple. The base type is defined in C (`PyType_FromSpec` with `PyTuple_Type` as base).
`create_class` still builds a subclass per field list and caches it; instances are
allocated in C with the exact size and filled by decode. `as_dict`, `as_map`, `get` are
C functions over the tuple using interned field names. `as_json` and `as_bytes`
serialise the tuple through `serialize.h`, not the record, because a `Row` can be built
by hand with no frame behind it.

The record byte format is kept byte-identical (API.md §3.1) by writing msgpack
directly; `ormsgpack` is dropped. `nbytes()` on a `Row` is computed from the tuple
without serialising, with the same per-value widths as §4.6.

### 4.10 Schema and types

`RelationSchema` and `FlatColumn` stay Python classes: the PyPI opteryx binder
deep-copies them and round-trips them through `to_dict`/`from_dict`, and a nanobind type
would have to re-implement `__deepcopy__`, `__reduce__` and equality by hand for no gain.
They lose the dataclass machinery: `__slots__`, an explicit keyword `__init__`, a
hand-written `__eq__` and `to_dict`. That alone takes `FlatColumn` from 2.1 µs to about
0.3 µs. A `RelationSchema` lazily builds and caches a `name → column` dict (aliases
included, so `find_column` is O(1)), the `column_names` tuple, the Arrow schema, and the
native `ValidationPlan` that frames borrow. The `columns` list is wrapped so that
`.append` and item assignment invalidate the caches.

`OrsoTypes` keeps its `str`-Enum surface. The parameters v1 stored on the enum
singleton move to the column and to a small immutable `TypeSpec` returned beside
`from_name`; `parse(value, **kw)` takes them as keyword arguments as it already does.
Every parser is native: `iso_datetime.h` for dates and timestamps, `decimal_text.h` for
decimals, and the type-name grammar is a hand-written scanner instead of three regexes
per call.

### 4.11 Rendering

`display`, `markdown` and `str()` render in C++ from slots using `value_format.hpp`
(ryu for doubles, ISO for temporals, scale-aware decimals), so width measurement and
truncation never materialise Python strings per cell. The ANSI colour table and box
glyphs stay in `display.py`; only the per-cell work moves.

## 5. Binding

One nanobind module, `orso._native`, following draken's binding doc: it owns the C++
objects, marshals at the edge, exposes free functions, and loops over rows only in the
edge coders. Surface, approximately:

```
Frame(plan | nfields)
  .append(obj)  .append_dict(obj)  .extend(iterable)
  .row(i) -> Row   .rows(start, stop) -> list   .fetch(cursor, n)
  .slice(offset, length)  .select(indices)  .take(indices)  .length
  .nbytes()  .collect(field_indices, limit)
  .to_arrow(size) -> [(ArrowArray*, ArrowSchema*)]      Frame.from_arrow(capsules, nrows_cap)
  .to_draken() -> list[Vector]                           Frame.from_draken(vectors)
ValidationPlan([(name, nullable, tag, params) ...])
Row base type; make_row_type(fields) -> type
parse_timestamp / parse_date / parse_time / parse_decimal / parse_type_name
render_ascii(frame, ...) / render_markdown(frame, ...)
row_as_json / row_as_bytes / row_from_bytes / row_nbytes
```

The Python `DataFrame` keeps every signature in API.md and does argument validation,
the `dictionaries` path (first-dict schema), laziness (holding the source iterator until
something materialises), the cursor and its invalidation on append, and dispatch. It
should end up under 300 lines.

## 6. Free threading

Production runs the GIL build of 3.14, so v2 is built clean for free-threaded Python
and not tuned for it: nanobind compiled with `NB_FREE_THREADED` where
`Py_GIL_DISABLED`, a mutex on `Store` around append and on `Frame` around cache fill,
no mutable module-level state. Read-only views need no lock.

## 7. Build and packaging

- `setup.py` builds one extension, `orso._native`, from `native/native.cpp` with the
  flags `build_common.py` uses (`-O3 -std=c++20`, ISA floor `-march=haswell` on x86 and
  `armv8-a` on arm; no `-march=native` in published wheels). nanobind comes in the way
  opteryx-core already takes it.
- Dependencies after v2: `pyarrow` optional (needed for `arrow()` / `from_arrow()`,
  `MissingDependencyError` otherwise), `data-expectations` for `SchemaExpectation`,
  `xxhash` for `cityhash` or an inline xxh64. `numpy`, `ormsgpack`, `pandas` go;
  `numpy` stays optional for `ConstantColumn`/`FunctionColumn.materialize()`, which
  opteryx reads as arrays.
- Wheels for cp310 through cp314 plus cp314t, macOS arm64, manylinux x86_64 and
  aarch64, on CI as today.
- `make bench` against `benchmarks/baseline_0.0.246.json` gates every merge.

## 8. Bugs fixed on the way

| # | v1 | v2 |
|---|---|---|
| 1 | untyped columns export as `string` and fail on the first non-string | typed from tags |
| 2 | `OrsoTypes` singletons mutated with per-column parameters | parameters on the column / `TypeSpec` |
| 3 | `from_arrow(size=n)` converts the whole table | imports `n` rows |
| 4 | `nbytes()` raises on plain-tuple rows | estimate kept by the store |
| 5 | two record-size caps (8 and 16 MiB) | one, 16 MiB |
| 7 | `select()` drops the `RelationSchema` | keeps the matching `FlatColumn`s |
| 8 | append plan rebuilt per `DataFrame()` | cached on the schema |
| 9 | `find_column` builds lists per lookup | O(1) dict |
| 10 | `DATE` exports as `date64` while `date32` imports as `date` and `date64` as `datetime`, so a date round-trips as a datetime | export `date32`; import both as `date` |
| 11 | ISO parser drops `Z` / `+HH:MM`, shifting values | offsets honoured, normalised to UTC |
| 12 | `arrow()` with a `STRUCT` column lacking `fields` abandons the whole declared schema | only that column falls back |
| 13 | `__hash__` serialises every row to text | hash the record bytes |
| 14 | a row of the wrong width is stored and corrupts `arrow()` later | width checked at ingest |
| 15 | `DataFrame(dicts)` ignores keys a later dict adds | still ignored (API), but with a warning the first time |

Wart 6 (`RelationSchema.to_json()` returns a dict) is an API shape and stays. Fixes 10
and 11 change answers a consumer could observe; they get a changelog line and a test
that pins the new answer.

## 9. Targets

Minimum times against `benchmarks/baseline_0.0.246.json`:

| case | v1 ns/op | target | why |
|---|---|---|---|
| append/dict_with_relation_schema | 498 | ≤ 150 | one dict probe + one slot write per column |
| append/schema_validate | 844 | ≤ 150 | same plan, no store |
| arrow/from_arrow_materialized | 564 | ≤ 60 | scatter, no decode until touched |
| arrow/from_arrow_first_1000_rows | 55,900 | ≤ 60 per row imported | window on import |
| arrow/to_arrow_relation_schema | 104 | ≤ 40 | one pass, strided gathers |
| arrow/to_arrow_wide_20_columns | 749 | ≤ 120 | scales with slots, not Python cells |
| row/create_from_dict | 227 | ≤ 80 | C tuple alloc + `PyDict_GetItem` per field |
| row/as_dict | 247 | ≤ 60 | `PyDict_New` + interned keys |
| row/as_json | 1,613 | ≤ 300 | native writer |
| row/nbytes_first_call | 366 | ≤ 40 | widths, not serialisation |
| iterate/for_row_in_materialized (first pass) | 7 (tuples pre-exist) | ≤ 200, then 7 | decode once, cached |
| construct/flatcolumn_init_typed | 2,145 | ≤ 400 | slotted class |
| construct/schema_find_column | 859 | ≤ 60 | dict |
| types/parse_timestamps | 722 | ≤ 120 | `iso_datetime.h` |
| types/parse_decimals | 1,255 | ≤ 400 | `decimal_text.h` then one `Decimal()` |
| display/ascii_100_rows_with_types | 36,000 / row | ≤ 3,000 | native render |

The one path that gets slower is the first iteration over a frame built from Python
tuples, because v1 kept the tuples and v2 encodes them. It is bounded (about 200 ns/row,
once) and it pays for the memory and the Arrow numbers above.

## 10. Delivery order

Each step lands green against `tests/` (the executable spec) and the benchmark.

1. **Skeleton.** `native/` with vendored headers, nanobind module building in CI for every
   target, `make sync-draken`. No behaviour.
2. **Record store, encode, decode, Row.** Typed and untyped frames, `append`, `row(i)`,
   iteration, fetch cursor, windows, `nbytes` with the msgpack-width estimate.
   `DataFrame` delegates. Gate: append, row, iterate groups.
3. **Arrow.** Export and import through the C Data Interface, chunked tables, `size`
   windows, decimal / list / struct mapping. Gate: arrow group; mabel's blob writer and
   the opteryx cursor paths run on it.
4. **Schema and types.** Slotted `FlatColumn`/`RelationSchema`, cached plans and lookups,
   native parsers, `TypeSpec`. Gate: construct and types groups.
5. **Serialisation and rendering.** `as_json`, `as_bytes`/`from_bytes` byte-identical,
   native `display`/`markdown`. Gate: row and display groups.
6. **Delete.** `orso/compute/`, column encodings and the tier-3 converters; dependencies
   trimmed to §7; `tests/test_benchmark_suite.py` and `test_materialize_perf.py`
   replaced by `benchmarks/`.
7. **draken bridge.** `to_draken` / `from_draken` behind runtime symbol resolution, tested
   in an environment with opteryx-core's draken loaded.
8. **Consumers.** mabel's and the PyPI opteryx's test suites against v2 from a local
   checkout before tagging.

## 11. Decisions wanted

1. ~~Fixed 16-byte slots with a per-field tag~~ **Decided: yes.** Costs 2 to 8× the
   bytes of msgpack for small values; buys O(1) row addressing, strided column gathers,
   and a direct draken hand-off.
2. ~~The draken bridge is phase 7~~ **Decided: yes**, runtime-resolved, optional. Arrow
   stays the public interchange.
3. ~~Vendor the draken headers with a sync target~~ **Decided: yes**, never depend on
   the rugo wheel.
4. ~~Export copies buffers~~ **Decided: yes** in the first cut; zero-copy later if the
   benchmark says so.
5. ~~Schema classes stay Python~~ **Decided: yes** (slotted); parsers and plans go native.
6. ~~Fixes 10 and 11 ship in v2~~ **Decided: yes**, with changelog lines and pinning tests.
7. ~~Drop `numpy`, `ormsgpack`, `pandas`~~ **Decided: yes** as hard dependencies.

All seven decided 2026-10-16. Phase 1 starts from here.

8. **Windows.** The vendored draken headers use `__int128` (`decimal_text.h`,
   `DRAKEN_DECIMAL128`) and `<execinfo.h>` (`alloc.h`), neither of which exists under
   MSVC, and opteryx-core publishes no Windows wheels. orso's Windows wheels are
   release-critical (regression_suite.yaml builds and tests them). Phase 1 skips
   `orso._native` on Windows with a message, so the v1 package still ships there and
   nothing yet depends on the native layer. Before phase 2 lands behaviour in it, one of:
   (a) build the Windows wheel with clang-cl, which has `__int128` on x86-64, plus a
   Windows allocator shim with no backtrace; (b) a Windows-specific int128 and
   allocator in orso's own tree, kept out of the vendored files; (c) drop Windows
   wheels. **Decided 2026-10-16: (c), no Windows support.** The Windows release job
   and regression entries are gone and `setup.py` refuses to build there.

## 12. Phase 1 status (2026-10-16)

Done, on macOS arm64 / CPython 3.14: `native/` with 32 vendored files from opteryx-core
`6c3da721` (`tools/sync_draken.py`, `make sync-draken`, `make check-draken`, CI check);
`orso._native` built by `setup.py` as one nanobind extension beside the v1 Cython
modules, with `record.h` pinning the §4.1 stride arithmetic and probe functions proving
`iso_datetime.h`, `AppendBuffer`, and ryu compile and link under orso's flags;
`tests/test_native_skeleton.py` (11 tests). The v1 suite is unchanged (422 passed).
Not yet verified: Linux (manylinux_2_28 image) and free-threaded builds, which CI covers.
