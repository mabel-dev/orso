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
| anything else | `TAG_OBJECT` (0xFF, orso-private, never a `DrakenType`) | `PyObject*` in bytes 0..7; one strong reference held in the store's object list |

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

**Where rows live (phase 2 amendment).** A `DataFrame` keeps its rows in the form they
arrived in until an operation needs the other form. Rows handed to the constructor
(a list, a tuple, a generator) or built by `DataFrame(dictionaries)` stay Python `Row`
objects in a list, exactly as v1 kept them, so passing rows through costs what it always
did. Rows appended to an empty frame go into the record store, validated and encoded in
C, sized in O(1) and sliced as views. Reading `_rows` on a store frame decodes it once
and it becomes a list frame; that is the bridge for the v1 code paths (arrow, display,
collect, group_by) until phases 3 and 5 give them native routes. A store frame's DB-API
cursor is a native `FrameCursor` that iterates like v1's list iterator, so `fetchone()`
is `next(cursor)` for both kinds.

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

The phase 1 commit as first made could not build from a clean checkout: orso's
`.gitignore` rules for Cython output (`**.c`, `**.cpp`) also hid `native/native.cpp` and
the vendored `.c`/`.cpp` sources. Fixed by un-ignoring `native/**` in a follow-up commit,
verified by building that commit in a fresh worktree.

## 13. Phase 2 status (2026-10-16)

Done, on macOS arm64 / CPython 3.14:

- `native/row.h`: the C base of `Row` (`RowBase`). Construction from a dict or any
  iterable, `as_dict`, `as_map`, `values`, `get`, `keys`, `nbytes` in C. Row subclasses
  have `__slots__ = ()`, so a row is exactly a tuple in memory; 3.14's cached tuple hash
  is reset on allocation. `as_map` is a plain property (it was a `cached_property`, which
  needed the per-row `__dict__` v1 rows carried by accident).
- `native/width.h`: `nbytes()` without serialising. Widths match ormsgpack exactly for
  every value type in `tests/test_native_rows.py`; anything not sized natively is sized
  by v1's own expression for that one value, so v1's numbers and v1's errors are kept.
- `native/store.h`: `Plan` (v1's validation, compiled: names, nullability, `isinstance`,
  JSONB serialisation, and v1's exact errors via `RelationSchema.validate` on the slow
  path), `Store` (fixed-stride records, arena, object list), `Frame` (owner or view,
  copy-on-write on append to a view), `FrameCursor`.
- Encoded natively: None, bool, int64, float, str, bytes, naive datetime / date / time,
  timedelta. Held as objects for now: Decimal, lists, dicts, tuples, sets, aware
  temporals, subclasses, ints outside 64 bits. Native decimals and nested values move to
  phase 3, where the Arrow export is what they pay off for.
- Fixes landed: a store frame rejects a row of the wrong width at `append` (§8 #14);
  `nbytes()` works on frames of plain tuples (#4); the validation plan is built on first
  append instead of per `DataFrame()` (#8); `column_names` is cached per frame instead of
  in one process-wide slot that also pinned the last frame in memory.
- Tests: `tests/test_native_rows.py` (106) pins sizes against ormsgpack, store frames
  against list frames for every read path, v1's validation errors, and reference counts.
  Full suite: 539 passed, 6 skipped (opteryx-dependent, not installed here).

Measured against v1 (0.0.246) built and run back to back on the same machine, control
case drift +3.0%, 5 rounds, minimum times: **22 cases faster, 0 slower, 35 unchanged
within ±5%; geometric mean 22.2% faster.** The cases phase 2 targets:

| case | v1 ns/op | v2 ns/op | speedup |
|---|---|---|---|
| append/dict_with_relation_schema | 492 | 83 | 5.9x |
| construct/dataframe_from_dicts | 258 | 65 | 4.0x |
| row/nbytes_first_call | 367 | 94 | 3.9x |
| iterate/head_5_repeated | 1,676 | 438 | 3.8x |
| append/dict_with_column_names | 258 | 82 | 3.1x |
| append/append_then_nbytes_then_arrow | 932 | 302 | 3.1x |
| append/tuple_with_column_names | 190 | 72 | 2.6x |
| row/create_from_dict | 223 | 93 | 2.4x |
| row/get_by_name | 36 | 16 | 2.2x |
| row/as_dict | 240 | 116 | 2.1x |
| row/create_from_tuple | 154 | 79 | 2.0x |
| append/dict_with_jsonb_column | 1,249 | 708 | 1.8x |

Untouched groups (arrow conversion, display, schema, types) are within noise, as they
should be: their code has not moved yet. `append/dict_with_jsonb_column` is now bound by
the stdlib JSON encoder; `append/schema_validate` is `RelationSchema.validate` itself,
which is phase 4.

Known gaps carried into phase 3:

- The store does not take part in garbage collection. A value held as an object that
  refers back to its own frame is a cycle the collector cannot see. The fix is to make
  `Store` its own Python object that frames reference and that traverses its objects
  (frames cannot traverse a shared store: two views would each report the same
  reference). No consumer stores such values today.
- Free-threaded builds compile but the store has no locking yet (§6).
- `arrow()` on a store frame decodes to Python rows first; phase 3 replaces that.

## 14. Phase 3 status (2026-10-16)

Done, on macOS arm64 / CPython 3.14 / pyarrow 26:

- **Decimals** (`native/decimal.h`): exact `Decimal` in a DECIMAL128 slot (int128 unscaled,
  scale and a negative-zero flag in the tag), read from `str(Decimal)` and rebuilt with
  `Decimal(text)`, so sign, digits and exponent survive. NaN, infinities, positive
  exponents and more than 38 digits stay objects. `nbytes()` computes `len(str(d))` per
  the decimal spec instead of serialising.
- **Nested values**: lists, tuples and dicts are encoded recursively into the arena in
  the record's own slot grammar (§4.4), so they are sized, exported and copied natively.
  Observable difference: a frame now holds a copy, not the caller's list, so mutating the
  list after `append` no longer changes the frame.
- **Import** (`native/arrow_import.h`): `RecordBatch.__arrow_c_array__` capsules, one
  reader per column from the format string, a range pre-check for temporal columns, then
  a row-major scatter. Native readers cover ints, uints, floats, bool, utf8/large_utf8,
  binary/large_binary, date32/64, timestamp s/ms/us (naive), time32/64 (not ns),
  duration s/ms/us, decimal128, null, list/large_list/fixed_size_list, struct. Everything
  else (time zones, nanoseconds, float16, decimal256, string views, intervals) comes
  through pyarrow's `to_pylist()`, so every row equals v1's, type for type.
  `DataFrame.from_arrow` of a Table, RecordBatch or list of them builds a store frame;
  a generator of tables stays lazy, one table at a time, as v1 was. `converters.from_arrow`
  still returns an iterator (a `FrameCursor`), which the PyPI opteryx `Cursor` relies on.
- **Export** (`native/arrow_export.h`): one builder per column writes Arrow buffers from
  the slots and hands them over as capsules. Each target type accepts exactly what
  pyarrow's converter accepts (measured: bool is refused for int64, ints only up to 2^53
  for float64, datetimes truncate to date32, times truncate to ms, lossy decimal rescales
  refuse); anything else sends that one column back to pyarrow as v1's Python list.
  List frames export through a temporary store too: encoding plus native export beat
  extracting columns and letting pyarrow convert them, 1.7 to 3.1x.
- **Fixes landed**: DATE exports as date32 and date64 imports as DATE (§8 #10); a column
  with no declared type, or a STRUCT with no fields, is inferred on its own instead of
  forcing inference on the whole table (#1, #12); `from_arrow(size=n)` converts n rows (#3).
- **Tests**: `tests/test_native_arrow.py` (93) compares native export with pyarrow
  converting the same Python values, including raising the same exception types, and
  native import with `to_pylist()` across 36 Arrow types, sliced, chunked and batched.
  Full suite 632 passed, 6 skipped.

Measured against v1 (0.0.246) built and run back to back, 5 rounds, minimum times, on a
loaded machine (load average ~8, control drift +4.2%): **31 cases faster, 23 unchanged,
geometric mean 45.0% faster.** Four cases flagged 6–8% slower were re-measured with v1
and v2 interleaved over 9 rounds and came out within ±3% (one 19% faster): load noise.

| case | v1 ns/op | v2 ns/op | speedup |
|---|---|---|---|
| arrow/from_arrow_first_1000_rows | 59,180 | 401 | 148x |
| arrow/from_arrow_materialized | 606 | 34 | 17.8x |
| arrow/roundtrip_arrow_orso_arrow | 725 | 59 | 12.3x |
| append/append_then_nbytes_then_arrow | 979 | 154 | 6.4x |
| arrow/from_arrow_then_read_every_row | 615 | 160 | 3.8x |
| arrow/to_arrow_wide_20_columns | 1,013 | 331 | 3.1x |
| arrow/to_arrow_relation_schema | 116 | 59 | 2.0x |
| arrow/to_arrow_inferred_types | 116 | 66 | 1.7x |
| arrow/to_arrow_decimal_columns | 123 | 88 | 1.4x |

`from_arrow_materialized` measures an import that no longer decodes anything, so it
flatters v2; `from_arrow_then_read_every_row` (new this phase) is the honest end-to-end
number when every row is used.

v1 bugs found, not fixed (schema conversion is phase 4):

- `FlatColumn.arrow_field` uses `self.scale or 10`, so a DECIMAL with scale 0 exports as
  `decimal128(p, 10)`: `DECIMAL(10,0)` becomes `decimal128(10,10)`, which cannot hold 1.
- `FlatColumn.from_arrow` crashes on `map` columns, and `arrow_type_map` refuses
  dictionary-encoded columns, so neither v1 nor v2 can import them. Pinned by tests.

## 15. Phase 4 status (2026-10-16)

Done, on macOS arm64 / CPython 3.14:

- **Slotted schema classes, dataclass metadata kept.** `FlatColumn` and `RelationSchema`
  store every field in a hidden `__slots__` member behind `orso._native.TrackedSlot`, a C
  data descriptor: reads cost a slot read, and an unset field reads as its default, as v1's
  class attributes gave. They stay dataclasses in metadata (`__dataclass_fields__`, the
  generated `__eq__` and `__repr__`) because the PyPI opteryx planner declares
  `@dataclass(init=False) class ExpressionColumn(FlatColumn)`, whose generated equality
  and repr only cover the inherited fields if `FlatColumn` is still a dataclass, and
  because `to_dict()`/`to_json()` use `asdict()`. Construction is hand-written (a fast
  path for plain `FlatColumn`; subclasses go through their attributes so re-declared
  fields such as `ConstantColumn.length` land where reads find them). Copies and pickles
  carry the fields and drop the caches. Columns no longer have a `__dict__`.
- **Cached lookups that survive mutation.** The PyPI opteryx binder renames, re-aliases
  and retypes columns in place and reassigns `schema.columns`. Writes to the fields
  lookups depend on (name, aliases, type, element type, nullability, precision, scale,
  length, child fields) bump a process-wide schema epoch; a schema's cache (name and
  alias → column, case-folded names, name tuples, Arrow schema, validation plan) is
  valid while the epoch, the columns list's identity and its length are unchanged.
  Building a column never bumps the epoch. Not detected: replacing a list element in
  place (`schema.columns[i] = c`, `column.aliases[0] = "x"`); neither consumer does it.
  The validation plan is cached on the schema for `validate()`; `DataFrame` keeps its own
  per-frame plan (built on first append), which is cheap enough per mabel blob.
- **Native validation**: `RelationSchema.validate` confirms a passing record with the
  native plan (exact keys, nullability, `isinstance`) and sends anything else down v1's
  path, so errors are v1's. Checked against v1 on 3,000 random records.
- **Parsers**: `parse_iso` tries draken's `iso_datetime.h` first and falls back to v1's
  parser for anything it refuses, so every string v1 accepted still parses.
  `OrsoTypes.from_name` is memoised (deprecated spellings are not, so they still warn);
  `OrsoTypes.parse` skips the keyword set for parameterless types; decimal factories and
  their quantize contexts are cached; `arrow_type_map` builds its table once.
- **Fixes** (all pinned by tests):
  - `parse_iso` applies a time-zone offset (§8 fix 11) and keeps fractional seconds;
    a `datetime` argument keeps its microseconds. v1 dropped all three. **Observable.**
  - DECIMAL scale 0 exports as scale 0 (v1: `scale or 10` made `DECIMAL(10,0)`
    `decimal128(10,10)`).
  - Map columns import as ARRAY and dictionary-encoded columns as their value type
    (v1's schema conversion crashed on the first and refused the second).
  - `FlatColumn.from_arrow` no longer writes column parameters onto the shared
    `OrsoTypes` member (§8 fix 2). opteryx's own converter still sets them itself.
  - `to_flatcolumn()` keeps `length`, `disposition` and `expectations`.
  - `random_string()` always returns exactly `width` characters (v1 could return fewer).
  - `tests/test_faker.py::test_unfakeable_types` was flaky (the faker returns None for 1%
    of nullable values before its type check); the test's column is now non-nullable.
- **Tests**: `tests/test_native_schema.py` (23). Full suite 660 passed, 6 skipped.

Measured against v1 (0.0.246) built and run back to back, twice (control drift +12.0%,
then −5.2%): **44–48 cases faster, 0 slower; geometric mean 60.5–61.1% faster.**
From the second run:

| case | v1 ns/op | v2 ns/op | speedup |
|---|---|---|---|
| construct/schema_find_column | 920 | 74 | 12.4x |
| types/from_name | 654 | 60 | 11.0x |
| construct/orso_schema_to_arrow_schema | 2,923 | 314 | 9.3x |
| append/schema_validate | 891 | 109 | 8.2x |
| types/parse_timestamps | 762 | 118 | 6.4x |
| types/parse_dates | 654 | 130 | 5.0x |
| construct/flatcolumn_init_typed | 2,211 | 636 | 3.5x |
| construct/flatcolumn_from_arrow | 3,147 | 1,009 | 3.1x |
| construct/relation_schema_from_kwargs | 3,066 | 1,195 | 2.6x |
| construct/flatcolumn_init_from_string_type | 3,029 | 1,275 | 2.4x |
| types/parse_scalars | 374 | 219 | 1.7x |
| types/parse_decimals | 1,293 | 884 | 1.5x |

Short of the §9 targets: `FlatColumn()` (636 ns against ≤ 400; a random identity and
eighteen slot writes) and decimal parsing (884 ns against ≤ 400; the stdlib quantize
dominates). `flatcolumn_from_arrow` was re-measured after the `arrow_type_map` change.

## 16. Phase 5 status (2026-10-16)

Done, on macOS arm64 / CPython 3.14:

- **Records** (`native/serialize.h`): `Row.as_bytes`, `_packed` and `from_bytes` write
  and read msgpack natively, byte-identical to v1. The writer encodes the common exact
  types exactly as ormsgpack does (measured: unsigned family for positive ints, str8,
  RFC 3339 text for naive datetimes, subclasses as their base type, `str(value)` for
  Decimal); every other value (numpy, enums, UUIDs, dataclasses, aware datetimes, ints
  wider than 64 bits) is packed by ormsgpack itself, one value at a time. msgpack is
  concatenative, so the record is identical by construction. The reader handles what
  the writer emits and hands anything else to `ormsgpack.unpackb`; v1's header checks,
  errors and `__datetime__` post-processing are kept.
- **Deviation from decision 7**: ormsgpack stays a dependency, as the encoder of last
  resort. Dropping it would mean re-implementing ormsgpack's handling of numpy,
  dataclasses, enums and UUIDs to stay byte-identical, or changing those bytes. It is no
  longer on the path of ordinary rows.
- **JSON**: `Row.as_json` reproduces `json.dumps(as_dict, default=str,
  separators=(",", ":"))` natively: ASCII-only output with `\u` escapes and surrogate
  pairs, `float.__repr__`, NaN/Infinity, json's key conversions, `str(value)` for the
  rest. Rows it is not sure of (non-str or duplicate field names, cycles, deep nesting)
  go to Python's json.
- **Display**: profiling put ~70% of `display()` in `trunc_printable`, a character walk
  per cell. It is now native (`native/render.h`), character for character; widths come
  from the running Python's `unicodedata`, cached per character. Layout, colours and
  type formatting stay in `display.py`. `markdown()` was already cheap and is unchanged.
- **collect()** on a store frame decodes only the requested fields, returning the same
  object array v1 did, with v1's bounds errors.
- **Tests**: `tests/test_native_serialize.py` (95) compares payloads with ormsgpack and
  JSON with `json.dumps` across 3,000 random rows each plus every edge type,
  `from_bytes` with v1's Cython reader, and `trunc_printable` with v1's across 20,000
  random strings. Full suite 755 passed, 6 skipped.

Measured against v1 (0.0.246) built and run back to back, control drift +0.0%:
**47 cases faster, 0 slower, 11 unchanged; geometric mean 63.4% faster.**

| case | v1 ns/op | v2 ns/op | speedup |
|---|---|---|---|
| row/as_bytes | 326 | 122 | 2.7x |
| row/as_json | 1,629 | 696 | 2.3x |
| display/ascii_100_rows_with_types | 36,687 | 16,387 | 2.2x |
| display/str_of_100k_row_frame | 43,016 | 20,638 | 2.1x |
| row/from_bytes | 345 | 173 | 2.0x |
| display/ascii_wide_50_columns | 60,515 | 35,800 | 1.7x |
| display/markdown_50_rows | 1,854 | 1,850 | 1.0x |

Short of §9: `as_json` (696 ns against ≤ 300; float and datetime reprs still go through
Python's formatters) and `display` (16 µs a row against ≤ 3 µs; the rest of the per-cell
formatting is still Python). Both are interactive or occasional paths.

## 17. Phase 6 status (2026-10-16)

Done:

- **v1's Cython layer is gone.** `orso/compute` (compiled.pyx, column_encodings.pyx,
  their C++ header and docs) is deleted. Its remaining jobs are native
  (`native/columns.h`): `collect()` writes straight into the numpy object array's buffer,
  as v1's typed memoryview did, with a one-column fast path (8–13% faster than v1);
  `extract_columns` and `column_widths` replace v1's helpers for the v1 export fallback and
  `display()`. `process_table` (tables of a different width than the first) is Python.
  The RLE/dictionary encoders nothing used are gone; `RLEColumn`, `DictionaryColumn` and
  `SparseColumn` remain.
- **No Cython or numpy at build time.** The build needs setuptools, wheel and nanobind.
  `setup.py`, `pyproject.toml`, `build.sh`, the Makefile and both workflows are updated.
- **Runtime requirements are data-expectations, ormsgpack and pyarrow.** numpy is
  optional (`orso[numpy]`): only `collect()` and the virtual columns' `materialize()`
  return arrays, and they raise `MissingDependencyError` without it; everywhere else orso
  only looks for numpy values when numpy is already imported. mabel's own numpy import is
  guarded and pyarrow 26 does not need numpy. xxhash is replaced by the vendored
  `xxhash.h` (`CityHash64`, `CityHash32` and `DataFrame.__hash__` give the same values,
  checked on 5,000 inputs). Verified end to end with numpy, xxhash and pandas blocked.
- **The sdist was broken and is fixed.** `MANIFEST.in` shipped `orso/*.pyx` but not
  `native/`, so building from the sdist would have failed. It now carries all 55 native
  files; an sdist was built, extracted, compiled and imported.
- **Removed**: the converters that only raised `NotImplementedError` (`from_pandas`,
  `from_polars`, `to_csv`, `from_csv`, `to_parquet`, `from_parquet`, `to_jsonl`,
  `from_jsonl`); `tests/test_benchmark_suite.py`, `examples/` (both drove the old
  benchmark suite or the deleted encoders) and `compare_versions.sh` (it pip-installed
  other orso releases into the current environment), all superseded by `benchmarks/`.
- **Tests**: the Cython-facing tests were ported onto the public paths
  (`tests/test_collect.py`, both frame kinds; `tests/test_field_extractor.py` through the
  native dict reader; v1's record reader reproduced in Python as the oracle);
  `tests/test_native_hash.py` pins hash values. Full suite 736 passed, 6 skipped.

Measured against v1 (0.0.246) built and run back to back, control drift +4.4%:
**51 cases faster, 0 slower, 7 unchanged; geometric mean 64.4% faster.**


## 18. Consumer suites (2026-10-16)

Delivery step 8, run early so phase 7 starts from a known consumer baseline. Each
consumer's own test suite ran twice in one scratch Python 3.14 venv with no orso
installed: once with v1 (0.0.246, built from c09cc35) on `PYTHONPATH`, once with this
checkout. Outcomes were compared per test, not by totals.

| consumer | tests | v1 | v2 | passed on v1, not v2 |
|---|---|---|---|---|
| mabel | 192 | 190 passed, 2 failed | identical | 0 |
| opteryx 0.26.8 (unit, sql_battery, wildcard) | 10,431 | 6,865 passed | 6,864 passed | 1 |

- **mabel**: the two failures (`test_reader_validation.py::test_dataset_prefix_validator`,
  `test_reader_all_good`) are environmental and fail identically on v1. Wall time 51.7 s on
  v1, 35.7 s on v2.
- **opteryx**: the shared failures are environmental and identical on both versions: optional
  connectors absent from the venv (pyiceberg, psycopg2, cassandra, google-cloud, polars,
  redis, vortex) and opteryx's query builder calling `textwrap.indent()` in a way 3.14
  rejects.
- **The one difference is §8 fix 11, which conflicts with a consumer's test.** opteryx pins
  `TRY_CAST('2021-02-21T12:00:00+01:00' AS TIMESTAMP) == 2021-02-21 12:00:00` with the
  comment "Timezone ignored" (`tests/unit/core/test_cast.py`). v1 ignored every offset
  and kept the wall-clock time. v2 applies it and returns 11:00 UTC. This is the change the
  fix intends, but opteryx's TIMESTAMP is naive and its own test documents the v1
  behaviour, so either that test changes or fix 11 is reverted before tagging. **Open.**
  - Related: when draken's parser refuses an offset form (`+01`, hours only), v2 falls back to
    v1's parser, which drops the offset. So v2 is not yet consistent about offsets either way.
