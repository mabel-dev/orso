# Column Encodings

This module provides high-performance C++ and Cython implementations of column encoding schemes for the Orso data store.

## Features

### Run-Length Encoding (RLE)

RLE is optimized for sequences of repeated values. It stores each unique run of values as a (value, length) pair.

**Supported Physical Types:**
- 8-bit integers (`int8`)
- 16-bit integers (`int16`)
- 32-bit integers (`int32`)
- 64-bit integers (`int64`)
- 32-bit floats (`float32`)
- 64-bit floats (`float64`)

**Example:**
```python
import numpy as np
from orso.compute import rle_encode, rle_decode

# Encode data with repeated values
data = np.array([1, 1, 1, 2, 2, 3], dtype=np.int32)
values, lengths = rle_encode(data)
# values: [1, 2, 3]
# lengths: [3, 2, 1]

# Decode back to original
decoded = rle_decode(values, lengths)
# decoded: [1, 1, 1, 2, 2, 3]
```

### Dictionary Encoding

Dictionary encoding is optimized for columns with a small number of unique values. It stores a dictionary of unique values and an array of indices.

**Supported Physical Types:**
- 32-bit integers (`int32`)
- 64-bit integers (`int64`)
- Variable-width data (`object` - strings, etc.)

**Example:**
```python
import numpy as np
from orso.compute import dict_encode, dict_decode

# Encode data with few unique values
data = np.array([1, 3, 2, 2, 3, 1], dtype=np.int32)
dictionary, indices = dict_encode(data)
# dictionary: [1, 3, 2] (or similar, order may vary)
# indices: [0, 1, 2, 2, 1, 0] (indices into dictionary)

# Decode back to original
decoded = dict_decode(dictionary, indices)
# decoded: [1, 3, 2, 2, 3, 1]
```

**String/Variable-width Example:**
```python
import numpy as np
from orso.compute import dict_encode, dict_decode

# Encode string data
data = np.array(["apple", "banana", "apple", "cherry", "banana"], dtype=object)
dictionary, indices = dict_encode(data)
# dictionary: unique strings
# indices: indices into dictionary

# Decode back to original
decoded = dict_decode(dictionary, indices)
# decoded: ["apple", "banana", "apple", "cherry", "banana"]
```

## API Reference

### RLE Functions

- `rle_encode(data)` - Generic RLE encoder that dispatches based on dtype
- `rle_decode(values, lengths)` - Generic RLE decoder
- `rle_encode_int8(data)` - RLE encoder for 8-bit integers
- `rle_decode_int8(values, lengths)` - RLE decoder for 8-bit integers
- `rle_encode_int16(data)` - RLE encoder for 16-bit integers
- `rle_decode_int16(values, lengths)` - RLE decoder for 16-bit integers
- `rle_encode_int32(data)` - RLE encoder for 32-bit integers
- `rle_decode_int32(values, lengths)` - RLE decoder for 32-bit integers
- `rle_encode_int64(data)` - RLE encoder for 64-bit integers
- `rle_decode_int64(values, lengths)` - RLE decoder for 64-bit integers
- `rle_encode_float32(data)` - RLE encoder for 32-bit floats
- `rle_decode_float32(values, lengths)` - RLE decoder for 32-bit floats
- `rle_encode_float64(data)` - RLE encoder for 64-bit floats
- `rle_decode_float64(values, lengths)` - RLE decoder for 64-bit floats

### Dictionary Functions

- `dict_encode(data)` - Generic dictionary encoder that dispatches based on dtype
- `dict_decode(dictionary, indices)` - Generic dictionary decoder
- `dict_encode_int32(data)` - Dictionary encoder for 32-bit integers
- `dict_decode_int32(dictionary, indices)` - Dictionary decoder for 32-bit integers
- `dict_encode_int64(data)` - Dictionary encoder for 64-bit integers
- `dict_decode_int64(dictionary, indices)` - Dictionary decoder for 64-bit integers
- `dict_encode_object(data)` - Dictionary encoder for variable-width data (strings)
- `dict_decode_object(dictionary, indices)` - Dictionary decoder for variable-width data

## Physical Types

The implementation supports the following physical types as required:

1. **8-bit** - `int8` (via RLE)
2. **16-bit** - `int16` (via RLE)
3. **32-bit** - `int32` (via RLE and Dictionary), `float32` (via RLE)
4. **64-bit** - `int64` (via RLE and Dictionary), `float64` (via RLE)
5. **Fixed-width arrays** - Supported through numpy array types
6. **Variable-width** - `object` type (via Dictionary for strings, etc.)

## Performance

All encoding functions are implemented in Cython with C/C++ optimizations:
- Compiled with `-O2` optimization level
- Type-specific implementations avoid dynamic dispatch
- Memory-efficient representation reduces storage requirements
- Fast encode/decode operations suitable for Arrow-compatible stores

## Arrow Compatibility

These encodings are designed to be compatible with Arrow's columnar format:
- RLE encoding maps to Arrow's RLE encoding
- Dictionary encoding maps to Arrow's Dictionary type
- Physical types align with Arrow's type system

## Future Extensions

The C++ header `column_types.h` provides templates that can be extended to support:
- Float types (float32, float64)
- Additional physical types (variable-width strings, etc.)
- Custom encoding schemes
- Integration with Arrow memory pools
