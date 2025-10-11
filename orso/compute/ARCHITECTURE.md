# Column Encodings Architecture

## Overview

This document describes the architecture of the C++ and Cython column encoding implementations in Orso.

## Design Principles

1. **Performance**: All critical paths are implemented in Cython with C/C++ optimizations
2. **Type Safety**: Separate implementations for each physical type to avoid dynamic dispatch overhead
3. **Arrow Compatibility**: Designed to integrate with Arrow's columnar format
4. **Extensibility**: Template-based C++ headers allow easy addition of new types

## Architecture

### Layers

```
┌─────────────────────────────────────────┐
│   Python Layer (schema.py)              │
│   - RLEColumn, DictionaryColumn         │
│   - High-level interface                │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│   Cython Layer (column_encodings.pyx)   │
│   - Type-specific implementations       │
│   - Generic dispatch functions          │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│   C++ Layer (column_types.h)            │
│   - Template-based column types         │
│   - Core encoding algorithms            │
└─────────────────────────────────────────┘
```

### Component Breakdown

#### 1. C++ Templates (`column_types.h`)

Provides template-based implementations for column encodings:

```cpp
template<typename T>
class RLEColumn {
    std::vector<T> values_;
    std::vector<uint32_t> lengths_;
    
    void encode(const T* data, size_t size);
    std::vector<T> decode() const;
};
```

**Benefits:**
- Type-safe implementations
- Compiler optimizations
- Reusable across different types
- Ready for future C++ integration

#### 2. Cython Implementations (`column_encodings.pyx`)

Type-specific Cython functions for each physical type:

```python
def rle_encode_int32(cnp.ndarray[int32_t, ndim=1] data):
    # Optimized implementation for int32
    ...

def rle_encode(cnp.ndarray data):
    # Generic dispatcher based on dtype
    if data.dtype == np.int32:
        return rle_encode_int32(data)
    ...
```

**Benefits:**
- Direct C-level performance
- NumPy array integration
- Type-specific optimizations
- Python-friendly API

#### 3. Python Interface (`__init__.py`, `schema.py`)

High-level Python API that integrates with existing Orso schema:

```python
from orso.compute import rle_encode, rle_decode, dict_encode, dict_decode

# Seamlessly works with existing column types
rle_column = RLEColumn(name="test", type=OrsoTypes.INTEGER, values=data)
```

## Physical Types

The implementation supports these physical types as required:

| Type | Bits | RLE | Dictionary | Use Case |
|------|------|-----|------------|----------|
| int8 | 8 | ✓ | - | Small integers, flags |
| int16 | 16 | ✓ | - | Medium integers |
| int32 | 32 | ✓ | ✓ | Standard integers |
| int64 | 64 | ✓ | ✓ | Large integers, IDs |
| float32 | 32 | ✓ | - | Floating point |
| float64 | 64 | ✓ | - | Double precision |
| object | var | - | ✓ | Strings, variable-width |

## Encoding Schemes

### Run-Length Encoding (RLE)

Compresses sequences of repeated values:

```
Input:  [1, 1, 1, 2, 2, 3, 3, 3, 3]
Values: [1, 2, 3]
Lengths: [3, 2, 4]
```

**When to use:**
- Sequential data with runs of repeated values
- Time-series with stable periods
- Status codes that change infrequently

**Compression ratio:** Depends on run length (higher is better)

### Dictionary Encoding

Stores unique values once and uses indices:

```
Input:       [1, 3, 2, 2, 3, 1]
Dictionary:  [1, 3, 2]
Indices:     [0, 1, 2, 2, 1, 0]
```

**When to use:**
- Low cardinality columns
- String columns with repeated values
- Categorical data

**Compression ratio:** Better with fewer unique values

## Performance Characteristics

### Time Complexity

| Operation | RLE | Dictionary |
|-----------|-----|------------|
| Encode | O(n) | O(n) |
| Decode | O(n) | O(n) |
| Random Access | O(log r) | O(1) |

Where:
- n = number of elements
- r = number of runs (RLE)

### Space Complexity

| Encoding | Space Used |
|----------|------------|
| RLE | O(r + sizeof(T) * r) where r = number of runs |
| Dictionary | O(u * sizeof(T) + n * sizeof(uint32)) where u = unique values |

## Arrow Integration

The encodings are designed for Arrow compatibility:

### RLE Mapping
- Maps to Arrow's Run-End Encoded (REE) arrays
- Compatible with Arrow's compression schemes
- Supports zero-copy when possible

### Dictionary Mapping
- Maps to Arrow's Dictionary type
- Compatible with Arrow IPC format
- Preserves dictionary across batches

## Build System

### Compilation

The module is compiled via `setup.py`:

```python
Extension(
    name="orso.compute.column_encodings",
    sources=["orso/compute/column_encodings.pyx"],
    include_dirs=[numpy.get_include(), "orso/compute"],
    extra_compile_args=["-O2", "-march=native"],
    language="c++",
)
```

**Optimization flags:**
- `-O2`: Standard optimizations
- `-march=native`: CPU-specific optimizations
- `language="c++"`: Enable C++ features

### Testing

Comprehensive test coverage in `test_column_encodings.py`:

```
TestRLEEncoding:
  - Basic functionality for all types
  - Edge cases (empty, single value, all same)
  - Generic dispatch
  
TestDictionaryEncoding:
  - Basic functionality
  - String/object support
  - Error handling

TestRealWorldScenarios:
  - Integration with schema
  - Performance benchmarks
  - Compression ratios
```

## Future Extensions

### Planned Enhancements

1. **Additional Types**
   - Boolean (bit-packed)
   - Timestamp/Date
   - Decimal

2. **Advanced Encodings**
   - Delta encoding for sequences
   - Frame-of-Reference (FOR)
   - Bit-packing

3. **Arrow Integration**
   - Direct Arrow array creation
   - IPC serialization
   - Flight RPC support

4. **Performance**
   - SIMD optimizations
   - Parallel encoding/decoding
   - GPU acceleration

## Usage Guidelines

### When to Use RLE

✅ **Good for:**
- Sorted data
- Status columns with long stable periods
- Sparse data with default values
- Time-series with plateaus

❌ **Avoid for:**
- Random/highly variable data
- Short run lengths (< 3 elements)
- Already compressed data

### When to Use Dictionary

✅ **Good for:**
- Low cardinality columns (< 1000 unique values)
- String columns with repetition
- Categorical data
- Enum-like values

❌ **Avoid for:**
- High cardinality columns
- Unique identifiers
- Continuous numeric data

### Choosing Physical Types

| Data Range | Recommended Type |
|------------|------------------|
| 0-255 | int8 |
| -32K to 32K | int16 |
| -2B to 2B | int32 |
| > 2B | int64 |
| Decimals (low precision) | float32 |
| Decimals (high precision) | float64 |
| Strings | object (with dict encoding) |

## References

- [Apache Arrow Encoding](https://arrow.apache.org/docs/format/Columnar.html)
- [Cython Documentation](https://cython.readthedocs.io/)
- [NumPy C API](https://numpy.org/doc/stable/reference/c-api/)
