"""
Integration example showing how to use the new C++/Cython column encodings
with the existing Orso schema column types.
"""

import numpy as np

from orso.compute import dict_decode
from orso.compute import dict_encode
from orso.compute import rle_decode
from orso.compute import rle_encode
from orso.schema import DictionaryColumn
from orso.schema import RLEColumn
from orso.types import OrsoTypes


def example_rle_integration():
    """
    Example showing how RLE encoding can be integrated with schema columns.
    """
    print("=== RLE Integration Example ===\n")
    
    # Create data with repeated values
    data = np.array([1, 1, 1, 2, 2, 3, 3, 3, 3], dtype=np.int32)
    print(f"Original data: {data}")
    
    # Use the C++/Cython RLE encoder
    values, lengths = rle_encode(data)
    print(f"RLE encoded - Values: {values}, Lengths: {lengths}")
    
    # Decode back
    decoded = rle_decode(values, lengths)
    print(f"Decoded data: {decoded}")
    print(f"Match: {np.array_equal(data, decoded)}\n")
    
    # Compare with Python RLEColumn
    rle_column = RLEColumn(name="test", type=OrsoTypes.INTEGER, values=data)
    print(f"Python RLEColumn - Values: {rle_column.values}, Lengths: {rle_column.lengths}")
    materialized = rle_column.materialize()
    print(f"Materialized: {materialized}")
    print(f"Match: {np.array_equal(data, materialized)}\n")


def example_dict_integration():
    """
    Example showing how dictionary encoding can be integrated with schema columns.
    """
    print("=== Dictionary Integration Example ===\n")
    
    # Create data with few unique values
    data = np.array([1, 3, 2, 2, 3, 1, 1], dtype=np.int32)
    print(f"Original data: {data}")
    
    # Use the C++/Cython dictionary encoder
    dictionary, indices = dict_encode(data)
    print(f"Dict encoded - Dictionary: {dictionary}, Indices: {indices}")
    
    # Decode back
    decoded = dict_decode(dictionary, indices)
    print(f"Decoded data: {decoded}")
    print(f"Match: {np.array_equal(data, decoded)}\n")
    
    # Compare with Python DictionaryColumn
    dict_column = DictionaryColumn(name="test", type=OrsoTypes.INTEGER, values=data)
    print(f"Python DictionaryColumn - Values: {dict_column.values}, Encoding: {dict_column.encoding}")
    materialized = dict_column.materialize()
    print(f"Materialized: {materialized}")
    print(f"Match: {np.array_equal(data, materialized)}\n")


def example_string_encoding():
    """
    Example showing variable-width string encoding.
    """
    print("=== String Dictionary Encoding Example ===\n")
    
    # Month names (variable-width strings)
    months = np.array(["Jan", "Feb", "Mar", "Jan", "Feb", "Mar", "Jan"], dtype=object)
    print(f"Original data: {months}")
    
    # Encode with dictionary
    dictionary, indices = dict_encode(months)
    print(f"Dictionary: {dictionary}")
    print(f"Indices: {indices}")
    
    # Decode back
    decoded = dict_decode(dictionary, indices)
    print(f"Decoded data: {decoded}")
    print(f"Match: {np.array_equal(months, decoded)}\n")
    
    # Calculate compression ratio
    original_size = sum(len(s) for s in months)
    encoded_size = sum(len(s) for s in dictionary) + len(indices) * 4  # 4 bytes per uint32
    print(f"Original size (approx): {original_size} bytes")
    print(f"Encoded size (approx): {encoded_size} bytes")
    print(f"Compression ratio: {original_size / encoded_size:.2f}x\n")


def example_performance_comparison():
    """
    Performance comparison between different physical types.
    """
    print("=== Physical Type Comparison ===\n")
    
    # Large dataset with repetition
    size = 10000
    data_pattern = [1, 1, 1, 2, 2, 3]
    
    # Test different physical types
    for dtype in [np.int8, np.int16, np.int32, np.int64]:
        data = np.array(data_pattern * (size // len(data_pattern)), dtype=dtype)
        values, lengths = rle_encode(data)
        
        compression_ratio = len(data) / len(values)
        print(f"{dtype.__name__:8s}: {len(data):6d} elements -> {len(values):4d} runs (ratio: {compression_ratio:.1f}x)")
    
    print()


if __name__ == "__main__":
    example_rle_integration()
    example_dict_integration()
    example_string_encoding()
    example_performance_comparison()
    
    print("=== All Examples Complete ===")
