import os
import sys

import pytest
import numpy as np

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.compute.column_encodings import (
    rle_encode, rle_decode,
    rle_encode_int8, rle_decode_int8,
    rle_encode_int16, rle_decode_int16,
    rle_encode_int32, rle_decode_int32,
    rle_encode_int64, rle_decode_int64,
    dict_encode, dict_decode,
    dict_encode_int32, dict_decode_int32,
    dict_encode_int64, dict_decode_int64,
)


class TestRLEEncoding:
    """Test Run-Length Encoding implementations"""
    
    def test_rle_int8_basic(self):
        data = np.array([1, 1, 1, 2, 2, 3], dtype=np.int8)
        values, lengths = rle_encode_int8(data)
        
        assert list(values) == [1, 2, 3]
        assert list(lengths) == [3, 2, 1]
        
        decoded = rle_decode_int8(values, lengths)
        np.testing.assert_array_equal(decoded, data)
    
    def test_rle_int8_empty(self):
        data = np.array([], dtype=np.int8)
        values, lengths = rle_encode_int8(data)
        
        assert len(values) == 0
        assert len(lengths) == 0
        
        decoded = rle_decode_int8(values, lengths)
        np.testing.assert_array_equal(decoded, data)
    
    def test_rle_int8_single(self):
        data = np.array([5], dtype=np.int8)
        values, lengths = rle_encode_int8(data)
        
        assert list(values) == [5]
        assert list(lengths) == [1]
        
        decoded = rle_decode_int8(values, lengths)
        np.testing.assert_array_equal(decoded, data)
    
    def test_rle_int8_all_same(self):
        data = np.array([7, 7, 7, 7, 7], dtype=np.int8)
        values, lengths = rle_encode_int8(data)
        
        assert list(values) == [7]
        assert list(lengths) == [5]
        
        decoded = rle_decode_int8(values, lengths)
        np.testing.assert_array_equal(decoded, data)
    
    def test_rle_int16(self):
        data = np.array([100, 100, 200, 200, 200], dtype=np.int16)
        values, lengths = rle_encode_int16(data)
        
        assert list(values) == [100, 200]
        assert list(lengths) == [2, 3]
        
        decoded = rle_decode_int16(values, lengths)
        np.testing.assert_array_equal(decoded, data)
    
    def test_rle_int32(self):
        data = np.array([1000, 1000, 2000, 3000, 3000], dtype=np.int32)
        values, lengths = rle_encode_int32(data)
        
        assert list(values) == [1000, 2000, 3000]
        assert list(lengths) == [2, 1, 2]
        
        decoded = rle_decode_int32(values, lengths)
        np.testing.assert_array_equal(decoded, data)
    
    def test_rle_int64(self):
        data = np.array([1000000, 1000000, 2000000], dtype=np.int64)
        values, lengths = rle_encode_int64(data)
        
        assert list(values) == [1000000, 2000000]
        assert list(lengths) == [2, 1]
        
        decoded = rle_decode_int64(values, lengths)
        np.testing.assert_array_equal(decoded, data)
    
    def test_rle_generic_int32(self):
        data = np.array([5, 5, 5, 10, 10, 15], dtype=np.int32)
        values, lengths = rle_encode(data)
        
        assert list(values) == [5, 10, 15]
        assert list(lengths) == [3, 2, 1]
        
        decoded = rle_decode(values, lengths)
        np.testing.assert_array_equal(decoded, data)
    
    def test_rle_generic_int64(self):
        data = np.array([100, 100, 200, 300, 300, 300], dtype=np.int64)
        values, lengths = rle_encode(data)
        
        assert list(values) == [100, 200, 300]
        assert list(lengths) == [2, 1, 3]
        
        decoded = rle_decode(values, lengths)
        np.testing.assert_array_equal(decoded, data)
    
    def test_rle_unsupported_dtype(self):
        data = np.array([1.0, 2.0, 3.0], dtype=np.float64)
        
        with pytest.raises(TypeError, match="Unsupported dtype"):
            rle_encode(data)


class TestDictionaryEncoding:
    """Test Dictionary Encoding implementations"""
    
    def test_dict_int32_basic(self):
        data = np.array([1, 3, 2, 2, 3, 1], dtype=np.int32)
        dictionary, indices = dict_encode_int32(data)
        
        # Check that dictionary contains unique values
        unique_vals = sorted(dictionary)
        assert unique_vals == [1, 2, 3]
        
        # Decode and verify
        decoded = dict_decode_int32(dictionary, indices)
        np.testing.assert_array_equal(decoded, data)
    
    def test_dict_int32_empty(self):
        data = np.array([], dtype=np.int32)
        dictionary, indices = dict_encode_int32(data)
        
        assert len(dictionary) == 0
        assert len(indices) == 0
        
        decoded = dict_decode_int32(dictionary, indices)
        np.testing.assert_array_equal(decoded, data)
    
    def test_dict_int32_single(self):
        data = np.array([42], dtype=np.int32)
        dictionary, indices = dict_encode_int32(data)
        
        assert list(dictionary) == [42]
        assert list(indices) == [0]
        
        decoded = dict_decode_int32(dictionary, indices)
        np.testing.assert_array_equal(decoded, data)
    
    def test_dict_int32_all_same(self):
        data = np.array([5, 5, 5, 5], dtype=np.int32)
        dictionary, indices = dict_encode_int32(data)
        
        assert list(dictionary) == [5]
        assert list(indices) == [0, 0, 0, 0]
        
        decoded = dict_decode_int32(dictionary, indices)
        np.testing.assert_array_equal(decoded, data)
    
    def test_dict_int64(self):
        data = np.array([100, 200, 100, 300, 200], dtype=np.int64)
        dictionary, indices = dict_encode_int64(data)
        
        # Check that dictionary contains unique values
        unique_vals = sorted(dictionary)
        assert unique_vals == [100, 200, 300]
        
        # Decode and verify
        decoded = dict_decode_int64(dictionary, indices)
        np.testing.assert_array_equal(decoded, data)
    
    def test_dict_generic_int32(self):
        data = np.array([10, 20, 10, 30, 20, 10], dtype=np.int32)
        dictionary, indices = dict_encode(data)
        
        decoded = dict_decode(dictionary, indices)
        np.testing.assert_array_equal(decoded, data)
    
    def test_dict_generic_int64(self):
        data = np.array([1000, 2000, 1000, 3000], dtype=np.int64)
        dictionary, indices = dict_encode(data)
        
        decoded = dict_decode(dictionary, indices)
        np.testing.assert_array_equal(decoded, data)
    
    def test_dict_unsupported_dtype(self):
        data = np.array([1.0, 2.0, 3.0], dtype=np.float64)
        
        with pytest.raises(TypeError, match="Unsupported dtype"):
            dict_encode(data)
    
    def test_dict_decode_invalid_index(self):
        dictionary = np.array([1, 2, 3], dtype=np.int32)
        indices = np.array([0, 1, 5], dtype=np.uint32)  # 5 is out of range
        
        with pytest.raises(IndexError, match="out of range"):
            dict_decode_int32(dictionary, indices)


class TestRealWorldScenarios:
    """Test with real-world data patterns"""
    
    def test_rle_with_month_lengths(self):
        # Similar to the original test_rle_column
        month_lengths = np.array([31, 30, 31, 31, 30, 31, 31, 31, 31], dtype=np.int32)
        values, lengths = rle_encode_int32(month_lengths)
        
        assert list(values) == [31, 30, 31, 30, 31]
        assert list(lengths) == [1, 1, 2, 1, 4]
        
        decoded = rle_decode_int32(values, lengths)
        np.testing.assert_array_equal(decoded, month_lengths)
    
    def test_dict_with_repeated_values(self):
        # Similar to the original test_dict_column
        month_lengths = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31], dtype=np.int32)
        dictionary, indices = dict_encode_int32(month_lengths)
        
        # Dictionary should contain unique values
        unique_vals = sorted(dictionary)
        assert unique_vals == [28, 30, 31]
        
        # Decode should match original
        decoded = dict_decode_int32(dictionary, indices)
        np.testing.assert_array_equal(decoded, month_lengths)
    
    def test_rle_performance_large_runs(self):
        # Test with large runs of repeated values
        data = np.array([1] * 1000 + [2] * 500 + [3] * 250, dtype=np.int32)
        values, lengths = rle_encode_int32(data)
        
        assert len(values) == 3
        assert list(values) == [1, 2, 3]
        assert list(lengths) == [1000, 500, 250]
        
        decoded = rle_decode_int32(values, lengths)
        np.testing.assert_array_equal(decoded, data)
    
    def test_dict_performance_few_uniques(self):
        # Test dictionary encoding with very few unique values
        data = np.array([1, 2, 3, 1, 2, 3, 1, 2, 3] * 100, dtype=np.int32)
        dictionary, indices = dict_encode_int32(data)
        
        assert len(dictionary) == 3
        assert sorted(dictionary) == [1, 2, 3]
        assert len(indices) == 900
        
        decoded = dict_decode_int32(dictionary, indices)
        np.testing.assert_array_equal(decoded, data)
