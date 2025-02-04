import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.compute.compiled import extract_dict_columns
import random

def test_extract_dict_columns_basic():
    data = {'a': 1, 'b': 2, 'c': 3}
    fields = ('a', 'b', 'c')
    result = extract_dict_columns(data, fields)
    assert result == (1, 2, 3), result

def test_extract_dict_columns_missing_fields():
    data = {'a': 1, 'b': 2, 'c': 3}
    fields = ('a', 'd', 'c')
    result = extract_dict_columns(data, fields)
    assert result == (1, None, 3)

def test_extract_dict_columns_empty_data():
    data = {}
    fields = ('a', 'b', 'c')
    result = extract_dict_columns(data, fields)
    assert result == (None, None, None)

def test_extract_dict_columns_empty_fields():
    data = {'a': 1, 'b': 2, 'c': 3}
    fields = ()
    result = extract_dict_columns(data, fields)
    assert result == ()

def test_extract_dict_columns_all_fields_missing():
    data = {'a': 1, 'b': 2, 'c': 3}
    fields = ('x', 'y', 'z')
    result = extract_dict_columns(data, fields)
    assert result == (None, None, None)

def test_extract_dict_columns_large_dataset():
    data = {str(i): i for i in range(10000)}
    fields = tuple(str(i) for i in range(10000))
    result = extract_dict_columns(data, fields)
    assert result == tuple(range(10000))

def test_extract_dict_columns_partial_missing_fields():
    data = {str(i): i for i in range(10000)}
    fields = tuple(str(i) for i in range(9995, 10005))
    result = extract_dict_columns(data, fields)
    assert result == (9995, 9996, 9997, 9998, 9999, None, None, None, None, None)

def test_extract_dict_columns_mixed_data_types():
    data = {'a': 1, 'b': "text", 'c': 3.14, 'd': [1, 2, 3], 'e': {'key': 'value'}}
    fields = ('a', 'b', 'c', 'd', 'e')
    result = extract_dict_columns(data, fields)
    assert result == (1, "text", 3.14, [1, 2, 3], {'key': 'value'})

def test_extract_dict_columns_repeated_fields():
    data = {'a': 1, 'b': 2, 'c': 3}
    fields = ('a', 'b', 'a')
    result = extract_dict_columns(data, fields)
    assert result == (1, 2, 1)

def test_extract_dict_columns_nested_structures():
    data = {'a': {'b': {'c': 1}}, 'x': [1, 2, 3], 'y': (4, 5, 6)}
    fields = ('a', 'x', 'y')
    result = extract_dict_columns(data, fields)
    assert result == ({'b': {'c': 1}}, [1, 2, 3], (4, 5, 6))

def test_extract_dict_columns_with_none_values():
    data = {'a': None, 'b': 2, 'c': None}
    fields = ('a', 'b', 'c')
    result = extract_dict_columns(data, fields)
    assert result == (None, 2, None)

def test_extract_dict_columns_empty_data_and_fields():
    data = {}
    fields = ()
    result = extract_dict_columns(data, fields)
    assert result == ()

def test_extract_dict_columns_unicode_keys():
    data = {'α': 1, 'β': 2, 'γ': 3}
    fields = ('α', 'β', 'γ', 'δ')
    result = extract_dict_columns(data, fields)
    assert result == (1, 2, 3, None)

def test_extract_dict_columns_special_characters():
    data = {'!': 1, '@': 2, '#': 3}
    fields = ('!', '@', '#')
    result = extract_dict_columns(data, fields)
    assert result == (1, 2, 3)

def test_extract_dict_columns_very_large_values():
    large_value = "x" * 10**6
    data = {'a': large_value, 'b': large_value, 'c': large_value}
    fields = ('a', 'b', 'c')
    result = extract_dict_columns(data, fields)
    assert result == (large_value, large_value, large_value)

def test_extract_dict_columns_order_consistency():
    data = {'a': 1, 'b': 2, 'c': 3, 'd': 4}
    fields = ('a', 'b', 'c', 'd')

    # Randomize the dictionary's order multiple times
    for _ in range(100):
        items = list(data.items())
        random.shuffle(items)
        randomized_data = dict(items)
        
        result = extract_dict_columns(randomized_data, fields)
        assert result == (1, 2, 3, 4), f"Failed with data: {randomized_data}"

def test_extract_dict_columns_partial_fields_order_consistency():
    data = {'a': 1, 'b': 2, 'c': 3, 'd': 4}
    fields = ('d', 'b', 'a')

    # Randomize the dictionary's order multiple times
    for _ in range(100):
        items = list(data.items())
        random.shuffle(items)
        randomized_data = dict(items)
        
        result = extract_dict_columns(randomized_data, fields)
        assert result == (4, 2, 1), f"Failed with data: {randomized_data}"

def test_extract_dict_columns_randomized_order_with_missing_keys():
    data = {'a': 1, 'b': 2, 'c': 3}
    fields = ('c', 'a', 'd', 'b')

    # Randomize the dictionary's order multiple times
    for _ in range(100):
        items = list(data.items())
        random.shuffle(items)
        randomized_data = dict(items)
        
        result = extract_dict_columns(randomized_data, fields)
        assert result == (3, 1, None, 2), f"Failed with data: {randomized_data}"

def test_extract_dict_columns_empty_dict_with_random_fields():
    data = {}
    fields = ('a', 'b', 'c')

    result = extract_dict_columns(data, fields)
    assert result == (None, None, None)
    
    # Even with different orders in fields, the result should still be None for each missing key
    randomized_fields = list(fields)
    random.shuffle(randomized_fields)
    result = extract_dict_columns(data, tuple(randomized_fields))
    assert result == (None, None, None), f"Failed with fields: {randomized_fields}"

def test_extract_dict_columns_with_same_field_order():
    data = {'a': 1, 'b': 2, 'c': 3, 'd': 4}
    fields1 = ('a', 'b', 'c', 'd')
    fields2 = ('d', 'c', 'b', 'a')

    result1 = extract_dict_columns(data, fields1)
    result2 = extract_dict_columns(data, fields2)
    
    assert result1 == (1, 2, 3, 4)
    assert result2 == (4, 3, 2, 1)

def test_extract_dict_columns_sparse_dict():
    data = {'a': 1}
    fields = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i')
    result = extract_dict_columns(data, fields)
    assert result == (1, None, None, None, None, None, None, None, None)

def test_extract_dict_columns_immutability():
    data = {'a': 1, 'b': 2, 'c': 3}
    original_data = data.copy()
    fields = ('a', 'b', 'c')
    extract_dict_columns(data, fields)
    assert data == original_data, "Function modified input data!"

def test_extract_dict_columns_stress_test():
    data = {str(i): i for i in range(10000)}
    fields = tuple(str(i) for i in range(20000))
    result = extract_dict_columns(data, fields)
    assert result[:10000] == tuple(range(10000)) and result[10000:] == (None,) * 10000

def test_extract_dict_columns_duplicate_missing_fields():
    data = {'a': 1, 'b': 2}
    fields = ('a', 'x', 'x', 'b')
    result = extract_dict_columns(data, fields)
    assert result == (1, None, None, 2)

def test_extract_dict_columns_case_sensitivity():
    data = {'A': 1, 'b': 2}
    fields = ('a', 'b')
    result = extract_dict_columns(data, fields)
    assert result == (None, 2)

def test_extract_dict_columns_whitespace_keys():
    data = {' a': 1, 'b ': 2}
    fields = ('a', 'b ')
    result = extract_dict_columns(data, fields)
    assert result == (None, 2)

def test_extract_dict_columns_non_string_keys():
    data = {None: 1, 42: "forty-two"}
    fields = (None, 42, "missing")
    result = extract_dict_columns(data, fields)
    assert result == (1, "forty-two", None)

def test_extract_dict_columns_deeply_nested_keys():
    data = {'a': {'b': {'c': 1}}}
    fields = ('a.b.c', 'a')
    result = extract_dict_columns(data, fields)
    assert result == (None, {'b': {'c': 1}})

def test_extract_dict_columns_large_dataset_with_missing():
    data = {str(i): i for i in range(10**6)}
    fields = ('1000001', '500000', '999999')
    result = extract_dict_columns(data, fields)
    assert result == (None, 500000, 999999)

if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
