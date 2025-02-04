import os
import sys
import numpy

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.compute.compiled import collect_cython

def test_collector():
    columns = collect_cython([(1, 2), (2, 1), (7, 8)], numpy.array([1, 0], dtype=numpy.int32))
    assert len(columns) == 2
    assert len(columns[0]) == 3
    assert sum(columns[0]) == 11
    assert sum(columns[1]) == 10

def test_collector_empty_input():
    columns = collect_cython([], numpy.array([], dtype=numpy.int32))
    assert len(columns) == 0, len(columns)

def test_collector_single_tuple():
    columns = collect_cython([(5, 10)], numpy.array([1], dtype=numpy.int32))
    assert len(columns) == 1, len(columns)
    assert columns[0] == [10]

def test_collector_large_data():
    data = [(i, i * 2) for i in range(10000)]
    index = numpy.array([1, 0], dtype=numpy.int32)
    columns = collect_cython(data, index)
    assert len(columns) == 2
    assert len(columns[0]) == 10000
    assert sum(columns[0]) == sum(i * 2 for i in range(10000))
    assert sum(columns[1]) == sum(range(10000))


def test_collector_non_integer_index():
    data = [(1, 2), (3, 4)]
    index = numpy.array([0.5, 1.5], dtype=numpy.float64)
    try:
        collect_cython(data, index)
        assert False, "Expected a ValueError"
    except ValueError:
        pass

def test_collector_negative_index():
    data = [(1, 2), (3, 4)]
    index = numpy.array([-1, 0], dtype=numpy.int32)
    try:
        collect_cython(data, index)
        assert False, "Expected an IndexError"
    except IndexError:
        pass

def test_collector_large_index_values():
    data = [(1, 2), (3, 4)]
    index = numpy.array([100, 200], dtype=numpy.int32)
    try:
        collect_cython(data, index)
        assert False, "Expected an IndexError"
    except IndexError:
        pass

def test_collector_duplicate_indices():
    data = [(1, 2), (3, 4), (5, 6)]
    index = numpy.array([1, 1, 0], dtype=numpy.int32)
    columns = collect_cython(data, index)
    assert len(columns) == 3
    assert sum(columns[0]) == 12, sum(columns[0])
    assert sum(columns[1]) == 12, sum(columns[1])
    assert sum(columns[2]) == 9, sum(columns[2])

if __name__ == "__main__":  # pragma: nocover
    from tests import run_tests

    run_tests()
