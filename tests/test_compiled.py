import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.compute.compiled import collect_cython
import numpy

def test_collector():

    columns = collect_cython([(1, 2), (2, 1), (7, 8)], numpy.array([1, 0], dtype=numpy.int32))
    assert len(columns) == 2
    assert len(columns[0]) == 3
    assert sum(columns[0]) == 11
    assert sum(columns[1]) == 10

if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests
    test_collector()
    run_tests()
