import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.types import OrsoTypes


def test_types_is_numeric():
    # is numeric
    assert not OrsoTypes.ARRAY.is_numeric()
    assert not OrsoTypes.BLOB.is_numeric()
    assert OrsoTypes.BOOLEAN.is_numeric()
    assert not OrsoTypes.DATE.is_numeric()
    assert OrsoTypes.DECIMAL.is_numeric()
    assert OrsoTypes.DOUBLE.is_numeric()
    assert OrsoTypes.INTEGER.is_numeric()
    assert not OrsoTypes.INTERVAL.is_numeric()
    assert not OrsoTypes.STRUCT.is_numeric()
    assert not OrsoTypes.TIME.is_numeric()
    assert not OrsoTypes.TIMESTAMP.is_numeric()
    assert not OrsoTypes.VARCHAR.is_numeric()


def test_types_is_temporal():
    # is temporal
    assert not OrsoTypes.ARRAY.is_temporal()
    assert not OrsoTypes.BLOB.is_temporal()
    assert not OrsoTypes.BOOLEAN.is_temporal()
    assert OrsoTypes.DATE.is_temporal()
    assert not OrsoTypes.DECIMAL.is_temporal()
    assert not OrsoTypes.DOUBLE.is_temporal()
    assert not OrsoTypes.INTEGER.is_temporal()
    assert not OrsoTypes.INTERVAL.is_temporal()
    assert not OrsoTypes.STRUCT.is_temporal()
    assert OrsoTypes.TIME.is_temporal()
    assert OrsoTypes.TIMESTAMP.is_temporal()
    assert not OrsoTypes.VARCHAR.is_temporal()


def test_types_is_large_object():
    # is temporal
    assert not OrsoTypes.ARRAY.is_large_object()
    assert OrsoTypes.BLOB.is_large_object()
    assert not OrsoTypes.BOOLEAN.is_large_object()
    assert not OrsoTypes.DATE.is_large_object()
    assert not OrsoTypes.DECIMAL.is_large_object()
    assert not OrsoTypes.DOUBLE.is_large_object()
    assert not OrsoTypes.INTEGER.is_large_object()
    assert not OrsoTypes.INTERVAL.is_large_object()
    assert not OrsoTypes.STRUCT.is_large_object()
    assert not OrsoTypes.TIME.is_large_object()
    assert not OrsoTypes.TIMESTAMP.is_large_object()
    assert OrsoTypes.VARCHAR.is_large_object()

def test_types_python_type():
    # don't need to test them all to provide the code
    assert OrsoTypes.ARRAY.python_type == list
    assert OrsoTypes.BLOB.python_type == bytes


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
