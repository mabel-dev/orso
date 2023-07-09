import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.types import OrsoTypes


def test_types_is_numeric():
    # is numeric
    assert not OrsoTypes.ARRAY.is_numeric()
    assert not OrsoTypes.BLOB.is_numeric()
    assert not OrsoTypes.BOOLEAN.is_numeric()
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


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
