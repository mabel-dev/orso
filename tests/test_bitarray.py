import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pytest

from orso.bitarray import BitArray


def test_init_size():
    with pytest.raises(AssertionError, match="bitarray size must be a positive integer"):
        BitArray(0)


def test_get_index_error():
    b = BitArray(8)
    with pytest.raises(IndexError, match="Index out of range"):
        print(b.get(8))
    with pytest.raises(IndexError, match="Index out of range"):
        print(b.get(-1))


def test_set_index_error():
    b = BitArray(8)
    with pytest.raises(IndexError, match="Index out of range"):
        b.set(8, 1)
    with pytest.raises(IndexError, match="Index out of range"):
        b.set(-1, 1)


def test_get_set():
    b = BitArray(8)
    b.set(2, 1)
    assert b.get(2) is True
    b.set(2, 0)
    assert b.get(2) is False


def test_bits_representation():
    b = BitArray(8)
    b.set(2, 1)
    b.set(4, 1)
    assert b.array == bytearray([20]), b.array


def test_load_and_unload():
    b = BitArray(12)
    b.set(1, 1)
    b.set(5, 1)
    b.set(11, 1)

    d = BitArray.from_array(b.array, 12)

    assert b.array.hex() == d.array.hex()

    assert d.get(0) == 0
    assert d.get(1) == 1
    assert d.get(2) == 0
    assert d.get(3) == 0
    assert d.get(4) == 0
    assert d.get(5) == 1
    assert d.get(6) == 0
    assert d.get(7) == 0
    assert d.get(8) == 0
    assert d.get(9) == 0
    assert d.get(10) == 0
    assert d.get(11) == 1


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
