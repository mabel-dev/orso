import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso import cityhash


def test_cityhash_32():
    # Test data and expected hash values
    test_data = [
        (b"", 3696677242),
        (b"Hello, world!", 1877063093),
        (b"Lorem ipsum dolor sit amet, consectetur adipiscing elit.", 3701358904),
    ]

    for data, expected_hash in test_data:
        assert cityhash.CityHash32(data) == expected_hash, f"{data} - {cityhash.CityHash32(data)}"


def test_cityhash_64():
    # Test data and expected hash values
    test_data = [
        (b"", 11160318154034397263),
        (b"Hello, world!", 3493709964939663943),
        (b"Lorem ipsum dolor sit amet, consectetur adipiscing elit.", 11377970064775502085),
    ]

    for data, expected_hash in test_data:
        assert cityhash.CityHash64(data) == expected_hash


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
