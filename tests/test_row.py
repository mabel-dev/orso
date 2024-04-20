import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pytest
from orso import Row
from orso.exceptions import DataError
from orso.row import extract_columns, MAXIMUM_RECORD_SIZE


def test_row_from_dict():
    row_factory = Row.create_class(
        (
            "col1",
            "col2",
        )
    )
    r = row_factory([1, "a"])
    assert r[0] == 1 and r[1] == "a"


def test_row_as_dict():
    row_factory = Row.create_class(
        (
            "col1",
            "col2",
        )
    )
    r = row_factory([1, "a"])
    assert r.as_dict == {"col1": 1, "col2": "a"}


def test_row_as_map():
    row_factory = Row.create_class(
        (
            "col1",
            "col2",
        )
    )
    r = row_factory([1, "a"])
    assert r.as_map == (("col1", 1), ("col2", "a")), r.as_map


def test_row_from_bytes():
    row_factory = Row.create_class(
        (
            "col1",
            "col2",
        )
    )
    original_row = row_factory([1, "a"])
    serialized_row = original_row.as_bytes
    deserialized_row = Row.from_bytes(serialized_row)
    assert original_row == deserialized_row


def test_row_to_bytes():
    row_factory = Row.create_class(
        (
            "col1",
            "col2",
        )
    )
    r = row_factory([1, "a"])
    byte_result = r.as_bytes
    assert isinstance(byte_result, bytes)


def test_row_to_bytes_size_limit():
    big_row = Row(["a" * MAXIMUM_RECORD_SIZE])
    with pytest.raises(DataError):
        big_row.as_bytes


def test_row_to_json():
    row_factory = Row.create_class(
        (
            "col1",
            "col2",
        )
    )
    r = row_factory([1, "a"])
    json_result = r.as_json
    assert json_result == b'{"col1":1,"col2":"a"}'


def test_item_get_access():
    import opteryx

    rows = opteryx.query("SELECT * FROM $planets")
    items = []
    for row in rows:
        assert isinstance(row, Row)
        items.append(row.get("name"))

    assert items == [
        "Mercury",
        "Venus",
        "Earth",
        "Mars",
        "Jupiter",
        "Saturn",
        "Uranus",
        "Neptune",
        "Pluto",
    ]


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
