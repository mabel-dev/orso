import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame


def test_fetchall():
    dataframe = DataFrame(
        rows=[(1, "John"), (2, "Jane"), (3, "Bob")],
        schema=["id", "name"],
    )
    result = dataframe.fetchall()
    expected = [(1, "John"), (2, "Jane"), (3, "Bob")]
    assert result == expected
    assert dataframe.fetchall() == [], dataframe.fetchall()


def test_fetchone():
    dataframe = DataFrame(
        rows=[(1, "John"), (2, "Jane"), (3, "Bob")],
        schema=["id", "name"],
    )
    result1 = dataframe.fetchone()
    expected1 = (1, "John")
    assert result1 == expected1

    result2 = dataframe.fetchone()
    expected2 = (2, "Jane")
    assert result2 == expected2

    result3 = dataframe.fetchone()
    expected3 = (3, "Bob")
    assert result3 == expected3

    result4 = dataframe.fetchone()
    expected4 = None
    assert result4 == expected4


def test_fetchmany():
    dataframe = DataFrame(
        rows=[(1, "John"), (2, "Jane"), (3, "Bob")],
        schema=["id", "name"],
    )
    result1 = dataframe.fetchmany(2)
    expected1 = [(1, "John"), (2, "Jane")]
    assert result1 == expected1, result1

    result2 = dataframe.fetchmany(2)
    expected2 = [(3, "Bob")]
    assert result2 == expected2, result2

    result3 = dataframe.fetchmany(2)
    expected3 = []
    assert result3 == expected3, result3


def test_fetch_methods():
    dataframe = DataFrame(
        rows=[(1, "John"), (2, "Jane"), (3, "Bob")],
        schema=["id", "name"],
    )

    # Test fetchone
    result1 = dataframe.fetchone()
    expected1 = (1, "John")
    assert result1 == expected1

    # Test fetchmany
    result2 = dataframe.fetchmany(2)
    expected2 = [(2, "Jane"), (3, "Bob")]
    assert result2 == expected2

    # Test fetchall
    result3 = dataframe.fetchall()
    expected3 = []
    assert result3 == expected3


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
