import os
import sys


sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso import DataFrame
from tests.cities import values, schema


def test_group_by_sum_population():
    df = DataFrame(values)
    result = df.group_by("language").sum("population")

    expected_result = {
        "Japanese": 13929286,
        "English": 37606863,
        "Hindi, English": 18500000,
        "Afrikaans, English": 433688,
        "French": 2148000,
    }

    for row in result:
        row = row.as_dict
        if row["language"] in expected_result:
            assert row["SUM(population)"] == expected_result[row["language"]], row


def test_group_by_min_population():
    df = DataFrame(values)
    result = df.group_by("country").min("population")

    assert len(result) == 20

    for row in result:
        row_dict = row.as_dict
        if row_dict["country"] == "United States":
            assert row_dict["MIN(population)"] == 8399000


def test_group_by_max_population():
    df = DataFrame(values)
    result = df.group_by("country").max("population")

    assert len(result) == 20

    for row in result:
        row_dict = row.as_dict
        if row_dict["country"] == "United Kingdom":
            assert row_dict["MAX(population)"] == 8982000


def test_group_by_avg_population():
    df = DataFrame(values)
    result = df.group_by("language").avg("population")

    assert len(result) == 17

    for row in result:
        row_dict = row.as_dict
        if row_dict["language"] == "English":
            assert (
                row_dict["AVG(population)"] == 9401715.75
            )  # (8399000 + 5312163 + 14913700, 8982000) / 4


def test_group_by_count_population():
    df = DataFrame(values)
    result = df.group_by("language").count()

    assert len(result) == 17

    for row in result:
        row_dict = row.as_dict
        if row_dict["language"] == "English":
            assert row_dict["COUNT(*)"] == 4


def test_group_by_groups_language():
    df = DataFrame(values)
    result = df.group_by("language").groups()

    assert len(result) == 17


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    test_group_by_count_population()
    run_tests()
