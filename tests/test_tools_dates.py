import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import datetime

try:  # optional in v2: their cases run when installed
    import numpy
except ImportError:  # pragma: no cover
    numpy = None
try:
    import pandas
except ImportError:  # pragma: no cover
    pandas = None

from orso.tools import parse_iso

# fmt:off
# v2 (DESIGN.md §8 fix 11 and phase 4): an offset is applied, so the result is naive UTC,
# and fractional seconds are kept to the microsecond. v1 ignored both.
DATE_TESTS = [
        ("2021001011", datetime.datetime(2034, 1, 16, 5, 10, 11)),
        ("2021-02-21", datetime.datetime(2021,2,21)),
        ("2021-02-21T", None),
        ("2021-01-11 12:00", datetime.datetime(2021,1,11,12,0)),
        ("2021-01-11 12:00+0100", datetime.datetime(2021,1,11,11,0)),  # v1 ignored the offset: 12:00
        ("2021-01-11 12:00Z", datetime.datetime(2021,1,11,12,0)),
        ("2021-01-11T12:00", datetime.datetime(2021,1,11,12,0)),
        ("2021-01-11T12:00Z", datetime.datetime(2021,1,11,12,0)),
        ("2020-10-01 18:05:20", datetime.datetime(2020,10,1,18,5,20)),
        ("2020-10-01T18:05:20", datetime.datetime(2020,10,1,18,5,20)),
        ("2020-10-01T18:05:20+0100", datetime.datetime(2020,10,1,17,5,20)),  # v1: 18:05:20
        ("1999-12-31 23:59:59.9", datetime.datetime(1999,12,31,23,59,59,900000)),  # v1 dropped fractions
        ("1999-12-31 23:59:59.9999", datetime.datetime(1999,12,31,23,59,59,999900)),
        ("1999-12-31T23:59:59.9999", datetime.datetime(1999,12,31,23,59,59,999900)),
        ("1999-12-31T23:59:59.9999Z", datetime.datetime(1999,12,31,23,59,59,999900)),
        ("1999-12-31T23:59:59.999999", datetime.datetime(1999,12,31,23,59,59,999999)),
        ("1999-12-31T23:59:59.999999+0800", datetime.datetime(1999,12,31,15,59,59,999999)),  # v1: 23:59:59
        ("1999-12-31T23:59:59.99999999", datetime.datetime(1999,12,31,23,59,59,999999)),  # past us: truncated
        # still accepted through the v1 fallback, which ignores what follows the seconds
        ("2024-01-01 00:00:00 UTC", datetime.datetime(2024, 1, 1)),
        ("2021-1-5", datetime.datetime(2021, 1, 5)),  # v1: None (it needed two-digit fields)
        ("2023-02-29", None),
        ("2021-01-11T12:00-0530", datetime.datetime(2021, 1, 11, 17, 30)),  # v1: 12:00
        (datetime.datetime(2024, 1, 1, 1, 2, 3, 456), datetime.datetime(2024, 1, 1, 1, 2, 3, 456)),  # v1 dropped the 456
        # offsets that cannot be applied are refused, never dropped (v1: all 12:00 or 12:00:00)
        ("2021-02-21T12:00:00+01", None),
        ("2021-02-21T12:00:00-05", None),
        ("2021-02-21 12:00+01", None),
        ("2021-02-21T12:00:00+01:00:00", None),
        ("2021-02-21T12:00:00 +01:00", None),
        ("2021-02-21T12:00:00+1:00", None),
    ]
# fmt:on
if numpy is not None:
    DATE_TESTS += [
        (numpy.datetime64("2021-01-11T12:00"), datetime.datetime(2021, 1, 11, 12, 0)),
        (numpy.datetime64("2021-02-21T00:00"), datetime.datetime(2021, 2, 21)),
    ]
if pandas is not None:
    DATE_TESTS += [
        (pandas.Timestamp("2021-03-11T12:00"), datetime.datetime(2021, 3, 11, 12, 0, 0)),
        (pandas.Timestamp("2021-04-21T00:00"), datetime.datetime(2021, 4, 21)),
    ]


@pytest.mark.parametrize("string, expect", DATE_TESTS)
def test_date_parser(string, expect):
    assert parse_iso(string) == expect, f"in:{string}  res:{parse_iso(string)} exp:{expect}"


# The rule is draken's (and so opteryx-core's CAST): orso must give the same instant, or
# refuse, wherever draken's parser has an answer, and must never accept an offset it
# refuses.
DRAKEN_AGREEMENT = [
    "2021-02-21T12:00:00",
    "2021-02-21T12:00:00Z",
    "2021-02-21T12:00:00+01:00",
    "2021-02-21T12:00:00+0100",
    "2021-02-21T12:00:00-05:30",
    "2021-02-21T12:00:00+01",
    "2021-02-21T12:00:00.123456+01:00",
    "2021-02-21 23:30-02:00",
    "2021-02-21T00:30:00+14:00",
]


@pytest.mark.parametrize("text", DRAKEN_AGREEMENT)
def test_matches_drakens_parser(text):
    from orso._native import parse_timestamp_us

    us = parse_timestamp_us(text)
    expected = (
        None if us is None else datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=us)
    )
    assert parse_iso(text) == expected


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(DATE_TESTS)} DATE TESTS")
    for date_string, date_date in DATE_TESTS:
        print(date_string)
        test_date_parser(date_string, date_date)
    print("okay")
