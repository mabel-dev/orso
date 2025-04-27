import datetime
import decimal
import pytest
import sys


sys.path.insert(1, "/".join([str(p) for p in __file__.split("/")[:-2]])) 

print(sys.path)

from orso.types import OrsoTypes
import orso

print(orso.__version__)

CAST_TESTS = [
    ("BOOLEAN", "true", True),
    ("BOOLEAN", "FALSE", False),
    ("BOOLEAN", None, None),
    ("BOOLEAN", "yes", True),
    ("BOOLEAN", "no", False),
    ("BOOLEAN", "on", True),
    ("BOOLEAN", "off", False),
    ("BOOLEAN", "1", True),
    ("BOOLEAN", "0", False),
    ("BOOLEAN", 1, True),
    ("BOOLEAN", 0, False),
    ("BOOLEAN", 1.000, True),
    ("BOOLEAN", 0.000, False),
    ("BOOLEAN", True, True),
    ("BOOLEAN", False, False),
    ("BOOLEAN", "y", True),
    ("BOOLEAN", "n", False),
    ("BOOLEAN", "t", True),
    ("BOOLEAN", "f", False),
    ("BOOLEAN", b"true", True),
    ("BOOLEAN", b"FALSE", False),
    ("BOOLEAN", b"yes", True),
    ("BOOLEAN", b"no", False),
    ("BOOLEAN", b"on", True),
    ("BOOLEAN", b"off", False),
    ("BOOLEAN", b"1", True),
    ("BOOLEAN", b"0", False),
    ("BOOLEAN", 1, True),
    ("BOOLEAN", 0, False),
    ("BOOLEAN", 1.000, True),
    ("BOOLEAN", 0.000, False),
    ("BOOLEAN", True, True),
    ("BOOLEAN", False, False),
    ("BOOLEAN", b"y", True),
    ("BOOLEAN", b"n", False),
    ("BOOLEAN", b"t", True),
    ("BOOLEAN", b"f", False),

    ("BLOB", "hello", b"hello"),
    ("BLOB", b"bytes", b"bytes"),
    ("BLOB", None, None),
    ("BLOB[5]", "hello world", b"hello"),  # Test length limit
    ("BLOB", 123, b"123"),
    ("BLOB", 123.45, b"123.45"),
    ("BLOB", True, b"True"),
    ("BLOB", False, b"False"),
    ("BLOB", "", b""),
    ("BLOB", b"", b""),
    ("BLOB", "特殊字符", b"\xe7\x89\xb9\xe6\xae\x8a\xe5\xad\x97\xe7\xac\xa6"),
    ("BLOB", ["list", "item"], b'["list","item"]'),
    ("BLOB", {"key": "value"}, b'{"key":"value"}'),
   
    ("DATE", "2023-04-18", datetime.date(2023, 4, 18)),
    ("DATE", b"2023-04-18", datetime.date(2023, 4, 18)),
    ("DATE", "2023-04-18 12:12:12", datetime.date(2023, 4, 18)),
    ("DATE", datetime.date(2023, 4, 18), datetime.date(2023, 4, 18)),
    ("DATE", datetime.datetime(2023, 4, 18, 12, 34, 56), datetime.date(2023, 4, 18)),
    ("DATE", 1681776000, datetime.date(2023, 4, 18)),  # Unix timestamp for 2023-04-18
    ("DATE", None, None),
    ("DATE", "2024-02-29", datetime.date(2024, 2, 29)),  # Valid leap year date

    ("TIMESTAMP", "2023-04-18T12:34:56", datetime.datetime(2023, 4, 18, 12, 34, 56)),
    ("TIMESTAMP", b"2023-04-18T12:34:56", datetime.datetime(2023, 4, 18, 12, 34, 56)),
    ("TIMESTAMP", None, None),
    ("TIMESTAMP", "2023-04-18", datetime.datetime(2023, 4, 18, 0, 0, 0)),
    ("TIMESTAMP", 1681776600, datetime.datetime(2023, 4, 18, 0, 10, 0)),

    ("DECIMAL", "123.45", decimal.Decimal("123.45")),
    ("DECIMAL", 123, decimal.Decimal(123)),
    ("DECIMAL(5,3)", "123.45", decimal.Decimal("123.45")),
    ("DECIMAL(4,4)", "123.45", decimal.Decimal("123.4")),
    ("DECIMAL(5,3)", b"123.45", decimal.Decimal("123.45")),

    ("DOUBLE", "123.45", 123.45),
    ("DOUBLE", 123, 123.0),
    ("DOUBLE", "-123.45", -123.45),
    ("DOUBLE", b"123.45", 123.45),
    ("DOUBLE", "1.23e5", 123000.0),
    ("DOUBLE", "1.23E-5", 0.0000123),
    ("DOUBLE", float('inf'), float('inf')),
    ("DOUBLE", "-inf", float('-inf')),
    ("DOUBLE", None, None),
    ("DOUBLE", True, 1.0),
    ("DOUBLE", False, 0.0),
    ("DOUBLE", " 123.45 ", 123.45),  # Test trimming
    ("DOUBLE", "000123.45000", 123.45),  # Test trimming

    ("INTEGER", "42", 42),
    ("INTEGER", 42.9, 42),
    ("INTEGER", "-42", -42),
    ("INTEGER", b"42", 42),
    ("INTEGER", True, 1),
    ("INTEGER", False, 0),
    ("INTEGER", None, None),
    ("INTEGER", "0042", 42),  # Test leading zeros
    ("INTEGER", " 42 ", 42),  # Test trimming
    ("INTEGER", 9223372036854775807, 9223372036854775807),  # Max int64
    ("INTEGER", -9223372036854775808, -9223372036854775808),  # Min int64

    ("VARCHAR", 123, "123"),
    ("VARCHAR", "hello", "hello"),
    ("VARCHAR", None, None),
    ("VARCHAR", 123.45, "123.45"),
    ("VARCHAR", True, "True"),
    ("VARCHAR", False, "False"),
    ("VARCHAR", b"binary string", "binary string"),
    ("VARCHAR", ["list", "items"], '["list","items"]'),
    ("VARCHAR", {"key": "value"}, '{"key":"value"}'),
    ("VARCHAR", datetime.date(2023, 4, 18), "2023-04-18"),
    ("VARCHAR", datetime.datetime(2023, 4, 18, 12, 34, 56), "2023-04-18 12:34:56"),
    ("VARCHAR", "", ""),
    ("VARCHAR[5]", "hello world", "hello"),  # Test length limit
    ("VARCHAR[10]", "hello", "hello"),  # Test length under limit
    ("VARCHAR", " padded ", " padded "),  # Test that spaces are preserved

    ("NULL", "anything", None),
    ("NULL", None, None),

    ("ARRAY<INTEGER>", [1, 2, 3], [1, 2, 3]),
    ("ARRAY<INTEGER>", "[1, 2, 3]", [1, 2, 3]),
    ("ARRAY<INTEGER>", b"[1, 2, 3]", [1, 2, 3]),
    ("ARRAY<INTEGER>", "[1, null, 3]", [1, None, 3]),
    ("ARRAY<INTEGER>", None, None),
    ("ARRAY<INTEGER>", [], []),

    ("ARRAY<DOUBLE>", [1.1, 2.2, 3.3], [1.1, 2.2, 3.3]),
    ("ARRAY<DOUBLE>", "[1.1, 2.2, 3.3]", [1.1, 2.2, 3.3]),

    ("ARRAY<BOOLEAN>", [True, False, True], [True, False, True]),
    ("ARRAY<BOOLEAN>", '["true", "false", "yes"]', [True, False, True]),

    ("ARRAY<VARCHAR>", ["a", "b", "c"], ["a", "b", "c"]),
    ("ARRAY<VARCHAR>", [1, 2, 3], ["1", "2", "3"]),

    ("ARRAY<BLOB>", '["hello", "world"]', [b"hello", b"world"]),
    ("ARRAY<BLOB>", b'["hello", "world"]', [b"hello", b"world"]),

    ("ARRAY<DATE>", ["2023-04-18", "2024-02-29"], [datetime.date(2023, 4, 18), datetime.date(2024, 2, 29)]),

    ("ARRAY<TIMESTAMP>", ["2023-04-18T12:34:56", "2023-04-19T00:00:00"], [datetime.datetime(2023, 4, 18, 12, 34, 56), datetime.datetime(2023, 4, 19, 0, 0, 0)]),
]

@pytest.mark.parametrize("type_name,input_value,expected", CAST_TESTS)
def test_orso_type_parsers(type_name, input_value, expected):
    _type, _length, _precision, _scale, _element_type = OrsoTypes.from_name(type_name)
    value = _type.parse(input_value, length=_length, precision=_precision, scale=_scale, element_type=_element_type)
    assert value == expected, f"{type_name} parsing of {input_value} returned {value}, expected {expected}"



BOOLEAN_STRINGS = ("TRUE", "ON", "YES", "1", "1.0", b"TRUE", b"ON", b"YES", b"1", b"1.0")
bp = lambda x: str(x).upper() in BOOLEAN_STRINGS

print(bp("FALSE"))


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(CAST_TESTS)} DATE TESTS")
    for type_name, input_value, expected in CAST_TESTS:
        print(f"{type_name} {input_value} => {expected}" )
        test_orso_type_parsers(type_name, input_value, expected)
    print("okay")


