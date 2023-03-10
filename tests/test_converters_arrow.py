import os
import sys

import pandas
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso import converters


def test_from_arrow():
    # Create some sample data
    table1 = pyarrow.Table.from_arrays(
        [
            pyarrow.array([1, 2, 3]),
            pyarrow.array(["a", "b", "c"]),
            pyarrow.array([True, False, True]),
        ],
        ["id", "name", "active"],
    )
    table2 = pyarrow.Table.from_arrays(
        [
            pyarrow.array([4, 5, 6]),
            pyarrow.array(["d", "e", "f"]),
            pyarrow.array([False, True, False]),
        ],
        ["id", "name", "active"],
    )
    tables = [table1, table2]

    # Test the function with a limit of 4 rows
    result = converters.from_arrow(tables, 4)

    # Verify that the result has the correct number of rows and columns
    assert len(result) == 4
    assert len(result.column_names) == 3

    # Verify that the result has the correct column names and data types
    assert result.column_names == ("id", "name", "active")


if __name__ == "__main__":  # pragma: no cover
    test_from_arrow()
    print("✅ okay")
