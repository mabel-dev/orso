import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame
from tests import cities


def test_to_polars():
    import polars

    odf = DataFrame(cities.values)
    pdf = odf.polars()

    assert len(pdf) == 20
    assert "name" in pdf.columns
    assert type(pdf) == polars.DataFrame

    pdf = odf.polars(size=4)
    assert len(pdf) == 4
    assert "name" in pdf.columns
    assert type(pdf) == polars.DataFrame


if __name__ == "__main__":  # pragma: no cover
    test_to_polars()
    print("✅ okay")
