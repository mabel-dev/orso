import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame
from tests import cities
import pytest


def test_to_pandas():
    # optional integration dependency - may not have wheels on the newest
    # interpreters, so skip rather than fail the suite
    pandas = pytest.importorskip("pandas")

    odf = DataFrame(cities.values)
    pdf = odf.pandas()

    assert len(pdf) == 20
    assert "name" in pdf.columns
    assert type(pdf) == pandas.DataFrame

    pdf = odf.pandas(size=4)
    assert len(pdf) == 4
    assert "name" in pdf.columns
    assert type(pdf) == pandas.DataFrame


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
