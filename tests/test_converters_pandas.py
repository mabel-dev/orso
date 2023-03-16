import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame


def test_to_pandas():
    import pandas

    # fmt:off
    cities = [
        {"name": "Tokyo", "population": 13929286, "country": "Japan", "founded": "1457", "area": 2191, "language": "Japanese"},
        {"name": "London", "population": 8982000, "country": "United Kingdom", "founded": "43 AD", "area": 1572, "language": "English"},
        {"name": "New York City", "population": 8399000, "country": "United States", "founded": "1624", "area": 468.9, "language": "English"},
        {"name": "Mumbai", "population": 18500000, "country": "India", "founded": "7th century BC", "area": 603.4, "language": "Hindi, English"},
        {"name": "Cape Town", "population": 433688, "country": "South Africa", "founded": "1652", "area": 400, "language": "Afrikaans, English"},
        {"name": "Paris", "population": 2148000, "country": "France", "founded": "3rd century BC", "area": 105.4, "language": "French"},
        {"name": "Beijing", "population": 21710000, "country": "China", "founded": "1045", "area": 16410.54, "language": "Mandarin"},
        {"name": "Rio de Janeiro", "population": 6747815, "country": "Brazil", "founded": "1 March 1565", "area": 1264, "language": "Portuguese"}
    ]
    # fmt:on
    odf = DataFrame(cities)
    pdf = odf.pandas()

    assert len(pdf) == 8
    assert "name" in pdf.columns
    assert type(pdf) == pandas.DataFrame


if __name__ == "__main__":  # pragma: no cover
    test_to_pandas()
    print("✅ okay")
