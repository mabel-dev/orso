import pandas
import pyarrow

from orso.dataframe import Dataframe
from orso.row import Row

# test data
df_data = {
    "A": [1, 2, 3, 4],
    "B": ["foo", "bar", "foo", "foo"],
    "C": pandas.date_range("2022-01-01", periods=4, freq="D"),
}


def test_dataframe_from_arrow():
    # create an arrow table from a pandas DataFrame
    pdf = pandas.DataFrame(df_data)
    table = pyarrow.Table.from_pandas(pdf)

    # create a Dataframe from the arrow table
    df = Dataframe.from_arrow(table)

    # check that the dataframe has the expected schema and rows
    assert df._schema == {
        "A": {"type": "int64"},
        "B": {"type": "object"},
        "C": {"type": "datetime64[ns]", "nullable": True},
    }
    assert list(df._rows) == [
        Row(1, "foo", pandas.Timestamp("2022-01-01 00:00:00")),
        Row(2, "bar", pandas.Timestamp("2022-01-02 00:00:00")),
        Row(3, "foo", pandas.Timestamp("2022-01-03 00:00:00")),
        Row(4, "foo", pandas.Timestamp("2022-01-04 00:00:00")),
    ]
