import pandas as pd
import pytest
from orso.row import Row
from orso.dataframe import Dataframe

# test data
df_data = {
    'A': [1, 2, 3, 4],
    'B': ['foo', 'bar', 'foo', 'foo'],
    'C': pd.date_range('2022-01-01', periods=4, freq='D')
}

def test_dataframe_from_arrow():
    # create an arrow table from a pandas DataFrame
    pdf = pd.DataFrame(df_data)
    table = pa.Table.from_pandas(pdf)

    # create a Dataframe from the arrow table
    df = Dataframe.from_arrow(table)

    # check that the dataframe has the expected schema and rows
    assert df._schema == {'A': {'type': 'int64'}, 'B': {'type': 'object'}, 'C': {'type': 'datetime64[ns]', 'nullable': True}}
    assert list(df._rows) == [
        Row(1, 'foo', pd.Timestamp('2022-01-01 00:00:00')),
        Row(2, 'bar', pd.Timestamp('2022-01-02 00:00:00')),
        Row(3, 'foo', pd.Timestamp('2022-01-03 00:00:00')),
        Row(4, 'foo', pd.Timestamp('2022-01-04 00:00:00'))
    ]