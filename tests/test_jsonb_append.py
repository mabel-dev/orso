import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pyarrow as pa

from orso.schema import RelationSchema, FlatColumn
from orso.types import OrsoTypes
from orso.dataframe import DataFrame


def test_append_dict_to_jsonb_column():
    schema = RelationSchema(
        name="jtest",
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER),
            FlatColumn(name="payload", type=OrsoTypes.JSONB),
        ],
    )

    df = DataFrame(schema=schema)
    df.append({"id": 1, "payload": {"a": 1, "b": [1,2]}})

    # Stored value should be bytes
    row = df._rows[0]
    assert isinstance(row[1], (bytes, bytearray))

    # Arrow conversion should yield a binary column
    table = df.arrow()
    assert table.schema.field("payload").type == pa.binary()


def test_append_bytes_to_jsonb_column():
    schema = RelationSchema(
        name="jtest",
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER),
            FlatColumn(name="payload", type=OrsoTypes.JSONB),
        ],
    )

    df = DataFrame(schema=schema)
    payload = b"{\"x\":1}"
    df.append({"id": 2, "payload": payload})

    row = df._rows[0]
    assert row[1] == payload
