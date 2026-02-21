import os
import sys
import time

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes


def create_schema(width: int) -> RelationSchema:
    columns = [
        FlatColumn(name="A", type=OrsoTypes.INTEGER, nullable=False),
        FlatColumn(name="B", type=OrsoTypes.VARCHAR),
        FlatColumn(name="C", type=OrsoTypes.DOUBLE, nullable=False),
    ]
    columns.extend(
        FlatColumn(name=f"I{i}", type=OrsoTypes.INTEGER, nullable=False)
        for i in range(max(width - 3, 0))
    )
    return RelationSchema(name="dataset", columns=columns)


def build_row(i: int, width: int) -> dict:
    row = {"A": i + 1, "B": chr(97 + i % 26), "C": (i + 1) * 1.1}
    for c in range(max(width - 3, 0)):
        row[f"I{c}"] = i + c
    return row


def legacy_append(df: DataFrame, entry: dict) -> None:
    """
    Emulates the append path from older versions where _nbytes was updated
    row-by-row via serialization.
    """
    if isinstance(df._schema, RelationSchema):
        df._schema.validate(entry)
    new_row = df._row_factory(entry)
    df._rows.append(new_row)
    df._nbytes += new_row.nbytes()
    df._cursor = None


def run_builder_probe(rows: int, width: int, *, legacy_mode: bool, to_arrow: bool) -> dict:
    schema = create_schema(width)
    df = DataFrame(rows=[], schema=schema)
    df._nbytes = 0

    append_start = time.perf_counter()
    for i in range(rows):
        row = build_row(i, width)
        if legacy_mode:
            legacy_append(df, row)
        else:
            df.append(row)
    append_seconds = time.perf_counter() - append_start

    arrow_seconds = 0.0
    if to_arrow:
        arrow_start = time.perf_counter()
        table = df.arrow()
        arrow_seconds = time.perf_counter() - arrow_start
        assert table.num_rows == rows

    return {
        "rows": rows,
        "columns": width,
        "legacy_mode": legacy_mode,
        "append_seconds": append_seconds,
        "append_rows_per_sec": rows / append_seconds,
        "arrow_seconds": arrow_seconds,
        "total_seconds": append_seconds + arrow_seconds,
    }


@pytest.mark.skipif(
    os.environ.get("ORSO_RUN_PERF") != "1",
    reason="performance probe is opt-in; set ORSO_RUN_PERF=1",
)
def test_dataframe_builder_probe():
    rows = int(os.environ.get("ORSO_PERF_ROWS", "200000"))
    narrow_cols = int(os.environ.get("ORSO_PERF_NARROW_COLS", "3"))
    wide_cols = int(os.environ.get("ORSO_PERF_WIDE_COLS", "120"))
    include_arrow = os.environ.get("ORSO_PERF_ARROW", "1") == "1"

    if include_arrow:
        # Warm converter imports before timing.
        DataFrame(rows=[(1,)], schema=["warm"]).arrow()

    probes = []
    for cols in (narrow_cols, wide_cols):
        probes.append(run_builder_probe(rows, cols, legacy_mode=False, to_arrow=include_arrow))
        probes.append(run_builder_probe(rows, cols, legacy_mode=True, to_arrow=include_arrow))

    for probe in probes:
        mode = "legacy" if probe["legacy_mode"] else "current"
        print(
            f"{mode:7s} cols={probe['columns']:3d} rows={probe['rows']:,} "
            f"append={probe['append_seconds']:.3f}s ({probe['append_rows_per_sec']:.0f} rows/s) "
            f"arrow={probe['arrow_seconds']:.3f}s total={probe['total_seconds']:.3f}s"
        )


if __name__ == "__main__":  # pragma: no cover
    os.environ.setdefault("ORSO_RUN_PERF", "1")
    test_dataframe_builder_probe()
