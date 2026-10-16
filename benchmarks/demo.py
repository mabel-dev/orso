#!/usr/bin/env python3
"""
How fast is orso v2? The same workloads, through the public API only, on orso v1
(0.0.246, built from its git commit) and on this checkout, side by side.

The workloads are the paths orso's consumers actually take: mabel's blob writer, an
opteryx query result being fetched, an Arrow round trip, peeking at a result, schema
work in a planner, record validation, timestamp parsing, and Python rows to Arrow.

    python benchmarks/demo.py              # build v1 once (cached), then run everything
    python benchmarks/demo.py -k arrow     # only workloads whose name contains "arrow"
    python benchmarks/demo.py --scale 0.2  # smaller data, quicker run

v1 is exported from git (commit c09cc35) into benchmarks/.v1/ and its Cython extensions
are built there with this interpreter the first time. Each workload runs in a fresh
process per version, v1 and v2 alternating, and the best time is kept.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
V1_COMMIT = "c09cc35"
V1_DIR = os.path.join(HERE, ".v1")


# ---------------------------------------------------------------------------------
# workloads: each takes a scale, builds its inputs untimed, and returns a callable to
# time plus a check value that must come out the same on both versions
# ---------------------------------------------------------------------------------


def _arrow_table(n):
    import datetime

    import pyarrow

    base = datetime.datetime(2024, 1, 1)
    return pyarrow.table(
        {
            "id": pyarrow.array(range(n), pyarrow.int64()),
            "user": [f"user_{i:07d}" for i in range(n)],
            "score": [i * 0.5 for i in range(n)],
            "active": [i % 3 == 0 for i in range(n)],
            "seen": pyarrow.array([base + datetime.timedelta(seconds=i) for i in range(n)], pyarrow.timestamp("us")),
            "tag": [None if i % 11 == 0 else ("alpha", "beta", "gamma")[i % 3] for i in range(n)],
        }
    )


def _writer_schema():
    from orso.schema import FlatColumn
    from orso.schema import RelationSchema
    from orso.types import OrsoTypes

    return RelationSchema(
        name="events",
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER, nullable=False),
            FlatColumn(name="user", type=OrsoTypes.VARCHAR),
            FlatColumn(name="score", type=OrsoTypes.DOUBLE),
            FlatColumn(name="active", type=OrsoTypes.BOOLEAN),
            FlatColumn(name="seen", type=OrsoTypes.TIMESTAMP),
            FlatColumn(name="tag", type=OrsoTypes.VARCHAR),
        ],
    )


def _records(n):
    import datetime

    base = datetime.datetime(2024, 1, 1)
    return [
        {
            "id": i,
            "user": f"user_{i:07d}",
            "score": i * 0.5,
            "active": i % 3 == 0,
            "seen": base + datetime.timedelta(seconds=i),
            "tag": None if i % 11 == 0 else ("alpha", "beta", "gamma")[i % 3],
        }
        for i in range(n)
    ]


def w_blob_writer(scale):
    """mabel's writer: append typed records, size the blob every 1,000, flush to Arrow."""
    import orso

    schema = _writer_schema()
    records = _records(int(100_000 * scale))

    def run():
        df = orso.DataFrame(rows=[], schema=schema)
        for i, record in enumerate(records, 1):
            df.append(record)
            if i % 1000 == 0:
                df.nbytes()
        return df.arrow().num_rows

    return run


def w_fetch_results(scale):
    """An opteryx query result: Arrow in, every row fetched in batches of 1,000."""
    import orso

    table = _arrow_table(int(500_000 * scale))

    def run():
        df = orso.DataFrame.from_arrow(table)
        total = 0
        while True:
            batch = df.fetchmany(1000)
            if not batch:
                return total
            total += len(batch)

    return run


def w_arrow_round_trip(scale):
    """Arrow -> orso -> Arrow, as a pass-through operator would."""
    import orso

    table = _arrow_table(int(500_000 * scale))
    return lambda: orso.DataFrame.from_arrow(table).arrow().num_rows


def w_peek(scale):
    """Peek at a large result: the first ten rows of 500,000."""
    import orso

    table = _arrow_table(int(500_000 * scale))
    return lambda: len(orso.DataFrame.from_arrow(table).head(10).fetchall())


def w_planner_schemas(scale):
    """Planner-style schema work: build schemas from type strings, look columns up by
    name and alias, and convert schemas to Arrow."""
    from orso.schema import FlatColumn
    from orso.schema import RelationSchema
    from orso.schema import convert_orso_schema_to_arrow_schema

    types = ["INTEGER", "VARCHAR", "DOUBLE", "TIMESTAMP", "DECIMAL(12,2)", "BOOLEAN"]
    n_schemas = max(1, int(200 * scale))

    def run():
        found = 0
        for s in range(n_schemas):
            schema = RelationSchema(
                name=f"rel_{s}",
                columns=[
                    FlatColumn(name=f"col_{c}", type=types[c % len(types)], aliases=[f"r{s}.col_{c}"])
                    for c in range(30)
                ],
            )
            for c in range(500):
                if schema.find_column(f"col_{c % 30}") is not None:
                    found += 1
                if schema.find_column(f"r{s}.col_{c % 30}") is not None:
                    found += 1
            for _ in range(20):
                convert_orso_schema_to_arrow_schema(schema)
        return found

    return run


def w_validate(scale):
    """Validate records against a schema, as mabel's writers do before writing."""
    schema = _writer_schema()
    records = _records(int(200_000 * scale))
    validate = schema.validate
    return lambda: sum(1 for r in records if validate(r))


def w_parse_timestamps(scale):
    """Parse ISO-8601 timestamps the way a planner casts literals."""
    import datetime

    from orso.types import OrsoTypes

    base = datetime.datetime(2024, 1, 1)
    texts = [(base + datetime.timedelta(seconds=i * 37)).isoformat(sep=" ") for i in range(int(200_000 * scale))]
    parse = OrsoTypes.TIMESTAMP.parse
    return lambda: len([parse(t) for t in texts])


def w_python_rows_to_arrow(scale):
    """Rows fetched from a database driver handed to orso and converted to Arrow."""
    import orso

    schema = _writer_schema()
    names = [c.name for c in schema.columns]
    rows = [tuple(r[n] for n in names) for r in _records(int(300_000 * scale))]
    return lambda: orso.DataFrame(rows=rows, schema=schema).arrow().num_rows


WORKLOADS = {
    "blob writer (append + nbytes + arrow)": w_blob_writer,
    "query result: from_arrow + fetch every row": w_fetch_results,
    "arrow round trip": w_arrow_round_trip,
    "peek at a result: from_arrow + head(10)": w_peek,
    "planner schemas: build, find_column, to arrow": w_planner_schemas,
    "validate records": w_validate,
    "parse timestamps": w_parse_timestamps,
    "python rows to arrow": w_python_rows_to_arrow,
}


# ---------------------------------------------------------------------------------
# worker: one version, one workload, in its own process
# ---------------------------------------------------------------------------------


def worker(root, name, scale, repeats):
    sys.path.insert(0, root)
    import gc

    import orso

    run = WORKLOADS[name](scale)
    check = run()  # warm-up, and the value both versions must agree on
    best = float("inf")
    for _ in range(repeats):
        gc.collect()
        start = time.perf_counter()
        run()
        best = min(best, time.perf_counter() - start)
    print(json.dumps({"version": orso.__version__, "orso_file": orso.__file__, "seconds": best, "check": check}))


# ---------------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------------


def ensure_v1():
    marker = os.path.join(V1_DIR, ".built")
    if os.path.exists(marker):
        return V1_DIR
    print(f"building orso v1 ({V1_COMMIT}) into {os.path.relpath(V1_DIR, ROOT)} (first run only) ...", flush=True)
    os.makedirs(V1_DIR, exist_ok=True)
    archive = subprocess.run(["git", "archive", V1_COMMIT], cwd=ROOT, check=True, capture_output=True).stdout
    subprocess.run(["tar", "-x", "-C", V1_DIR], input=archive, check=True)
    subprocess.run(
        [sys.executable, "setup.py", "build_ext", "--inplace"],
        cwd=V1_DIR,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    open(marker, "w").close()
    return V1_DIR


def measure(root, name, scale, repeats):
    out = subprocess.run(
        [sys.executable, __file__, "--worker", root, name, str(scale), str(repeats)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return json.loads(out.strip().splitlines()[-1])


def fmt_seconds(s):
    return f"{s * 1000:,.0f} ms" if s >= 0.001 else f"{s * 1e6:,.0f} µs"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-k", "--filter", help="only workloads whose name contains this text")
    parser.add_argument("--scale", type=float, default=1.0, help="multiply every data size (default 1)")
    parser.add_argument("--repeats", type=int, default=3, help="timed runs per process (default 3)")
    parser.add_argument("--worker", nargs=4, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        root, name, scale, repeats = args.worker
        worker(root, name, float(scale), int(repeats))
        return

    names = [n for n in WORKLOADS if not args.filter or args.filter in n]
    v1_root = ensure_v1()
    v2_root = ROOT

    print(f"\norso v1 0.0.246 vs v2 (this checkout) | Python {sys.version.split()[0]} | scale {args.scale:g}\n")
    header = f"{'workload':<48} {'v1':>10} {'v2':>10} {'speedup':>9}"
    print(header)
    print("-" * len(header))
    ratios = []
    for name in names:
        best = {}
        checks = {}
        for _ in range(2):  # alternate so a slow moment on the machine hits both
            for label, root in (("v1", v1_root), ("v2", v2_root)):
                result = measure(root, name, args.scale, args.repeats)
                best[label] = min(best.get(label, float("inf")), result["seconds"])
                checks[label] = result["check"]
        if checks["v1"] != checks["v2"]:
            print(f"{name:<48} results differ: v1 {checks['v1']!r} v2 {checks['v2']!r}")
            continue
        speedup = best["v1"] / best["v2"]
        ratios.append(speedup)
        print(f"{name:<48} {fmt_seconds(best['v1']):>10} {fmt_seconds(best['v2']):>10} {speedup:>8.1f}x", flush=True)
    if ratios:
        print("-" * len(header))
        print(f"{'geometric mean':<48} {'':>10} {'':>10} {statistics.geometric_mean(ratios):>8.1f}x")


if __name__ == "__main__":
    main()
