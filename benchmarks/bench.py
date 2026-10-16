#!/usr/bin/env python3
# isort: skip_file  (cases imports orso, so ROOT must be on sys.path first)
"""
Orso micro-benchmark harness.

Every case is a self-contained unit of work sized to take tens to hundreds of
milliseconds, so that timer noise is small relative to the measurement. Each case
is set up once, warmed up once, then timed over several rounds with a GC pass
between rounds. Both the minimum (least-disturbed run) and the median (typical
run) are reported; use the minimum when comparing code changes.

Usage:
    python benchmarks/bench.py                      # run everything, print table
    python benchmarks/bench.py -k append            # only cases whose id contains 'append'
    python benchmarks/bench.py -o results.json      # also save results
    python benchmarks/bench.py -c baseline.json     # compare against a saved run
    python benchmarks/bench.py --rounds 9 --scale 2 # more rounds, bigger data

Record a baseline before changing anything, then compare every change against it:

    python benchmarks/bench.py -o benchmarks/baseline_$(python -c 'import orso;print(orso.__version__)').json
    ...make changes, rebuild extensions...
    python benchmarks/bench.py -c benchmarks/baseline_0.0.246.json
"""

import argparse
import datetime
import gc
import json
import os
import platform
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

import cases  # noqa: E402  (registers the benchmark cases)
from cases import CASES  # noqa: E402


def _git(*args):
    try:
        return (
            subprocess.check_output(["git", *args], cwd=ROOT, stderr=subprocess.DEVNULL)
            .decode()
            .strip()
        )
    except Exception:
        return None


def collect_meta(rounds: int, scale: float) -> dict:
    import orso

    return {
        "orso": orso.__version__,
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "commit": _git("rev-parse", "--short", "HEAD"),
        "dirty": bool(_git("status", "--porcelain")),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "rounds": rounds,
        "scale": scale,
    }


def time_case(case, rounds: int, scale: float) -> dict:
    run = case.setup(scale)
    run()  # warm-up: imports, caches, first-call allocations
    samples = []
    for _ in range(rounds):
        gc.collect()
        start = time.perf_counter_ns()
        run()
        samples.append((time.perf_counter_ns() - start) / 1e6)
    return {
        "group": case.group,
        "ops": int(case.ops * scale),
        "min_ms": min(samples),
        "median_ms": statistics.median(samples),
        "rounds_ms": samples,
    }


def fmt_delta(current: float, baseline: float) -> str:
    if not baseline:
        return "     n/a"
    delta = (current - baseline) / baseline * 100.0
    return f"{delta:+7.1f}%"


def print_table(results: dict, baseline: dict | None, threshold: float):
    has_base = baseline is not None
    header = f"{'case':<46} {'ops':>8} {'min ms':>9} {'median ms':>10} {'ns/op':>9}"
    if has_base:
        header += f" {'base min':>9} {'delta':>8}"
    print(header)
    print("-" * len(header))

    faster, slower, ratios = [], [], []
    current_group = None
    for case_id, res in results.items():
        if res["group"] != current_group:
            current_group = res["group"]
            print(f"[{current_group}]")
        ns_per_op = res["min_ms"] * 1e6 / res["ops"] if res["ops"] else 0.0
        line = (
            f"  {case_id:<44} {res['ops']:>8,} {res['min_ms']:>9.2f} "
            f"{res['median_ms']:>10.2f} {ns_per_op:>9,.0f}"
        )
        if has_base:
            base = baseline.get("results", {}).get(case_id)
            if base:
                base_min = base["min_ms"]
                delta = (res["min_ms"] - base_min) / base_min * 100.0
                marker = ""
                if res["group"] == "control":
                    # the machine, not orso: reported below, kept out of the scoring
                    line += f" {base_min:>9.2f} {fmt_delta(res['min_ms'], base_min)}"
                    print(line)
                    continue
                ratios.append(res["min_ms"] / base_min)
                if delta <= -threshold:
                    marker = " faster"
                    faster.append(case_id)
                elif delta >= threshold:
                    marker = " SLOWER"
                    slower.append(case_id)
                line += f" {base_min:>9.2f} {fmt_delta(res['min_ms'], base_min)}{marker}"
            else:
                line += f" {'-':>9} {'new':>8}"
        print(line)

    if has_base and ratios:
        geo = statistics.geometric_mean(ratios)
        print()
        control = results.get("control/pure_python_loop")
        base_control = baseline.get("results", {}).get("control/pure_python_loop")
        if control and base_control:
            drift = (control["min_ms"] - base_control["min_ms"]) / base_control["min_ms"] * 100.0
            if abs(drift) >= threshold:
                print(
                    f"WARNING: the control case moved {drift:+.1f}% with no orso code involved; "
                    f"the machine is {'slower' if drift > 0 else 'faster'} than at baseline time and "
                    f"every delta below carries roughly that bias."
                )
            else:
                print(f"control case drift {drift:+.1f}% (machine comparable to baseline)")
        print(
            f"vs baseline ({baseline['meta'].get('orso')} @ {baseline['meta'].get('commit')}): "
            f"{len(faster)} faster, {len(slower)} slower, {len(ratios) - len(faster) - len(slower)} "
            f"within ±{threshold:g}%  |  geometric mean of time ratios: {geo:.3f} "
            f"({(1 - geo) * 100:+.1f}% faster overall)"
            if geo <= 1
            else f"vs baseline ({baseline['meta'].get('orso')} @ {baseline['meta'].get('commit')}): "
            f"{len(faster)} faster, {len(slower)} slower, {len(ratios) - len(faster) - len(slower)} "
            f"within ±{threshold:g}%  |  geometric mean of time ratios: {geo:.3f} "
            f"({(geo - 1) * 100:+.1f}% slower overall)"
        )
        if slower:
            print("regressions: " + ", ".join(slower))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-k", "--filter", help="only run cases whose id contains this substring")
    parser.add_argument("--rounds", type=int, default=5, help="timed rounds per case (default 5)")
    parser.add_argument(
        "--scale", type=float, default=1.0, help="multiply the data size of every case"
    )
    parser.add_argument("-o", "--output", help="write results to this JSON file")
    parser.add_argument("-c", "--compare", help="compare against this saved JSON file")
    parser.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help="percent change to flag as faster/slower (default 5)",
    )
    parser.add_argument("--list", action="store_true", help="list case ids and exit")
    args = parser.parse_args(argv)

    selected = [c for c in CASES if not args.filter or args.filter in c.id]
    if args.list:
        for c in selected:
            print(c.id)
        return 0
    if not selected:
        print(f"no cases match {args.filter!r}", file=sys.stderr)
        return 2

    baseline = None
    if args.compare:
        with open(args.compare) as f:
            baseline = json.load(f)

    meta = collect_meta(args.rounds, args.scale)
    print(
        f"orso {meta['orso']} @ {meta['commit']}{' (dirty)' if meta['dirty'] else ''} | "
        f"{meta['implementation']} {meta['python']} | {meta['machine']} | "
        f"{len(selected)} cases x {args.rounds} rounds, scale {args.scale:g}"
    )
    if baseline:
        bm = baseline["meta"]
        if bm.get("scale") != args.scale:
            print(
                f"WARNING: baseline scale {bm.get('scale')} differs from {args.scale}; deltas are not comparable"
            )
        if bm.get("python") != meta["python"] or bm.get("machine") != meta["machine"]:
            print(
                f"WARNING: baseline ran on {bm.get('implementation')} {bm.get('python')} / {bm.get('machine')}"
            )
    print()

    results = {}
    interactive = sys.stdout.isatty()
    for case in selected:
        if interactive:
            print(f"  running {case.id} ...".ljust(70), end="\r", flush=True)
        results[case.id] = time_case(case, args.rounds, args.scale)
    if interactive:
        print(" " * 70, end="\r")

    print_table(results, baseline, args.threshold)

    if args.output:
        with open(args.output, "w") as f:
            json.dump({"meta": meta, "results": results}, f, indent=2)
        print(f"\nsaved {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
