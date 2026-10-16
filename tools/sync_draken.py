#!/usr/bin/env python3
"""
Vendor the draken headers orso's native layer is built on.

orso adopts draken's memory model (DESIGN.md §2) but must not import draken at runtime:
mabel installs orso on its own, and the rugo wheel that bundles draken is far too big to
pull in for a handful of headers. So the headers are copied into `native/draken/` and
`native/third_party/`, and the commit they came from is recorded in `native/draken/SOURCE`
together with a sha256 per file, which is how `--check` tells whether the copy has
drifted from the source tree or been edited locally.

The copies are never edited by hand. If draken needs a change, change it in opteryx-core
and re-run this script.

Usage:
    python tools/sync_draken.py                       # copy from ../opteryx-core
    python tools/sync_draken.py --source /path/to/opteryx-core
    python tools/sync_draken.py --check               # exit 1 if the copy is stale or edited
"""

import argparse
import datetime
import hashlib
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DEST = os.path.join(ROOT, "native")
SOURCE_RECORD = os.path.join(DEST, "draken", "SOURCE")

# (path in opteryx-core, path under orso/native). Kept as a flat, explicit list so a
# review of this file is a review of exactly what orso depends on.
MANIFEST = [
    # the memory model
    ("draken/core/alloc.h", "draken/core/alloc.h"),
    ("draken/core/mem_account.h", "draken/core/mem_account.h"),
    ("draken/core/mem_account.cpp", "draken/core/mem_account.cpp"),
    ("draken/core/append_buffer.h", "draken/core/append_buffer.h"),
    # slot formats and the frozen type vocabulary
    ("draken/core/string_slot.h", "draken/core/string_slot.h"),
    ("draken/core/interval_slot.h", "draken/core/interval_slot.h"),
    ("draken/core/buffers.h", "draken/core/buffers.h"),
    ("draken/logical_type.h", "draken/logical_type.h"),
    # the one parser / renderer for each text form
    ("draken/core/iso_datetime.h", "draken/core/iso_datetime.h"),
    ("draken/core/decimal_text.h", "draken/core/decimal_text.h"),
    ("draken/core/ipv4.h", "draken/core/ipv4.h"),
    ("draken/core/uuid_text.h", "draken/core/uuid_text.h"),
    ("draken/interop/value_format.hpp", "draken/interop/value_format.hpp"),
    # interchange
    ("draken/interop/arrow_c_data_interface.h", "draken/interop/arrow_c_data_interface.h"),
    ("draken/vectors/_vector_bridge.h", "draken/vectors/_vector_bridge.h"),
    ("draken/ops/vec_result.h", "draken/ops/vec_result.h"),
    # third party the headers above include
    ("third_party/cyan4973/xxhash.h", "third_party/cyan4973/xxhash.h"),
    ("third_party/cyan4973/LICENSE.txt", "third_party/cyan4973/LICENSE.txt"),
    ("third_party/ulfjack/LICENSE-APACHE.txt", "third_party/ulfjack/LICENSE-APACHE.txt"),
    ("third_party/ulfjack/LICENSE-BOOST.txt", "third_party/ulfjack/LICENSE-BOOST.txt"),
    ("third_party/ulfjack/ryu/ryu.h", "third_party/ulfjack/ryu/ryu.h"),
    ("third_party/ulfjack/ryu/common.h", "third_party/ulfjack/ryu/common.h"),
    ("third_party/ulfjack/ryu/digit_table.h", "third_party/ulfjack/ryu/digit_table.h"),
    ("third_party/ulfjack/ryu/d2s.c", "third_party/ulfjack/ryu/d2s.c"),
    ("third_party/ulfjack/ryu/d2s_full_table.h", "third_party/ulfjack/ryu/d2s_full_table.h"),
    ("third_party/ulfjack/ryu/d2s_small_table.h", "third_party/ulfjack/ryu/d2s_small_table.h"),
    ("third_party/ulfjack/ryu/d2s_intrinsics.h", "third_party/ulfjack/ryu/d2s_intrinsics.h"),
    ("third_party/ulfjack/ryu/d2fixed.c", "third_party/ulfjack/ryu/d2fixed.c"),
    (
        "third_party/ulfjack/ryu/d2fixed_full_table.h",
        "third_party/ulfjack/ryu/d2fixed_full_table.h",
    ),
    ("third_party/ulfjack/ryu/f2s.c", "third_party/ulfjack/ryu/f2s.c"),
    ("third_party/ulfjack/ryu/f2s_full_table.h", "third_party/ulfjack/ryu/f2s_full_table.h"),
    ("third_party/ulfjack/ryu/f2s_intrinsics.h", "third_party/ulfjack/ryu/f2s_intrinsics.h"),
]


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()


def git(source: str, *args: str) -> str:
    return (
        subprocess.check_output(["git", *args], cwd=source, stderr=subprocess.DEVNULL)
        .decode()
        .strip()
    )


def read_record() -> dict:
    """Parse SOURCE into {"meta": {...}, "files": {dest: sha}}."""
    meta, files = {}, {}
    if not os.path.exists(SOURCE_RECORD):
        return {"meta": meta, "files": files}
    with open(SOURCE_RECORD) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            if ": " in line and not line.startswith(" "):
                key, _, value = line.partition(": ")
                if len(key) < 40:
                    meta[key] = value
                    continue
            digest, _, path = line.partition("  ")
            if len(digest) == 64:
                files[path] = digest
    return {"meta": meta, "files": files}


def sync(source: str) -> int:
    source = os.path.abspath(source)
    missing = [src for src, _ in MANIFEST if not os.path.exists(os.path.join(source, src))]
    if missing:
        print(f"missing in {source}:\n  " + "\n  ".join(missing), file=sys.stderr)
        return 1

    commit = git(source, "rev-parse", "HEAD")
    commit_date = git(source, "log", "-1", "--format=%ci")
    dirty = [
        p
        for p in git(source, "status", "--porcelain").splitlines()
        if p[3:].startswith(("draken/", "third_party/"))
    ]
    if dirty:
        print(
            "WARNING: source tree has uncommitted changes under draken/ or third_party/:",
            file=sys.stderr,
        )
        for line in dirty[:10]:
            print("  " + line, file=sys.stderr)

    lines = [
        "# Vendored draken headers. DO NOT EDIT: regenerate with `make sync-draken`.",
        "# `python tools/sync_draken.py --check` verifies this record against the files.",
        f"source: {source}",
        f"commit: {commit}",
        f"commit-date: {commit_date}",
        f"synced: {datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')}",
        f"dirty-source: {'yes' if dirty else 'no'}",
        "",
    ]
    for src, dst in MANIFEST:
        src_path = os.path.join(source, src)
        dst_path = os.path.join(DEST, dst)
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        shutil.copyfile(src_path, dst_path)
        lines.append(f"{sha256(dst_path)}  {dst}")
    os.makedirs(os.path.dirname(SOURCE_RECORD), exist_ok=True)
    with open(SOURCE_RECORD, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"synced {len(MANIFEST)} files from opteryx-core @ {commit[:8]} into {DEST}")
    return 0


def check() -> int:
    record = read_record()
    if not record["files"]:
        print(f"no record at {SOURCE_RECORD}; run tools/sync_draken.py first", file=sys.stderr)
        return 1
    problems = []
    for _, dst in MANIFEST:
        path = os.path.join(DEST, dst)
        if not os.path.exists(path):
            problems.append(f"missing: {dst}")
        elif dst not in record["files"]:
            problems.append(f"not in SOURCE: {dst}")
        elif sha256(path) != record["files"][dst]:
            problems.append(f"edited locally: {dst}")
    for dst in record["files"]:
        if dst not in {d for _, d in MANIFEST}:
            problems.append(f"in SOURCE but not in manifest: {dst}")
    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 1
    print(
        f"ok: {len(MANIFEST)} vendored files match SOURCE (opteryx-core @ {record['meta'].get('commit', '?')[:8]})"
    )
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--source",
        default=os.path.join(ROOT, "..", "opteryx-core"),
        help="path to an opteryx-core checkout",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the vendored copy against SOURCE instead of copying",
    )
    args = parser.parse_args(argv)
    return check() if args.check else sync(args.source)


if __name__ == "__main__":
    sys.exit(main())
