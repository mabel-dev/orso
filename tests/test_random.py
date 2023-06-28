import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import time
from orso.tools import random_int
from orso.tools import random_string


def test_random():
    collected = [random_int() for i in range(1000000)]
    # allow some collisions
    assert len(set(collected)) > (len(collected) * 0.999), len(set(collected))

    t = time.monotonic_ns()
    collected = [random_string() for i in range(1000000)]
    t = time.monotonic_ns() - t

    assert len(set(collected)) == len(collected)
    assert all(len(c) == 16 for c in collected)
    assert t < 5e8, t  # it should take less than 0.5 second to generate 1m items


if __name__ == "__main__":  # pragma: no cover
    test_random()

    print("✅ okay")
