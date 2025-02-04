import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from orso.dataframe import DataFrame
from tests import cities
import re
from typing import List

lengths = {
    0: 5,
    1: 6,
    2: 7,
    3: 8,
    4: 9,
    5: 10,
    6: 11,
    7: 12,
    8: 12,
    9: 12,
    10: 12,
}



def find_all_substrings(s: str, sub: str) -> List[int]:
    """
    Finds all instances of a substring within a string and returns their start indices.

    Parameters:
        s (str): The string to search within.
        sub (str): The substring to search for.

    Returns:
        List[int]: A list of start indices where the substring is found.
    """
    return [m.start() for m in re.finditer(re.escape(sub), s)]

def test_display_ascii_lazy():

    for i in range(10):
        df = DataFrame(cities.values).head(i)
        df._rows = (r for r in df._rows)

        ascii = df.display(limit=3, show_types=True)

        assert len(ascii.split("\n")) == lengths[i], i
        assert len(find_all_substrings(ascii, "Tokyo")) == (1 if i != 0 else 0)


def test_display_ascii_greedy():

    for i in range(10):

        df = DataFrame(cities.values).head(i)
        df.materialize()

        ascii = df.display(limit=3, show_types=True)

        assert len(ascii.split("\n")) == lengths[i], i
        assert len(find_all_substrings(ascii, "Tokyo")) == (1 if i != 0 else 0)



if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
