import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from tests.cities import cities_list


def test_can_profile():
    # fmt:off
    
    import orso
    cities = orso.DataFrame(cities_list)
    profile = cities.profile

    assert profile.shape == (6, 10), profile.shape
    assert profile.collect("count") == [20] * 6

    # we've seen problems that this has uncovered
    for i in range(5):
        profile = profile.profile


def test_opteryx_profile():
    import opteryx

    planets = opteryx.query("SELECT * FROM $planets")
    profile = planets.profile
    assert profile.shape == (20, 10), profile.shape
    assert profile.collect("count") == [9] * 20


if __name__ == "__main__":  # pragma: no cover
    test_can_profile()
    test_opteryx_profile()

    print("✅ okay")
