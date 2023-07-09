import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from tests import cities


def test_can_profile():
    # fmt:off
    
    import orso
    df = orso.DataFrame(cities.values)
    profile = df.profile

    assert profile.shape == (6, 10), profile.shape
    assert profile.collect("count") == [20] * 6

    # we've seen problems that this has uncovered
    for i in range(5):
        profile = profile.profile


def test_opteryx_profile():
    try:
        sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
        import opteryx

        planets = opteryx.query("SELECT * FROM $planets")
        profile = planets.profile
        assert profile.shape == (20, 10), profile.shape
        assert profile.collect("count") == [9] * 20
    except ImportError:
        # if Opteryx isn't installed, don't fail
        pass


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
