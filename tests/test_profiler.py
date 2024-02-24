import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from tests import cities


def test_can_profile():
    # fmt:off
    
    import orso
    df = orso.DataFrame(cities.values)
    profile = df.profile.to_dataframe()

    assert profile.shape == (6, 10), profile.shape
    assert profile.collect("count") == [20] * 6


def test_opteryx_profile_planets():
    try:
        sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
        import opteryx

        planets = opteryx.query("SELECT * FROM $planets")
        profile = planets.profile.to_dataframe()
        assert profile.shape == (20, 10), profile.shape
        assert profile.collect("count") == [9] * 20
    except ImportError:
        # if Opteryx isn't installed, don't fail
        pass


def test_opteryx_profile_satellites():
    try:
        sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
        import opteryx

        planets = opteryx.query("SELECT * FROM $satellites")
        profile = planets.profile.to_dataframe()
        assert profile.shape == (8, 10), profile.shape
        assert profile.collect("count") == [177] * 8
    except ImportError:
        # if Opteryx isn't installed, don't fail
        pass


def test_opteryx_profile_astronauts():
    try:
        sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
        import opteryx

        planets = opteryx.query("SELECT * FROM $astronauts")
        profile = planets.profile.to_dataframe()
        assert profile.shape == (19, 10), profile.shape
        assert profile.collect("count") == [357] * 19
    except ImportError:
        # if Opteryx isn't installed, don't fail
        pass


def test_opteryx_profile_fake():
    try:
        sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
        import opteryx

        planets = opteryx.query("SELECT * FROM FAKE(100, 100) AS FK")
        profile = planets.profile.to_dataframe()
        assert profile.shape == (100, 10), profile.shape
        assert profile.collect("count") == [100] * 100
    except ImportError:
        # if Opteryx isn't installed, don't fail
        pass


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
