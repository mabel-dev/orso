import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from tests import cities


def test_can_profile():
    # fmt:off
    
    import orso
    df = orso.DataFrame(cities.values)
    profile = df.profile.to_dataframe()

    assert profile.shape == (6, 12), profile.shape
    assert profile.collect("count").tolist() == [20] * 6


def test_opteryx_profile_planets():
    try:
        sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
        import opteryx

        planets = opteryx.query("SELECT * FROM $planets")
        profile = planets.profile.to_dataframe()
        assert profile.shape == (20, 12), profile.shape
        assert profile.collect("count").tolist() == [9] * 20
    except ImportError:
        # if Opteryx isn't installed, don't fail
        pass


def test_opteryx_profile_satellites():
    try:
        sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
        import opteryx

        planets = opteryx.query("SELECT * FROM $satellites")
        profile = planets.profile.to_dataframe()
        assert profile.shape == (8, 12), profile.shape
        assert profile.collect("count").tolist() == [177] * 8
    except ImportError:
        # if Opteryx isn't installed, don't fail
        pass


def test_opteryx_profile_astronauts():
    try:
        sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
        import opteryx

        planets = opteryx.query("SELECT * FROM $astronauts")
        profile = planets.profile.to_dataframe()
        assert profile.shape == (19, 12), profile.shape
        assert profile.collect("count").tolist() == [357] * 19
    except ImportError:
        # if Opteryx isn't installed, don't fail
        pass


def test_opteryx_profile_missions():
    try:
        sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
        import opteryx

        missions = opteryx.query("SELECT * FROM $missions")
        profile = missions.profile.to_dataframe()
        assert profile.shape == (8, 12), profile.shape
        assert profile.collect("count").tolist() == [4630] * 8, profile.collect("count")
    except ImportError:
        # if Opteryx isn't installed, don't fail
        pass


def test_opteryx_profile_fake():
    try:
        sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
        import opteryx

        planets = opteryx.query("SELECT * FROM FAKE(100, 100) AS FK")
        profile = planets.profile.to_dataframe()
        assert profile.shape == (100, 12), profile.shape
        assert profile.collect("count").tolist() == [100] * 100
    except ImportError:
        # if Opteryx isn't installed, don't fail
        pass


def test_profile_estimators():
    """
    Make sure the estimators are returning something like what
    they're meant to.

    Build a profile, and then test against results from SQL queries
    which perform the same analysis
    """
    sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
    import opteryx
    from orso.profiler import TableProfile

    missions = opteryx.query("SELECT * FROM $missions")
    profile: TableProfile = missions.profile

    source = opteryx.query("SELECT COUNT(*) as missing FROM $missions WHERE Lauched_at IS NULL")
    values = source.fetchone().as_dict
    assert (
        profile.column("Lauched_at").missing == values["missing"]
    ), f"{profile.column('Lauched_at').missing}, {values['missing']}"

    source = opteryx.query("SELECT MIN(Price) as minimum FROM $missions")
    values = source.fetchone().as_dict
    assert profile.column("Price").minimum == int(
        values["minimum"]
    ), f"{profile.column('Price').minimum} != {values['minimum']}"

    source = opteryx.query("SELECT MAX(Price) as maximim FROM $missions")
    values = source.fetchone().as_dict
    assert profile.column("Price").maximum == int(
        values["maximim"]
    ), f"{profile.column('Price').maximum} != {values['maximim']}"

    source = opteryx.query(
        "SELECT COUNT(*) AS frequency, Company FROM $missions GROUP BY Company ORDER BY COUNT(*) DESC"
    )
    values = source.fetchone().as_dict
    assert profile.column("Company").most_frequent_values[0] == values["Company"], values
    assert profile.column("Company").most_frequent_counts[0] == values["frequency"], values

    source = opteryx.query("SELECT COUNT_DISTINCT(Lauched_at) AS unique_timestamps FROM $missions")
    values = source.fetchone().as_dict
    estimated_cardinality = profile.column("Lauched_at").estimate_cardinality()
    assert (
        estimated_cardinality * 0.70 < values["unique_timestamps"] < estimated_cardinality * 1.30
    ), f"{profile.column('Lauched_at').estimate_cardinality()} != {values['unique_timestamps']}"

    source = opteryx.query("SELECT COUNT(*) AS price_over_100 FROM $missions WHERE Price > 100")
    values = source.fetchone().as_dict
    estimate = profile.column("Price").estimate_values_above(100)
    assert (
        estimate * 0.9 < values["price_over_100"] < estimate * 1.1
    ), f"{estimate}, {values['price_over_100']}"

    source = opteryx.query("SELECT COUNT(*) AS price_under_250 FROM $missions WHERE Price < 250")
    values = source.fetchone().as_dict
    estimate = profile.column("Price").estimate_values_below(250)
    assert (
        estimate * 0.9 < values["price_under_250"] < estimate * 1.1
    ), f"{estimate}, {values['price_under_250']}"


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
