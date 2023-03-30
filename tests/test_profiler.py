import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))


def test_can_profile():
    # fmt:off
    cities_list:list = [
        {"name": "Tokyo", "population": 13929286, "country": "Japan", "founded": "1457", "area": 2191, "language": "Japanese"},
        {"name": "London", "population": 8982000, "country": "United Kingdom", "founded": "43 AD", "area": 1572, "language": "English"},
        {"name": "New York City", "population": 8399000, "country": "United States", "founded": "1624", "area": 468.9, "language": "English"},
        {"name": "Mumbai", "population": 18500000, "country": "India", "founded": "7th century BC", "area": 603.4, "language": "Hindi, English"},
        {"name": "Cape Town", "population": 433688, "country": "South Africa", "founded": "1652", "area": 400, "language": "Afrikaans, English"},
        {"name": "Paris", "population": 2148000, "country": "France", "founded": "3rd century BC", "area": 105.4, "language": "French"},
        {"name": "Beijing", "population": 21710000, "country": "China", "founded": "1045", "area": 16410.54, "language": "Mandarin"},
        {"name": "Rio de Janeiro", "population": 6747815, "country": "Brazil", "founded": "1 March 1565", "area": 1264, "language": "Portuguese"},
        {"name": "Moscow", "population": 12506468, "country": "Russia", "founded": "1147", "area": 2511, "language": "Russian"},
        {"name": "Sydney", "population": 5312163, "country": "Australia", "founded": "1788", "area": 12144.6, "language": "English"},
        {"name": "Berlin", "population": 3669491, "country": "Germany", "founded": "1237", "area": 891.8, "language": "German"},
        {"name": "Istanbul", "population": 15462482, "country": "Turkey", "founded": "660 BC", "area": 5461, "language": "Turkish"},
        {"name": "Lagos", "population": 14913700, "country": "Nigeria", "founded": "15th century", "area": 1171.3, "language": "English"},
        {"name": "Bangkok", "population": 10089267, "country": "Thailand", "founded": "1782", "area": 1568.7, "language": "Thai"},
        {"name": "Toronto", "population": 2930000, "country": "Canada", "founded": "1750", "area": 630.2, "language": "English, French"},
        {"name": "Dubai", "population": 3013000, "country": "United Arab Emirates", "founded": "1833", "area": 4114, "language": "Arabic"},
        {"name": "Mexico City", "population": 8918653, "country": "Mexico", "founded": "1325", "area": 1485, "language": "Spanish"},
        {"name": "Stockholm", "population": 975551, "country": "Sweden", "founded": "1187", "area": 188, "language": "Swedish"},
        {"name": "Seoul", "population": 9720846, "country": "South Korea", "founded": "18 BC", "area": 605.2, "language": "Korean"},
        {"name": "Rome", "population": 2870500, "country": "Italy", "founded": "753 BC", "area": 1285, "language": "Italian"}
    ]
    import orso
    cities = orso.DataFrame(cities_list)
    profile = cities.profile

    assert profile.shape == (6, 10), profile.shape
    assert profile.collect("count") == [20] * 6

    # we've seen problems that this has uncovered
    for i in range(5):
        profile = profile.profile


if __name__ == "__main__":  # pragma: no cover
    test_can_profile()

    print("✅ okay")
