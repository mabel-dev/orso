from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup

LIBRARY = "orso"


__version__ = "notset"
with open(f"{LIBRARY}/version.py", mode="r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", mode="r", encoding="UTF8") as rm:
    long_description = rm.read()

try:
    with open("requirements.txt", "r") as f:
        required = f.read().splitlines()
except:
    with open(f"{LIBRARY}.egg-info/requires.txt", "r") as f:
        required = f.read().splitlines()

extensions = [
    Extension(
        name="orso.cityhash.cityhash",
        sources=[
            "orso/cityhash/city.cc",
            "orso/cityhash/cityhash.cpp",
        ],
    ),
    Extension(
        name="orso.bitarray.cbitarray",
        sources=["orso/bitarray/cbitarray.pyx"],
    ),
]

setup_config = {
    "name": LIBRARY,
    "version": __version__,
    "description": "🐻 DataFrame Library",
    "long_description": long_description,
    "long_description_content_type": "text/markdown",
    "author_email": "justin.joyce@joocer.com",
    "packages": find_packages(include=[LIBRARY, f"{LIBRARY}.*"]),
    "url": "https://github.com/mabel-dev/orso/",
    "ext_modules": cythonize(extensions),
    "install_requires": required,
}

setup(**setup_config)
