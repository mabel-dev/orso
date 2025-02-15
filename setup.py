import platform

import numpy
from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup

LIBRARY = "orso"


def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"


COMPILE_FLAGS = ["-O2"] if is_mac() else ["-O2", "-march=native"]

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
    # Cython code
    Extension(
        name="orso.compute.compiled",
        sources=["orso/compute/compiled.pyx"],
        include_dirs=[numpy.get_include()],
        extra_compile_args=COMPILE_FLAGS,
        extra_link_args=COMPILE_FLAGS,
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
    "package_data": {
        "": ["*.pyx", "*.pxd"],
    },
}

setup(**setup_config)
