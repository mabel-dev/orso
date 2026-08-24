import os
import platform

import numpy
from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup

LIBRARY = "orso"


def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"


def is_windows():  # pragma: no cover
    return platform.system().lower() == "windows"


# MSVC does not understand GCC/Clang style flags. Passing "-O2"/"-march=native" to
# cl.exe and link.exe only produces D9002/LNK4044 "unknown option" diagnostics, so the
# extensions end up built unoptimised. Use the MSVC spellings on Windows instead.
if is_windows():  # pragma: no cover
    COMPILE_FLAGS = ["/O2"]
    LINK_FLAGS = []
elif is_mac():  # pragma: no cover
    COMPILE_FLAGS = ["-O2"]
    LINK_FLAGS = ["-O2"]
else:  # pragma: no cover
    # NOTE: do not add -march=native here. Wheels are built on CI and published, so
    # baking in the build machine's instruction set makes them crash with SIGILL on
    # any older CPU. Opt in explicitly for local/self-hosted builds instead.
    COMPILE_FLAGS = ["-O2"]
    LINK_FLAGS = ["-O2"]
    if os.environ.get("ORSO_NATIVE_ARCH") == "1":
        COMPILE_FLAGS.append("-march=native")
        LINK_FLAGS.append("-march=native")

__version__ = "notset"
with open(f"{LIBRARY}/version.py", mode="r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", mode="r", encoding="UTF8") as rm:
    long_description = rm.read()

def _read_requirements() -> list:
    """
    Read the install requirements.

    requirements.txt is the source of truth and is shipped in the sdist. The
    egg-info fallback exists for trees where it is missing, but that file also
    carries `[extras]` sections which are not valid requirement specifiers - reading
    them verbatim made the sdist impossible to build, so stop at the first section.
    """
    try:
        with open("requirements.txt", "r") as f:
            lines = f.read().splitlines()
    except OSError:
        with open(f"{LIBRARY}.egg-info/requires.txt", "r") as f:
            lines = []
            for line in f.read().splitlines():
                if line.startswith("["):  # start of an extras section
                    break
                lines.append(line)

    return [line.strip() for line in lines if line.strip() and not line.startswith("#")]


required = _read_requirements()

extensions = [
    # Cython code
    Extension(
        name="orso.compute.compiled",
        sources=["orso/compute/compiled.pyx"],
        include_dirs=[numpy.get_include()],
        extra_compile_args=COMPILE_FLAGS,
        extra_link_args=LINK_FLAGS,
    ),
    Extension(
        name="orso.compute.column_encodings",
        sources=["orso/compute/column_encodings.pyx"],
        include_dirs=[numpy.get_include(), "orso/compute"],
        extra_compile_args=COMPILE_FLAGS,
        extra_link_args=LINK_FLAGS,
        language="c++",
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
    "extras_require": {
        # pysimdjson provides the `simdjson` Python module used as an optional accelerator.
        "simdjson": ["pysimdjson"],
    },
    "package_data": {
        "": ["*.pyx", "*.pxd"],
    },
}

setup(**setup_config)
