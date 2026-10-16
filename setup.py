import os
import platform
import sysconfig

import numpy
from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup
from setuptools.command.build_ext import build_ext as _build_ext

LIBRARY = "orso"


def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"


# Windows is not supported from v2 on (DESIGN.md §11, decided 2026-10-16): the native
# layer is built on draken headers that need __int128 and <execinfo.h>, neither of
# which MSVC has, and the alternatives were not worth carrying.
if platform.system().lower() == "windows":  # pragma: no cover
    raise SystemExit("orso does not build on Windows; Linux and macOS wheels are published.")

if is_mac():  # pragma: no cover
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

# ---------------------------------------------------------------------------------
# v1 Cython extensions. These stay until phase 6 of the v2 rewrite deletes
# orso/compute (DESIGN.md §10); nothing in v1 changes while the native layer is
# being built beside it.
# ---------------------------------------------------------------------------------
extensions = [
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
ext_modules = cythonize(extensions)


# ---------------------------------------------------------------------------------
# v2 native layer: one nanobind extension, `orso._native`, over the draken headers
# vendored into native/ by `make sync-draken` (DESIGN.md §3, §7).
#
# Flags follow opteryx-core's build_common.py: -O3, C++20, an ISA floor rather than
# -march=native so published wheels run on any CPU of the generation, and
# NB_FREE_THREADED when the build interpreter has the GIL disabled.
# ---------------------------------------------------------------------------------
NATIVE_DIR = "native"
DRAKEN_SOURCE_RECORD = os.path.join(NATIVE_DIR, "draken", "SOURCE")


def _draken_source_commit() -> str:
    try:
        with open(DRAKEN_SOURCE_RECORD) as f:
            for line in f:
                if line.startswith("commit: "):
                    return line.split(": ", 1)[1].strip()
    except OSError:
        pass
    return "unknown"


def _native_arch_flags() -> list:
    """ISA floor per target. A universal2 macOS build (both archs in ARCHFLAGS)
    gets none, because one -march cannot serve two architectures."""
    archflags = os.environ.get("ARCHFLAGS", "").lower()
    has_x86 = "x86_64" in archflags
    has_arm = "arm64" in archflags or "aarch64" in archflags
    if has_x86 and has_arm:
        return []
    machine = platform.machine().lower()
    if has_x86 or (not has_arm and machine in ("x86_64", "amd64")):
        # haswell: AVX2/BMI/FMA. Everything since 2013 on the server side.
        return ["-march=haswell", "-mtune=generic"]
    if has_arm or "aarch64" in machine or "arm64" in machine:
        return ["-march=armv8-a+aes", "-mtune=generic"]
    return []


def _native_extension():
    if not os.path.exists(DRAKEN_SOURCE_RECORD):
        raise SystemExit(
            f"{DRAKEN_SOURCE_RECORD} not found: the vendored draken headers are missing. "
            "Run `make sync-draken` (needs an opteryx-core checkout beside this one)."
        )

    import nanobind

    nanobind_root = os.path.dirname(nanobind.__file__)
    ryu_dir = os.path.join(NATIVE_DIR, "third_party", "ulfjack", "ryu")

    cpp_flags = ["-O3", "-std=c++20", "-fvisibility=hidden", "-fno-strict-aliasing"] + _native_arch_flags()
    define_macros = [
        ("NB_COMPACT_ASSERTIONS", "1"),
        # Keep orso's nanobind type registry separate from any other nanobind
        # extension in the process (opteryx-core ships its own vendored nanobind).
        ("NB_DOMAIN", "orso"),
        ("ORSO_DRAKEN_SOURCE", f'"{_draken_source_commit()}"'),
    ]
    if sysconfig.get_config_var("Py_GIL_DISABLED"):
        # nanobind must see this in nb_combined.cpp AND every unit that includes its
        # headers, so it is a define on the extension, not a flag on one source.
        define_macros.append(("NB_FREE_THREADED", "1"))

    return Extension(
        name="orso._native",
        sources=[
            os.path.join(NATIVE_DIR, "native.cpp"),
            os.path.join(NATIVE_DIR, "draken", "core", "mem_account.cpp"),
            os.path.join(ryu_dir, "d2s.c"),
            os.path.join(ryu_dir, "d2fixed.c"),
            os.path.join(ryu_dir, "f2s.c"),
            os.path.join(nanobind.source_dir(), "nb_combined.cpp"),
        ],
        include_dirs=[
            NATIVE_DIR,
            os.path.join(NATIVE_DIR, "draken"),  # "core/buffers.h", "logical_type.h", "ops/vec_result.h"
            os.path.join(NATIVE_DIR, "draken", "core"),  # draken's own quote-includes ("string_slot.h")
            os.path.join(NATIVE_DIR, "third_party", "cyan4973"),  # "xxhash.h"
            ryu_dir,  # "ryu.h"
            nanobind.include_dir(),
            nanobind.source_dir(),
            os.path.join(nanobind_root, "ext", "robin_map", "include"),
        ],
        define_macros=define_macros,
        extra_compile_args=cpp_flags,
        extra_link_args=LINK_FLAGS,
        language="c++",
        depends=[
            os.path.join(NATIVE_DIR, "record.h"),
            DRAKEN_SOURCE_RECORD,
        ],
    )


class build_ext(_build_ext):
    """The native extension mixes C++20 units with ryu's C sources. setuptools applies
    one set of extra_compile_args to every source, and clang refuses -std=c++20 for a
    C unit, so give C units a C standard instead (the same fix build_common.py makes)."""

    def build_extensions(self):
        original_compile = self.compiler._compile

        def _compile(obj, src, ext, cc_args, extra_postargs, pp_opts):
            if src.endswith(".c"):
                extra_postargs = [a for a in extra_postargs if not a.startswith("-std=")]
                extra_postargs = [a for a in extra_postargs if a != "-fvisibility=hidden"] + ["-std=c11"]
            return original_compile(obj, src, ext, cc_args, extra_postargs, pp_opts)

        if hasattr(self.compiler, "_compile"):
            self.compiler._compile = _compile
        super().build_extensions()


ext_modules.append(_native_extension())

setup_config = {
    "name": LIBRARY,
    "version": __version__,
    "description": "🐻 DataFrame Library",
    "long_description": long_description,
    "long_description_content_type": "text/markdown",
    "author_email": "justin.joyce@joocer.com",
    "packages": find_packages(include=[LIBRARY, f"{LIBRARY}.*"]),
    "url": "https://github.com/mabel-dev/orso/",
    "ext_modules": ext_modules,
    "cmdclass": {"build_ext": build_ext},
    "install_requires": required,
    # 3.9 is EOL (Oct 2025) and our own dependencies no longer publish cp39
    # wheels; without this pip would offer 3.9 users an sdist they cannot use.
    "python_requires": ">=3.10",
    "extras_require": {
        # pysimdjson provides the `simdjson` Python module used as an optional accelerator.
        "simdjson": ["pysimdjson"],
    },
    "package_data": {
        "": ["*.pyx", "*.pxd"],
    },
}

setup(**setup_config)
