<div align="center">

![Orso](https://raw.githubusercontent.com/mabel-dev/orso/main/orso.png)

**Orso is a shared DataFrame library for [Opteryx](https://opteryx.dev/) and [HadroDB](https://github.com/mabel-dev/hadrodb).**

[![PyPI Latest Release](https://img.shields.io/pypi/v/orso.svg)](https://pypi.org/project/orso/)
[![Downloads](https://static.pepy.tech/badge/orso)](https://pepy.tech/project/orso)
[![codecov](https://codecov.io/gh/mabel-dev/orso/branch/main/graph/badge.svg?token=nl9JwOVdPs)](https://codecov.io/gh/mabel-dev/orso)
[![Documentation](https://img.shields.io/badge/Documentation-018EF5?logo=ReadMe&logoColor=fff&style=flat)](https://opteryx.dev/latest/get-started/ecosystem/orso/)

</div>

Orso is not intended to compete with [Polars](https://www.pola.rs/) or [Pandas](https://pandas.pydata.org/) (or your favorite DataFrame technology), instead it is developed as a common layer for HadroDB and Opteryx.

In Opteryx, orso provides much of the functionality of the Cursor.

In HadroDB, orso provides functionality for handling datasets.

## License

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/mabel-dev/orso/blob/master/LICENSE)

Orso is licensed under Apache 2.0 unless explicitly indicated otherwise.

## Status

[![Status](https://img.shields.io/badge/Status-alpha-orange)](https://github.com/mabel-dev/orso)

Orso is in alpha. Alpha means different things to different people, to us, being alpha means:

- Interfaces may be significantly changed
- Expected functionality is missing
- Things that worked yesterday, don't work today
- The results of the system may be unreliable

As such, we really don't recommend using Orso anywhere where your data matters.