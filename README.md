<div align="center">

![Orso](https://raw.githubusercontent.com/mabel-dev/orso/main/orso.png)

**Orso is a shared DataFrame library for [Opteryx](https://opteryx.dev/) and [Mabel](https://github.com/mabel-dev/mabel).**

[![PyPI Latest Release](https://img.shields.io/pypi/v/orso.svg)](https://pypi.org/project/orso/)
[![Downloads](https://static.pepy.tech/badge/orso)](https://pepy.tech/project/orso)
[![codecov](https://codecov.io/gh/mabel-dev/orso/branch/main/graph/badge.svg?token=nl9JwOVdPs)](https://codecov.io/gh/mabel-dev/orso)
[![Documentation](https://img.shields.io/badge/Documentation-018EF5?logo=ReadMe&logoColor=fff&style=flat)](https://opteryx.dev/latest/get-started/ecosystem/orso/)

</div>

Orso is not intended to compete with [Polars](https://www.pola.rs/) or [Pandas](https://pandas.pydata.org/) (or your favorite ~~bear~~ DataFrame technology), instead it is developed as a common layer for Mabel and Opteryx.

In Opteryx, Orso provides most of the database Cursor functionality.

In Mabel, Orso provides the data schema and validation functionality.

Orso DataFrames are row-based, this is driven by it's initial target use-case as the WAL for Mabel and Cursor for Opteryx.

Each row in an Orso Dataframe is able to be be quickly converted to a Tuple of values, a Dictionary or to a byte representation.

## License

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/mabel-dev/orso/blob/master/LICENSE)

Orso is licensed under Apache 2.0 unless explicitly indicated otherwise.

## Status

[![Status](https://img.shields.io/badge/Status-beta-orange)](https://github.com/mabel-dev/orso)

Orso is in beta. Beta means different things to different people, to us, being beta means:

- Interfaces are generally stable but may still have breaking changes
- Unit test are not reliable enough to capture breaks to functionality
- Bugs are likely to exist in edge cases
- Code may not be tuned for performance

As such, we really don't recommend using Orso in critical applications.