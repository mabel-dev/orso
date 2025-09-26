<div align="center">

![Orso](https://raw.githubusercontent.com/mabel-dev/orso/main/orso.png)

**Orso is a shared DataFrame library for [Opteryx](https://opteryx.dev/) and [Mabel](https://github.com/mabel-dev/mabel).**

[![PyPI Latest Release](https://img.shields.io/pypi/v/orso.svg)](https://pypi.org/project/orso/)
[![Downloads](https://static.pepy.tech/badge/orso)](https://pepy.tech/project/orso)
[![codecov](https://codecov.io/gh/mabel-dev/orso/branch/main/graph/badge.svg?token=nl9JwOVdPs)](https://codecov.io/gh/mabel-dev/orso)
[![Documentation](https://img.shields.io/badge/Documentation-018EF5?logo=ReadMe&logoColor=fff&style=flat)](https://opteryx.dev/latest/get-started/ecosystem/orso/)

</div>

## Overview

Orso is not intended to compete with [Polars](https://www.pola.rs/) or [Pandas](https://pandas.pydata.org/) (or your favorite ~~bear~~ DataFrame technology), instead it is developed as a common layer for Mabel and Opteryx.

**Key Use Cases:**
- In [Opteryx](https://opteryx.dev/), Orso provides most of the database Cursor functionality
- In [Mabel](https://github.com/mabel-dev/mabel), Orso provides the data schema and validation functionality

Orso DataFrames are row-based, driven by their initial target use-case as the WAL for Mabel and Cursor for Opteryx. Each row in an Orso DataFrame can be quickly converted to a Tuple of values, a Dictionary, or a byte representation.

## Installation

Install Orso from PyPI:

```bash
pip install orso
```

## Quick Start

### Creating a DataFrame

```python
import orso

# Create from list of dictionaries
df = orso.DataFrame([
    {'name': 'Alice', 'age': 30, 'city': 'New York'},
    {'name': 'Bob', 'age': 25, 'city': 'San Francisco'},
    {'name': 'Charlie', 'age': 35, 'city': 'Chicago'}
])

print(f"Created DataFrame with {df.rowcount} rows and {df.columncount} columns")
```

### Displaying Data

```python
# Display the DataFrame
print(df.display())

# Convert to different formats
arrow_table = df.arrow()  # PyArrow Table
pandas_df = df.pandas()   # Pandas DataFrame
```

### Working with Schema

```python
# Access column names
print("Columns:", df.column_names)

# Access schema information  
print("Schema:", df.schema)
```

### Converting Between Formats

```python
# From PyArrow
import pyarrow as pa
arrow_table = pa.table({'x': [1, 2, 3], 'y': ['a', 'b', 'c']})
orso_df = orso.DataFrame.from_arrow(arrow_table)

# To Pandas
pandas_df = orso_df.pandas()
```

## Features

- **Lightweight**: Minimal overhead for tabular data operations
- **Row-based**: Optimized for row-oriented operations
- **Interoperable**: Easy conversion to/from PyArrow, Pandas
- **Schema-aware**: Built-in data validation and type checking
- **Fast serialization**: Efficient conversion to bytes, tuples, and dictionaries

## API Reference

### DataFrame Class

The main `DataFrame` class provides the following key methods:

- `DataFrame(dictionaries=None, *, rows=None, schema=None)` - Constructor
- `display(limit=5, colorize=True, show_types=True)` - Pretty print the DataFrame  
- `arrow(size=None)` - Convert to PyArrow Table
- `pandas(size=None)` - Convert to Pandas DataFrame
- `from_arrow(tables)` - Create DataFrame from PyArrow Table(s)
- `fetchall()` - Get all rows as list of Row objects
- `collect()` - Materialize the DataFrame
- `append(other)` - Append another DataFrame
- `distinct()` - Get unique rows

### Properties

- `rowcount` - Number of rows
- `columncount` - Number of columns  
- `column_names` - List of column names
- `schema` - Schema information

## Development

### Building from Source

```bash
# Clone the repository
git clone https://github.com/mabel-dev/orso.git
cd orso

# Install dependencies
pip install -r requirements.txt
pip install -r tests/requirements.txt

# Build Cython extensions
make compile

# Run tests
make test
```

### Contributing

Orso is part of the Mabel ecosystem. Contributions are welcome! Please ensure:

1. All tests pass: `make test`
2. Code follows the project style: `make lint`
3. New features include appropriate tests
4. Documentation is updated for API changes

## License

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/mabel-dev/orso/blob/main/LICENSE)

Orso is licensed under Apache 2.0 unless explicitly indicated otherwise.

## Status

[![Status](https://img.shields.io/badge/Status-beta-orange)](https://github.com/mabel-dev/orso)

Orso is in beta. Beta means different things to different people, to us, being beta means:

- Interfaces are generally stable but may still have breaking changes
- Unit tests are not reliable enough to capture breaks to functionality  
- Bugs are likely to exist in edge cases
- Code may not be tuned for performance

As such, we really don't recommend using Orso in critical applications.

## Related Projects

- **[Opteryx](https://opteryx.dev/)** - SQL query engine for data files
- **[Mabel](https://github.com/mabel-dev/mabel)** - Data processing framework