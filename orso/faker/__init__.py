import random

from orso.dataframe import DataFrame
from orso.faker.decimals import generate_random_decimal
from orso.faker.names import generate_random_name
from orso.faker.temporal import generate_random_datetime
from orso.schema import ColumnDisposition
from orso.schema import RelationSchema
from orso.tools import random_int
from orso.tools import random_string
from orso.types import OrsoTypes


def generate_random_row(schema: RelationSchema) -> tuple:
    """
    Generates a random row of values based on the given schema.

    Parameters:
        schema: RelationSchema
            The schema to generate the random row for.

    Returns:
        tuple: A tuple of random values based on the schema.
    """
    columns = [schema.column(i) for i in range(schema.num_columns)]
    row = []
    for column in columns:
        # if the column is nullable, set 1% of the values to null
        if column.nullable and random_int() % 100 == 0:
            row.append(None)
        elif column.type == OrsoTypes.INTEGER:
            if column.disposition == ColumnDisposition.AGE:
                row.append(random.randint(0, 100))
            else:
                row.append(random_int())
        elif column.type == OrsoTypes.VARCHAR:
            if column.disposition == ColumnDisposition.NAME:
                row.append(generate_random_name())
            else:
                row.append(random_string(random_int() % 16 + 8))
        elif column.type == OrsoTypes.BOOLEAN:
            row.append(bool(random.getrandbits(1)))
        elif column.type == OrsoTypes.DECIMAL:
            row.append(generate_random_decimal(column.precision, column.scale))
        elif column.type == OrsoTypes.DOUBLE:
            row.append(random.random())
        elif column.type == OrsoTypes.TIMESTAMP:
            row.append(generate_random_datetime())
        else:
            raise TypeError(f"Orso currently cannot fake {column.type} values.")
    return tuple(row)


def generate_fake_data(schema: RelationSchema, size: int = 100) -> DataFrame:
    """
    Generates a DataFrame of fake data based on the given schema and size.

    Parameters:
        schema: RelationSchema
            The schema to generate the DataFrame for.
        size: int
            The number of rows to generate.

    Returns:
        DataFrame: A DataFrame containing the generated fake data.
    """
    rows = [generate_random_row(schema) for i in range(size)]
    return DataFrame(rows=rows, schema=schema)
