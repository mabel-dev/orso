tests:
integrations:
- can convert from arrow table
- can convert to pandas
- can convert to polars

for opteryx
- fetchall
- fetchmany
- fetchone
? shape
? head
? describe?

for hadro
- can load with tuples
- can validate against a schema

other
- .column(name or id)
* .column_names
- .filter(mask)
- .from_ arrow/pandas/polars/lists
- .select(columns) <- project
* .slice()
- .sort_by()
- .take(indices)
- .shape
- .num_rows
- .num_columns
- .distinct()
- for r in df:
- df[index]
- .head()
- .tail()
- .query(filter)
- .group_by()
-------------

features
- the above
- profile dataset