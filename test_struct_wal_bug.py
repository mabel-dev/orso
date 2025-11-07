"""
Test to replicate the struct column WAL bug.

The issue: When using orso as a WAL (append functionality + .arrow() call),
struct columns are converted to binary/blob in the schema's arrow_field property,
but the actual data remains as struct/dict. This causes a type mismatch.
"""

import pyarrow as pa
from pyarrow.lib import ArrowException

from orso import DataFrame
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes


def test_struct_wal_without_schema():
    """Test appending struct data without explicit schema."""
    print("\n=== Test 1: WAL without explicit schema ===")
    
    # Create a DataFrame with initial data to establish schema
    data = [
        {"id": 1, "details": {"name": "Alice", "age": 30}},
        {"id": 2, "details": {"name": "Bob", "age": 25}},
    ]
    
    df = DataFrame(dictionaries=data)
    
    # Append more data (simulating WAL usage)
    df.append({"id": 3, "details": {"name": "Charlie", "age": 35}})
    
    print("DataFrame schema:", df.schema)
    print("DataFrame columns:", df.column_names)
    
    # Try to convert to Arrow - this is where the bug should manifest
    try:
        arrow_table = df.arrow()
    except ArrowException as error:
        print("✗ Arrow conversion failed with error:")
        print(f"  {error.__class__.__name__}: {error}")
        return False

    print("✓ Arrow conversion succeeded!")
    print("Arrow schema:", arrow_table.schema)
    print("Arrow table:\n", arrow_table)
    return True


def test_struct_wal_with_schema():
    """Test appending struct data with explicit schema."""
    print("\n=== Test 2: WAL with explicit schema ===")
    
    # Create a schema with a struct column
    schema = RelationSchema(
        name="test_schema",
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER),
            FlatColumn(name="details", type=OrsoTypes.STRUCT),
        ]
    )
    
    print("Schema columns:")
    for col in schema.columns:
        arrow_field = col.arrow_field
        print(f"  - {col.name}: {col.type} -> Arrow {arrow_field.type}")
    
    # Create DataFrame with schema
    df = DataFrame(schema=schema)
    
    data = [
        {"id": 1, "details": {"name": "Alice", "age": 30}},
        {"id": 2, "details": {"name": "Bob", "age": 25}},
        {"id": 3, "details": {"name": "Charlie", "age": 35}},
    ]
    
    for row in data:
        df.append(row)
    
    print(f"\nDataFrame has {len(df)} rows")
    
    # Try to convert to Arrow - this is where the bug should manifest
    try:
        arrow_table = df.arrow()
    except ArrowException as error:
        print("✗ Arrow conversion failed with error:")
        print(f"  {error.__class__.__name__}: {error}")
        return False

    print("✓ Arrow conversion succeeded!")
    print("Arrow schema:", arrow_table.schema)
    print("Arrow table:\n", arrow_table)
    return True


def test_struct_from_arrow_roundtrip():
    """Test creating DataFrame from Arrow table with struct, then converting back."""
    print("\n=== Test 3: Arrow -> Orso -> Arrow roundtrip ===")
    
    # Create an Arrow table with a struct column
    struct_type = pa.struct([
        pa.field('name', pa.string()),
        pa.field('age', pa.int64())
    ])
    
    arrow_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('details', struct_type)
    ])
    
    data = [
        [1, 2, 3],
        [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}, {'name': 'Charlie', 'age': 35}]
    ]
    
    original_table = pa.table(data, schema=arrow_schema)
    print(f"Original Arrow table schema: {original_table.schema}")
    print(f"Original Arrow table:\n{original_table}")
    
    # Convert to Orso DataFrame
    try:
        df = DataFrame.from_arrow(original_table)
        print("\n✓ Created DataFrame from Arrow")
        print("DataFrame schema:", df.schema)

        arrow_table = df.arrow()
    except ArrowException as error:
        print("✗ Conversion failed with error:")
        print(f"  {error.__class__.__name__}: {error}")
        return False

    print("✓ Converted back to Arrow!")
    print("Converted Arrow schema:", arrow_table.schema)
    print("Converted Arrow table:\n", arrow_table)
    return True


def test_schema_arrow_field_for_struct():
    """Test what arrow_field property returns for struct columns."""
    print("\n=== Test 4: Schema arrow_field property for STRUCT ===")
    
    # Create a FlatColumn with STRUCT type
    struct_col = FlatColumn(name="details", type=OrsoTypes.STRUCT)
    fallback_type = struct_col.arrow_field.type

    nested_struct = FlatColumn(
        name="details",
        type=OrsoTypes.STRUCT,
        fields=[
            FlatColumn(name="name", type=OrsoTypes.VARCHAR),
            FlatColumn(name="age", type=OrsoTypes.INTEGER),
        ],
    )
    nested_type = nested_struct.arrow_field.type

    print("Fallback arrow type:", fallback_type)
    print("Nested arrow type:", nested_type)

    return fallback_type == pa.binary() and nested_type == pa.struct(
        [pa.field("name", pa.string()), pa.field("age", pa.int64())]
    )


if __name__ == "__main__":
    print("=" * 60)
    print("Testing struct column WAL bug")
    print("=" * 60)
    
    test4_result = test_schema_arrow_field_for_struct()
    test1_result = test_struct_wal_without_schema()
    test2_result = test_struct_wal_with_schema()
    test3_result = test_struct_from_arrow_roundtrip()
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print("=" * 60)
    print(f"Test 4 (STRUCT arrow_field behavior): {'✓ PASS' if test4_result else '✗ FAIL'}")
    print(f"Test 1 (WAL without schema): {'✓ PASS' if test1_result else '✗ FAIL'}")
    print(f"Test 2 (WAL with schema): {'✓ PASS' if test2_result else '✗ FAIL'}")
    print(f"Test 3 (Arrow roundtrip): {'✓ PASS' if test3_result else '✗ FAIL'}")
    print("=" * 60)
    
    if all([test1_result, test2_result, test3_result, test4_result]):
        print("\n✓ All tests passed - structs now round-trip as structs")
    else:
        print("\n⚠ Mixed results - further investigation needed")
