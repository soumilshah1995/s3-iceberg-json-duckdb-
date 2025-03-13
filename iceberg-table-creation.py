#!/usr/bin/env python3
"""
Store Nested JSON Data as String in an Apache Iceberg Table on S3
"""

from pyiceberg.catalog import load_catalog
import pyarrow as pa
import json
import boto3

# Constants
REGION = 'us-east-2'
CATALOG = 's3tablescatalog'
DATABASE = 'myblognamespace'
TABLE_BUCKET = 'pyiceberg-blog-bucket'
CUSTOMERS_TABLE = 'customers_nested'

def initialize_catalog(account_id):
    """Initialize the Iceberg catalog using AWS Glue REST API."""
    try:
        rest_catalog = load_catalog(
            CATALOG,
            **{
                "type": "rest",
                "warehouse": f"{account_id}:{CATALOG}/{TABLE_BUCKET}",
                "uri": f"https://glue.{REGION}.amazonaws.com/iceberg",
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "glue",
                "rest.signing-region": REGION,
            },
        )
        print("Catalog loaded successfully!")
        return rest_catalog
    except Exception as e:
        print(f"Error loading catalog: {e}")
        return None

def create_customers_schema():
    """Create and return the PyArrow schema for the customers table."""
    return pa.schema([
        pa.field("customer_id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("contact_info", pa.string())  # JSON stored as a string
    ])

def create_table_if_not_exists(catalog, database, table_name, schema):
    """Check if the table exists, and create it if it doesn't."""
    try:
        catalog.create_table(identifier=f"{database}.{table_name}", schema=schema)
        print(f"Table {table_name} created successfully")
    except Exception as e:
        print(f"Table creation note: {e}")

def load_table(catalog, database, table_name):
    """Load an Iceberg table."""
    try:
        table = catalog.load_table(f"{database}.{table_name}")
        print(f"Table {table_name} schema: {table.schema()}")
        return table
    except Exception as e:
        print(f"Error loading the table: {e}")
        return None

def insert_data(table, data):
    """Insert data into the Iceberg table."""
    try:
        # Create a PyArrow schema that matches the Iceberg table schema
        pa_schema = pa.schema([
            pa.field("customer_id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field("contact_info", pa.string())
        ])
        table_data = pa.Table.from_pylist(data, schema=pa_schema)
        table.append(table_data)
        print("Data inserted successfully!")
    except Exception as e:
        print(f"Error inserting data: {e}")

def main():
    account_id = boto3.client('sts').get_caller_identity().get('Account')
    catalog = initialize_catalog(account_id)
    if not catalog:
        return

    customers_schema = create_customers_schema()
    create_table_if_not_exists(catalog, DATABASE, CUSTOMERS_TABLE, customers_schema)

    customers_table = load_table(catalog, DATABASE, CUSTOMERS_TABLE)
    if not customers_table:
        return

    # Insert sample data with nested JSON as a string
    customers_data = [
        {
            "customer_id": 1,
            "name": "Alice",
            "contact_info": json.dumps({
                "email": "alice@example.com",
                "phone": "123-456-7890",
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "zip": "10001"
                }
            })
        },
        {
            "customer_id": 2,
            "name": "Bob",
            "contact_info": json.dumps({
                "email": "bob@example.com",
                "phone": "987-654-3210",
                "address": {
                    "street": "456 Elm St",
                    "city": "Los Angeles",
                    "zip": "90001"
                }
            })
        }
    ]
    insert_data(customers_table, customers_data)

if __name__ == "__main__":
    main()
