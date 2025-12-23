"""
Load asset: Load CSV data into DuckDB raw tables.

This module takes the generated CSV files and loads them into
the DuckDB warehouse as raw tables for dbt to transform.
"""

from pathlib import Path

import pandas as pd
from dagster import asset, AssetExecutionContext, Output, MetadataValue, AssetKey

from dagster_project.resources.duckdb_resource import DuckDBResource


# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Table definitions with their CSV source files
TABLES = [
    "customers",
    "vendors", 
    "requests",
    "bids",
    "payments",
]


@asset(
    group_name="load",
    compute_kind="duckdb",
    deps=[AssetKey(["raw_csv_files"])],
    description="Load raw CSV files into DuckDB warehouse tables",
)
def duckdb_raw_tables(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[dict]:
    """
    Load all CSV files into DuckDB as raw tables.
    
    Creates/replaces tables in the 'raw' schema:
    - raw.customers
    - raw.vendors
    - raw.requests
    - raw.bids
    - raw.payments
    """
    table_info = {}
    
    with duckdb.get_connection() as conn:
        # Create raw schema if it doesn't exist
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        context.log.info("Ensured 'raw' schema exists")
        
        for table_name in TABLES:
            csv_path = DATA_DIR / f"{table_name}.csv"
            
            if not csv_path.exists():
                context.log.warning(f"CSV file not found: {csv_path}")
                continue
            
            # Read CSV and load into DuckDB
            context.log.info(f"Loading {table_name} from {csv_path}")
            
            # Use DuckDB's native CSV reader for efficiency
            conn.execute(f"""
                CREATE OR REPLACE TABLE raw.{table_name} AS 
                SELECT * FROM read_csv_auto('{csv_path}')
            """)
            
            # Get row count for metadata
            row_count = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
            
            table_info[table_name] = {
                "schema": "raw",
                "table": table_name,
                "rows": row_count,
                "source_file": str(csv_path),
            }
            
            context.log.info(f"Loaded {row_count} rows into raw.{table_name}")
    
    total_rows = sum(info["rows"] for info in table_info.values())
    
    return Output(
        value=table_info,
        metadata={
            "total_rows": MetadataValue.int(total_rows),
            "tables_loaded": MetadataValue.int(len(table_info)),
            "customers_rows": MetadataValue.int(table_info.get("customers", {}).get("rows", 0)),
            "vendors_rows": MetadataValue.int(table_info.get("vendors", {}).get("rows", 0)),
            "requests_rows": MetadataValue.int(table_info.get("requests", {}).get("rows", 0)),
            "bids_rows": MetadataValue.int(table_info.get("bids", {}).get("rows", 0)),
            "payments_rows": MetadataValue.int(table_info.get("payments", {}).get("rows", 0)),
            "database_path": MetadataValue.path(duckdb.database_path),
        }
    )

