"""
Load asset: Load CSV data into DuckDB raw tables.

This module takes the generated CSV files and loads them into
the DuckDB warehouse as raw tables for dbt to transform.

Data includes:
- projects: Construction projects requiring estimation
- blueprint_pages: Individual pages from plan sets
- cost_library: Reference costs for materials and labor
- takeoff_items: Extracted quantities from blueprints
- estimates: Rolled-up cost estimates per project
- estimate_line_items: Detailed breakdown by cost code
- qa_reviews: Quality assurance reviews
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
    "projects",
    "blueprint_pages",
    "cost_library",
    "takeoff_items",
    "estimates",
    "estimate_line_items",
    "qa_reviews",
]


@asset(
    group_name="load",
    compute_kind="duckdb",
    deps=[AssetKey(["raw_csv_files"])],
    description="Load raw CSV files into DuckDB warehouse tables for blueprint takeoff data",
)
def duckdb_raw_tables(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[dict]:
    """
    Load all CSV files into DuckDB as raw tables.
    
    Creates/replaces tables in the 'raw' schema:
    - raw.projects
    - raw.blueprint_pages
    - raw.cost_library
    - raw.takeoff_items
    - raw.estimates
    - raw.estimate_line_items
    - raw.qa_reviews
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
            "projects_rows": MetadataValue.int(table_info.get("projects", {}).get("rows", 0)),
            "blueprint_pages_rows": MetadataValue.int(table_info.get("blueprint_pages", {}).get("rows", 0)),
            "cost_library_rows": MetadataValue.int(table_info.get("cost_library", {}).get("rows", 0)),
            "takeoff_items_rows": MetadataValue.int(table_info.get("takeoff_items", {}).get("rows", 0)),
            "estimates_rows": MetadataValue.int(table_info.get("estimates", {}).get("rows", 0)),
            "estimate_line_items_rows": MetadataValue.int(table_info.get("estimate_line_items", {}).get("rows", 0)),
            "qa_reviews_rows": MetadataValue.int(table_info.get("qa_reviews", {}).get("rows", 0)),
            "database_path": MetadataValue.path(duckdb.database_path),
        }
    )
