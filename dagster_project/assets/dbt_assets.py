"""
dbt assets: Define dbt models as Dagster assets.

This module uses the dagster-dbt integration to automatically
create Dagster assets from dbt models, enabling native orchestration.
"""

import os
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject


# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"

# Set environment variables for dbt
os.environ["DBT_PROFILES_DIR"] = str(DBT_PROJECT_DIR)
os.environ["DBT_DUCKDB_PATH"] = str(PROJECT_ROOT / "warehouse" / "analytics.duckdb")

# Initialize dbt project
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)

# Prepare dbt manifest (parse project) - this generates manifest.json
dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    All dbt models as Dagster assets.
    
    This includes:
    - Staging models (stg_customers, stg_vendors, stg_requests, stg_bids, stg_payments)
    - Mart models (dim_customers, dim_vendors, fct_bids, fct_payments)
    
    The dagster-dbt integration automatically:
    - Creates an asset for each dbt model
    - Infers dependencies from dbt refs
    - Runs dbt build with proper ordering
    
    Note: The dbt sources (raw.customers, etc.) depend on the duckdb_raw_tables asset
    which must be materialized first to populate the raw schema.
    """
    yield from dbt.cli(["build"], context=context).stream()


# Create dbt resource for use in definitions
dbt_resource = DbtCliResource(
    project_dir=dbt_project,
    profiles_dir=DBT_PROJECT_DIR,
)
