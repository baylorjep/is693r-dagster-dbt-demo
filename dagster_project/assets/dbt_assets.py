"""
dbt assets: Define dbt models as Dagster assets.

This module uses the dagster-dbt integration to automatically
create Dagster assets from dbt models, enabling native orchestration.
"""

import os
import subprocess
import sys
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject


# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

# Set environment variables for dbt
os.environ["DBT_PROFILES_DIR"] = str(DBT_PROJECT_DIR)
os.environ["DBT_DUCKDB_PATH"] = str(PROJECT_ROOT / "warehouse" / "analytics.duckdb")

# Ensure the venv's bin is first in PATH so dbt uses the right Python
VENV_BIN = PROJECT_ROOT / ".venv" / "bin"
os.environ["PATH"] = f"{VENV_BIN}:{os.environ.get('PATH', '')}"

# Generate manifest if it doesn't exist, using the venv's dbt explicitly
if not MANIFEST_PATH.exists():
    dbt_executable = VENV_BIN / "dbt"
    result = subprocess.run(
        [str(dbt_executable), "parse", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        print(f"dbt parse failed: {result.stderr}")
        raise RuntimeError(f"dbt parse failed: {result.stderr}")

# Initialize dbt project with pre-existing manifest
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)


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
