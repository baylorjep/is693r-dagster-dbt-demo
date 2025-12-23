"""
Dagster definitions entry point.

This module defines all assets, resources, jobs, and checks
that make up the Bidi Contracting estimation pipeline.
"""

from dagster import Definitions, load_assets_from_modules

from dagster_project.assets import extract, load, dbt_assets, quality, publish
from dagster_project.assets.dbt_assets import dbt_resource
from dagster_project.assets.quality import check_valid_quantities, check_confidence_distribution
from dagster_project.resources.duckdb_resource import DuckDBResource
from dagster_project.jobs import (
    bidi_contracting_pipeline,
    extract_load_job,
    dbt_transform_job,
    quality_publish_job,
)


# Load all assets from asset modules
all_assets = load_assets_from_modules([extract, load, quality, publish])

# Add dbt assets
all_assets = [*all_assets, dbt_assets.dbt_project_assets]


# Define the complete Dagster deployment
defs = Definitions(
    assets=all_assets,
    asset_checks=[check_valid_quantities, check_confidence_distribution],
    resources={
        "duckdb": DuckDBResource(),
        "dbt": dbt_resource,
    },
    jobs=[
        bidi_contracting_pipeline,
        extract_load_job,
        dbt_transform_job,
        quality_publish_job,
    ],
)
