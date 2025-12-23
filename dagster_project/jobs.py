"""
Dagster jobs for the wedding marketplace pipeline.

Jobs define executable units of work that can be triggered
manually, on a schedule, or via sensors.
"""

from dagster import define_asset_job, AssetSelection


# Main pipeline job - runs all assets in dependency order
wedding_marketplace_pipeline = define_asset_job(
    name="wedding_marketplace_pipeline",
    description="Complete ELT pipeline: extract → load → transform → quality → publish",
    selection=AssetSelection.all(),
)

# Extract and load only (useful for refreshing raw data)
extract_load_job = define_asset_job(
    name="extract_load_job",
    description="Extract raw CSV data and load into DuckDB",
    selection=AssetSelection.groups("extract", "load"),
)

# dbt models only (assumes raw data is already loaded)
dbt_transform_job = define_asset_job(
    name="dbt_transform_job",
    description="Run dbt transformations (staging and marts)",
    selection=AssetSelection.groups("default"),  # dbt assets are in default group
)

# Quality checks and reporting
quality_publish_job = define_asset_job(
    name="quality_publish_job",
    description="Run quality checks and generate metrics report",
    selection=AssetSelection.groups("quality", "publish"),
)

