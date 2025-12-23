"""Dagster resources for the Bidi Contracting estimation pipeline."""

from dagster_project.resources.duckdb_resource import DuckDBResource, duckdb_resource

__all__ = ["DuckDBResource", "duckdb_resource"]
