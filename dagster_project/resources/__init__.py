"""Dagster resources for the wedding marketplace pipeline."""

from dagster_project.resources.duckdb_resource import DuckDBResource, duckdb_resource

__all__ = ["DuckDBResource", "duckdb_resource"]

