"""
DuckDB resource for the wedding marketplace pipeline.

This resource provides a connection to the local DuckDB warehouse
that is shared across assets for data loading and querying.
"""

from pathlib import Path
from contextlib import contextmanager
from typing import Generator

import duckdb
from dagster import ConfigurableResource, InitResourceContext


# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
DEFAULT_DB_PATH = WAREHOUSE_DIR / "analytics.duckdb"


class DuckDBResource(ConfigurableResource):
    """
    A Dagster resource for managing DuckDB connections.
    
    This resource provides a consistent way to connect to the local
    DuckDB warehouse file across all assets in the pipeline.
    """
    
    database_path: str = str(DEFAULT_DB_PATH)
    
    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Ensure the warehouse directory exists."""
        db_path = Path(self.database_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
    
    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get a connection to the DuckDB database.
        
        Usage:
            with duckdb_resource.get_connection() as conn:
                result = conn.execute("SELECT * FROM customers").fetchall()
        """
        conn = duckdb.connect(self.database_path)
        try:
            yield conn
        finally:
            conn.close()
    
    def execute(self, query: str, params: tuple = None) -> list:
        """Execute a query and return results."""
        with self.get_connection() as conn:
            if params:
                return conn.execute(query, params).fetchall()
            return conn.execute(query).fetchall()
    
    def execute_df(self, query: str):
        """Execute a query and return results as a DataFrame."""
        with self.get_connection() as conn:
            return conn.execute(query).fetchdf()
    
    def table_exists(self, table_name: str, schema: str = "main") -> bool:
        """Check if a table exists in the database."""
        query = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = ? AND table_name = ?
        """
        result = self.execute(query, (schema, table_name))
        return result[0][0] > 0
    
    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        with self.get_connection() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            return result[0] if result else 0


# Create a default instance for use in Dagster definitions
duckdb_resource = DuckDBResource(database_path=str(DEFAULT_DB_PATH))

