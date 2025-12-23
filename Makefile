.PHONY: setup install clean demo dagster dbt-docs dbt-run dbt-test help

# Colors for terminal output
GREEN := \033[0;32m
YELLOW := \033[0;33m
CYAN := \033[0;36m
NC := \033[0m # No Color

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
DBT := $(VENV)/bin/dbt
DAGSTER := $(VENV)/bin/dagster

help:
	@echo "$(CYAN)═══════════════════════════════════════════════════════════════$(NC)"
	@echo "$(CYAN)    IS 693R Dagster + dbt Demo - Wedding Marketplace Pipeline  $(NC)"
	@echo "$(CYAN)═══════════════════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(GREEN)Available commands:$(NC)"
	@echo "  $(YELLOW)make setup$(NC)      - Create virtual environment and install dependencies"
	@echo "  $(YELLOW)make demo$(NC)       - Run the complete pipeline (extract → load → transform → publish)"
	@echo "  $(YELLOW)make dagster$(NC)    - Launch Dagster UI at http://localhost:3000"
	@echo "  $(YELLOW)make dbt-docs$(NC)   - Generate and serve dbt documentation"
	@echo "  $(YELLOW)make dbt-run$(NC)    - Run dbt models only"
	@echo "  $(YELLOW)make dbt-test$(NC)   - Run dbt tests only"
	@echo "  $(YELLOW)make clean$(NC)      - Remove generated files and virtual environment"
	@echo ""

setup: $(VENV)/bin/activate
	@echo "$(GREEN)✓ Setup complete!$(NC)"

$(VENV)/bin/activate:
	@echo "$(CYAN)Creating virtual environment...$(NC)"
	python3 -m venv $(VENV)
	@echo "$(CYAN)Installing dependencies...$(NC)"
	$(PIP) install --upgrade pip
	$(PIP) install -e .
	@echo "$(CYAN)Installing dbt packages...$(NC)"
	cd dbt_project && $(DBT) deps || true
	@touch $(VENV)/bin/activate

install: setup

demo: setup
	@echo ""
	@echo "$(CYAN)═══════════════════════════════════════════════════════════════$(NC)"
	@echo "$(CYAN)    Running Complete ELT Pipeline Demo                          $(NC)"
	@echo "$(CYAN)═══════════════════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(YELLOW)Step 1: Extracting raw data (generating CSVs)...$(NC)"
	$(DAGSTER) asset materialize --select 'raw_csv_files' -m dagster_project.defs 2>&1 | grep -E "(Materialized|INFO|ERROR)" || true
	@echo ""
	@echo "$(YELLOW)Step 2: Loading data into DuckDB...$(NC)"
	$(DAGSTER) asset materialize --select 'duckdb_raw_tables' -m dagster_project.defs 2>&1 | grep -E "(Materialized|INFO|ERROR)" || true
	@echo ""
	@echo "$(YELLOW)Step 3: Running dbt transformations...$(NC)"
	DBT_DUCKDB_PATH=warehouse/analytics.duckdb $(DBT) build --quiet --project-dir dbt_project --profiles-dir dbt_project
	@echo ""
	@echo "$(YELLOW)Step 4: Running data quality checks...$(NC)"
	$(DAGSTER) asset materialize --select 'data_quality_checks' -m dagster_project.defs 2>&1 | grep -E "(Materialized|INFO|ERROR)" || true
	@echo ""
	@echo "$(YELLOW)Step 5: Generating metrics report...$(NC)"
	$(DAGSTER) asset materialize --select 'metrics_report' -m dagster_project.defs 2>&1 | grep -E "(Materialized|INFO|ERROR)" || true
	@echo ""
	@echo "$(GREEN)✓ Pipeline complete!$(NC)"
	@echo ""
	@echo "$(CYAN)═══════════════════════════════════════════════════════════════$(NC)"
	@echo "$(CYAN)    Pipeline Outputs                                            $(NC)"
	@echo "$(CYAN)═══════════════════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(YELLOW)Generated data files:$(NC)"
	@ls -la data/raw/*.csv 2>/dev/null || echo "  (no CSV files yet)"
	@echo ""
	@echo "$(YELLOW)DuckDB warehouse:$(NC)"
	@ls -la warehouse/*.duckdb 2>/dev/null || echo "  (no warehouse file yet)"
	@echo ""
	@echo "$(YELLOW)Metrics report:$(NC)"
	@cat reports/metrics.md 2>/dev/null || echo "  (no report yet)"
	@echo ""
	@echo "$(GREEN)Demo complete! Next steps:$(NC)"
	@echo "  - Run $(YELLOW)make dagster$(NC) to explore the asset graph in the UI"
	@echo "  - Run $(YELLOW)make dbt-docs$(NC) to view dbt lineage documentation"
	@echo ""

dagster: setup
	@echo "$(CYAN)Starting Dagster UI at http://localhost:3000 ...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	$(DAGSTER) dev -m dagster_project.defs -p 3000

dbt-docs: setup
	@echo "$(CYAN)Generating dbt documentation...$(NC)"
	DBT_DUCKDB_PATH=warehouse/analytics.duckdb $(DBT) docs generate --project-dir dbt_project --profiles-dir dbt_project
	@echo "$(CYAN)Serving dbt docs at http://localhost:8080 ...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	DBT_DUCKDB_PATH=warehouse/analytics.duckdb $(DBT) docs serve --port 8080 --project-dir dbt_project --profiles-dir dbt_project

dbt-run: setup
	@echo "$(CYAN)Running dbt models...$(NC)"
	DBT_DUCKDB_PATH=warehouse/analytics.duckdb $(DBT) run --project-dir dbt_project --profiles-dir dbt_project

dbt-test: setup
	@echo "$(CYAN)Running dbt tests...$(NC)"
	DBT_DUCKDB_PATH=warehouse/analytics.duckdb $(DBT) test --project-dir dbt_project --profiles-dir dbt_project

clean:
	@echo "$(YELLOW)Cleaning up generated files...$(NC)"
	rm -rf $(VENV)
	rm -rf data/raw/*.csv
	rm -rf warehouse/*.duckdb warehouse/*.duckdb.wal
	rm -rf reports/metrics.md
	rm -rf dbt_project/target dbt_project/logs dbt_project/dbt_packages
	rm -rf __pycache__ dagster_project/__pycache__ dagster_project/assets/__pycache__ dagster_project/resources/__pycache__
	rm -rf .dagster storage schedules logs
	@echo "$(GREEN)✓ Cleanup complete!$(NC)"

