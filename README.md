# Bidi Contracting - Blueprint Takeoff & Estimation Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Dagster](https://img.shields.io/badge/dagster-1.6+-purple.svg)](https://dagster.io/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-orange.svg)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/duckdb-0.9+-yellow.svg)](https://duckdb.org/)

A modern ELT analytics pipeline demonstrating AI-powered blueprint takeoffs and cost estimation for construction projects.

Built with applied learning from:
- **Microsoft DP-203** (Data Engineering on Azure concepts)
- **Dagster Essentials** (Orchestration)
- **Dagster + dbt** (Orchestration + Transformations)

This project is a portfolio artifact for the BYU MISM IS 693R Independent Study.

---

## About Bidi Contracting

Bidi Contracting is an AI automation platform for construction blueprint takeoffs and cost estimation. The platform:

1. **Ingests plan sets** (blueprint pages) for construction projects
2. **Performs AI-powered takeoffs** - extracting quantities from blueprints with confidence scores
3. **Generates cost estimates** - rolling up takeoffs against a cost library (low/mid/high ranges)
4. **Enables QA review** - flagging low-confidence extractions and cost outliers
5. **Produces analytics** - monitoring estimation accuracy, confidence distributions, and cost drivers

This is an **internal product workflow** (not a marketplace) - the pipeline supports Bidi's core estimation product.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   Bidi Contracting Estimation Pipeline                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────────────────┐    │
│   │   EXTRACT    │     │     LOAD     │     │        TRANSFORM         │    │
│   │              │     │              │     │                          │    │
│   │  CSV Files   │────▶│   DuckDB     │────▶│   dbt Models            │    │
│   │  (Faker)     │     │   Raw Tables │     │   • Staging (views)     │    │
│   │              │     │              │     │   • Marts (tables)      │    │
│   └──────────────┘     └──────────────┘     └──────────────────────────┘    │
│          │                    │                         │                    │
│          │                    │                         │                    │
│          ▼                    ▼                         ▼                    │
│   ┌──────────────────────────────────────────────────────────────────┐      │
│   │                        DAGSTER ORCHESTRATION                      │      │
│   │                                                                   │      │
│   │   raw_csv_files ──▶ duckdb_raw_tables ──▶ dbt_models ──▶         │      │
│   │                                            data_quality_checks ──▶│      │
│   │                                            metrics_report         │      │
│   └──────────────────────────────────────────────────────────────────┘      │
│                                    │                                         │
│                                    ▼                                         │
│   ┌──────────────────────────────────────────────────────────────────┐      │
│   │                           OUTPUTS                                 │      │
│   │   • warehouse/analytics.duckdb (queryable data warehouse)        │      │
│   │   • reports/metrics.md (estimation analytics)                    │      │
│   │   • Dagster UI (asset graph + materialization history)           │      │
│   │   • dbt Docs (data lineage + documentation)                      │      │
│   └──────────────────────────────────────────────────────────────────┘      │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Model

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│   stg_projects  │       │stg_blueprint_   │       │ stg_cost_library│
│   ─────────────│        │    pages        │       │   ─────────────│
│   project_id   │◄───────│   ─────────────│        │   cost_code    │
│   client_name  │        │   page_id      │        │   unit_cost_mid│
│   project_type │        │   discipline   │        │   item_type    │
└────────┬────────┘       └────────┬────────┘       └────────┬────────┘
         │                         │                          │
         │                         ▼                          │
         │              ┌─────────────────────────────────────┼──────────┐
         │              │            stg_takeoff_items        │          │
         │              │   ─────────────────────────────────│          │
         └─────────────▶│   takeoff_id | page_id | cost_code │◄─────────┘
                        │   quantity | confidence | method   │
                        └────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              MART MODELS                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   fct_takeoffs          fct_estimates         mart_estimation_dashboard     │
│   ─────────────         ─────────────         ─────────────────────────     │
│   Grain: takeoff        Grain: estimate       Grain: project                │
│   + project/page        + project context     + headline metrics            │
│   + cost context        + takeoff stats       + readiness status            │
│                                                                              │
│   mart_estimate_cost_breakdown       mart_takeoff_quality_monitoring        │
│   ────────────────────────────       ──────────────────────────────         │
│   Grain: estimate + cost_code        Grain: project + discipline            │
│   Cost driver analysis               Confidence & QA metrics                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites

- Python 3.11 or higher
- macOS, Linux, or WSL2

### Setup & Run

```bash
# Clone or navigate to the project
cd is693r-dagster-dbt-demo

# Option 1: Full demo (setup + run pipeline)
make demo

# Option 2: Step by step
make setup          # Create venv and install dependencies
make dagster        # Launch Dagster UI at http://localhost:3000
```

### Available Commands

| Command | Description |
|---------|-------------|
| `make setup` | Create virtual environment and install dependencies |
| `make demo` | Run complete pipeline (extract → load → transform → publish) |
| `make dagster` | Launch Dagster UI at http://localhost:3000 |
| `make dbt-docs` | Generate and serve dbt documentation at http://localhost:8080 |
| `make dbt-run` | Run dbt models only |
| `make dbt-test` | Run dbt tests only |
| `make clean` | Remove all generated files |

---

## Demo Script (5-10 minutes)

Use this script for screen recording your demo:

### Minute 0-1: Introduction & Setup

```bash
# Show the project structure
ls -la
cat README.md | head -50

# Run the complete pipeline
make demo
```

**What to show:** Terminal output showing each step completing successfully.

### Minute 1-4: Dagster UI Exploration

```bash
make dagster
# Opens http://localhost:3000
```

**What to show in Dagster UI:**
1. **Asset Graph** (Global Asset Lineage) - Show the complete DAG of assets
2. Click on `raw_csv_files` → Show metadata (row counts, data types)
3. Click on `dbt_project_assets` → Show dbt model dependencies
4. **Materialize All** → Watch the pipeline execute
5. **Run History** → Show successful run with timing

### Minute 4-6: dbt Documentation

```bash
# In a new terminal
make dbt-docs
# Opens http://localhost:8080
```

**What to show in dbt Docs:**
1. **Lineage Graph** → Visualize source → staging → marts flow
2. Click on `fct_takeoffs` → Show column documentation
3. Click on `mart_estimation_dashboard` → Show tests and descriptions
4. Show the `cost_library_snapshot` → Explain SCD Type 2 for pricing history

### Minute 6-8: Explore Outputs

```bash
# View the metrics report
cat reports/metrics.md

# Query DuckDB directly
.venv/bin/python -c "
import duckdb
conn = duckdb.connect('warehouse/analytics.duckdb')
print('\\n=== Top Cost Codes by Extended Cost ===')
print(conn.execute('''
    SELECT cost_code, division_name, SUM(extended_cost_mid) as total_cost
    FROM main.fct_takeoffs 
    GROUP BY cost_code, division_name
    ORDER BY total_cost DESC 
    LIMIT 5
''').fetchdf())
print('\\n=== Project Readiness Summary ===')
print(conn.execute('''
    SELECT readiness_status, COUNT(*) as projects, 
           ROUND(AVG(avg_confidence), 3) as avg_confidence
    FROM main.mart_estimation_dashboard
    GROUP BY readiness_status
    ORDER BY projects DESC
''').fetchdf())
"
```

### Minute 8-10: Code Walkthrough

**Files to highlight:**

1. **`dagster_project/assets/extract.py`** - Show data generation with CSI cost codes
2. **`dagster_project/assets/dbt_assets.py`** - Show dagster-dbt integration
3. **`dbt_project/models/marts/fct_takeoffs.sql`** - Show takeoff fact table design
4. **`dbt_project/models/sources.yml`** - Show dbt tests configuration
5. **`dbt_project/snapshots/cost_library_snapshot.sql`** - Show SCD Type 2

---

## Project Structure

```
is693r-dagster-dbt-demo/
├── README.md                    # This file
├── pyproject.toml               # Python dependencies
├── Makefile                     # CLI commands
├── data/
│   └── raw/                     # Generated CSVs (gitignored)
│       ├── projects.csv         # ~50 rows
│       ├── blueprint_pages.csv  # ~400 rows
│       ├── cost_library.csv     # ~80 rows
│       ├── takeoff_items.csv    # ~4000 rows
│       ├── estimates.csv        # ~80 rows
│       ├── estimate_line_items.csv # ~600 rows
│       └── qa_reviews.csv       # ~100 rows
├── warehouse/
│   └── analytics.duckdb         # DuckDB warehouse (gitignored)
├── reports/
│   └── metrics.md               # Auto-generated metrics report
├── dagster_project/
│   ├── __init__.py
│   ├── defs.py                  # Dagster definitions entry point
│   ├── jobs.py                  # Job definitions
│   ├── assets/
│   │   ├── extract.py           # CSV generation asset
│   │   ├── load.py              # DuckDB loading asset
│   │   ├── dbt_assets.py        # dbt model assets
│   │   ├── quality.py           # Data quality checks
│   │   └── publish.py           # Report generation
│   └── resources/
│       └── duckdb_resource.py   # DuckDB connection resource
└── dbt_project/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── models/
    │   ├── sources.yml          # Source definitions + tests
    │   ├── staging/             # 7 staging models (views)
    │   └── marts/               # 5 mart models (tables)
    ├── tests/
    │   ├── assert_positive_quantities.sql
    │   └── assert_valid_confidence.sql
    └── snapshots/
        └── cost_library_snapshot.sql # SCD Type 2
```

---

## Key Takeaways

### Orchestration Concepts (Dagster Essentials)

| Concept | Implementation |
|---------|---------------|
| **Software-Defined Assets** | Each pipeline step is an asset with typed inputs/outputs |
| **Asset Materialization** | Dagster tracks when assets were last computed |
| **Resources** | DuckDB connection shared across assets via `DuckDBResource` |
| **Asset Checks** | Custom validation (`check_valid_quantities`, `check_confidence_distribution`) |
| **Metadata** | Rich metadata attached to each materialization |

### ELT + Modeling (Dagster + dbt)

| Concept | Implementation |
|---------|---------------|
| **dagster-dbt Integration** | dbt models automatically become Dagster assets |
| **Staging Layer** | Clean, typed views over raw data |
| **Fact Tables** | `fct_takeoffs`, `fct_estimates` at appropriate grains |
| **Analytics Marts** | `mart_estimation_dashboard`, `mart_takeoff_quality_monitoring` |
| **dbt Tests** | 50+ tests (unique, not_null, relationships, accepted_values, custom) |
| **dbt Snapshots** | SCD Type 2 for cost library pricing history |

### Data Quality

| Concept | Implementation |
|---------|---------------|
| **Schema Tests** | dbt tests on sources and models |
| **Custom Tests** | `assert_positive_quantities.sql`, `assert_valid_confidence.sql` |
| **Asset Checks** | Dagster-native checks for quantities and confidence distribution |
| **Quality Gates** | Quality checks run before report generation |
| **Confidence Monitoring** | Track % of low-confidence AI extractions |

### DP-203 Aligned Concepts

| DP-203 Topic | Local Implementation | Cloud Scaling |
|--------------|---------------------|---------------|
| **Data Ingestion** | CSV files → DuckDB | Azure Data Factory, Event Hubs |
| **Data Transformation** | dbt models | Azure Synapse, Databricks |
| **Data Warehousing** | DuckDB star schema | Azure Synapse Dedicated Pools |
| **Orchestration** | Dagster | Azure Data Factory, Dagster Cloud |
| **Data Quality** | dbt tests, Dagster checks | Azure Purview, Great Expectations |
| **SCD Type 2** | dbt snapshots | Delta Lake merge operations |
| **Partitioning** | Not implemented locally | Date-based partitions in Synapse |

#### How This Would Translate to Azure

This local demo uses DuckDB for simplicity, but the patterns translate directly to Azure:

```python
# Local (this demo)
duckdb.connect('warehouse/analytics.duckdb')

# Azure Synapse (production)
# Replace DuckDB resource with Synapse connector
# Same dbt models work with dbt-synapse adapter

# Partitioning example (would add to dbt models)
{{ config(
    materialized='incremental',
    partition_by={'field': 'bid_date', 'data_type': 'date'},
    cluster_by=['project_type', 'location_state']
) }}
```

**Azure architecture for Bidi Contracting at scale:**
- **Azure Blob Storage** - Store blueprint PDFs and extracted images
- **Azure AI Document Intelligence** - Replace mock AI extraction with real OCR/ML
- **Azure Data Factory** - Orchestrate data movement at scale
- **Azure Synapse Analytics** - Data warehouse with dedicated SQL pools
- **Azure Purview** - Data governance and lineage
- **Power BI** - Executive dashboards for estimation metrics

---

## Sample Metrics Output

After running `make demo`, check `reports/metrics.md`:

```markdown
# Bidi Contracting - Estimation Analytics Report

## Executive Summary
| Metric | Value |
|--------|-------|
| Total Projects | 50 |
| Blueprint Pages Processed | 400 |
| Takeoff Items Extracted | 4,000 |
| Total Estimate Value | $X,XXX,XXX.XX |

## Takeoff Extraction Metrics
| Metric | Value |
|--------|-------|
| AI Extracted | 2,800 (70%) |
| Avg Confidence | 0.82 |
| Low Confidence Items | 320 (8%) |
```

---

## Development

### Adding New Models

1. Create SQL file in `dbt_project/models/staging/` or `marts/`
2. Add tests in corresponding `_staging.yml` or `_marts.yml`
3. Run `make dbt-run` to test locally
4. Dagster will automatically pick up new dbt models

### Modifying Data Generation

Edit `dagster_project/assets/extract.py` to:
- Change row counts
- Add new tables
- Modify data distributions
- Add new CSI cost codes

### Running Tests

```bash
# dbt tests
make dbt-test

# View test results in Dagster UI
make dagster
# Navigate to Assets → dbt tests
```

---

## Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Microsoft DP-203 Study Guide](https://learn.microsoft.com/en-us/certifications/exams/dp-203)
- [CSI MasterFormat](https://www.csiresources.org/standards/masterformat) - Construction cost code standard

---

## License

MIT License - feel free to use this as a template for your own projects.

---

*Built for IS 693R Independent Study at BYU Marriott School of Business*
