# IS 693R Dagster + dbt Demo: Wedding Marketplace Analytics Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Dagster](https://img.shields.io/badge/dagster-1.6+-purple.svg)](https://dagster.io/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-orange.svg)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/duckdb-0.9+-yellow.svg)](https://duckdb.org/)

A modern ELT analytics pipeline demonstrating applied learning from:
- **Microsoft DP-203** (Data Engineering on Azure concepts)
- **Dagster Essentials** (Orchestration)
- **Dagster + dbt** (Orchestration + Transformations)

This project is a portfolio artifact for the BYU MISM IS 693R Independent Study.

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Wedding Marketplace ELT Pipeline                      │
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
│   │   • reports/metrics.md (executive summary)                       │      │
│   │   • Dagster UI (asset graph + materialization history)           │      │
│   │   • dbt Docs (data lineage + documentation)                      │      │
│   └──────────────────────────────────────────────────────────────────┘      │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Model

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│   dim_customers │       │   dim_vendors   │       │ vendors_snapshot│
│   ─────────────│        │   ─────────────│        │   (SCD Type 2)  │
│   customer_id  │        │   vendor_id    │        │   ─────────────│
│   full_name    │        │   business_name│        │   vendor_id    │
│   lifetime_value│       │   total_revenue│        │   rating       │
│   customer_segment│     │   win_rate_pct │        │   valid_from   │
└────────┬────────┘       └────────┬────────┘       │   valid_to     │
         │                         │                 └─────────────────┘
         │                         │
         ▼                         ▼
┌─────────────────────────────────────────────────────┐
│                     fct_bids                         │
│   ─────────────────────────────────────────────────│
│   bid_id | request_id | vendor_id | customer_id    │
│   bid_amount | bid_status | category | event_date  │
└────────────────────────┬────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│                   fct_payments                       │
│   ─────────────────────────────────────────────────│
│   payment_id | bid_id | payment_amount              │
│   vendor_name | customer_name | service_category    │
└─────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

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

## 🎬 Demo Script (5-10 minutes)

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
2. Click on `fct_payments` → Show column documentation
3. Click on `dim_customers` → Show tests and descriptions
4. Show the `vendors_snapshot` → Explain SCD Type 2

### Minute 6-8: Explore Outputs

```bash
# View the metrics report
cat reports/metrics.md

# Query DuckDB directly
.venv/bin/python -c "
import duckdb
conn = duckdb.connect('warehouse/analytics.duckdb')
print('\\n=== Top 5 Vendors by Revenue ===')
print(conn.execute('''
    SELECT vendor_name, total_revenue 
    FROM main.dim_vendors 
    ORDER BY total_revenue DESC 
    LIMIT 5
''').fetchdf())
print('\\n=== Bid Conversion by Category ===')
print(conn.execute('''
    SELECT category, 
           COUNT(*) as total_bids,
           SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) as accepted,
           ROUND(AVG(CASE WHEN is_accepted THEN 1.0 ELSE 0.0 END) * 100, 1) as conversion_pct
    FROM main.fct_bids
    GROUP BY category
    ORDER BY conversion_pct DESC
''').fetchdf())
"
```

### Minute 8-10: Code Walkthrough

**Files to highlight:**

1. **`dagster_project/assets/extract.py`** - Show data generation with Faker
2. **`dagster_project/assets/dbt_assets.py`** - Show dagster-dbt integration
3. **`dbt_project/models/marts/fct_payments.sql`** - Show star schema design
4. **`dbt_project/models/sources.yml`** - Show dbt tests configuration
5. **`dbt_project/snapshots/vendors_snapshot.sql`** - Show SCD Type 2

---

## 📊 Project Structure

```
is693r-dagster-dbt-demo/
├── README.md                    # This file
├── pyproject.toml               # Python dependencies
├── Makefile                     # CLI commands
├── data/
│   └── raw/                     # Generated CSVs (gitignored)
│       ├── customers.csv        # ~300 rows
│       ├── vendors.csv          # ~100 rows
│       ├── requests.csv         # ~500 rows
│       ├── bids.csv             # ~400 rows
│       └── payments.csv         # ~200 rows
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
    │   ├── staging/             # 5 staging models (views)
    │   └── marts/               # 4 mart models (tables)
    ├── tests/
    │   └── assert_positive_bid_amounts.sql
    └── snapshots/
        └── vendors_snapshot.sql # SCD Type 2
```

---

## 🎓 Key Takeaways

### Orchestration Concepts (Dagster Essentials)

| Concept | Implementation |
|---------|---------------|
| **Software-Defined Assets** | Each pipeline step is an asset with typed inputs/outputs |
| **Asset Materialization** | Dagster tracks when assets were last computed |
| **Resources** | DuckDB connection shared across assets via `DuckDBResource` |
| **Asset Checks** | Custom validation (`check_positive_payment_amounts`) |
| **Metadata** | Rich metadata attached to each materialization |

### ELT + Modeling (Dagster + dbt)

| Concept | Implementation |
|---------|---------------|
| **dagster-dbt Integration** | dbt models automatically become Dagster assets |
| **Staging Layer** | Clean, typed views over raw data |
| **Dimensional Modeling** | Star schema with `dim_` and `fct_` prefixes |
| **dbt Tests** | 20+ tests (unique, not_null, relationships, accepted_values) |
| **dbt Snapshots** | SCD Type 2 for vendor profile history |

### Data Quality

| Concept | Implementation |
|---------|---------------|
| **Schema Tests** | dbt tests on sources and models |
| **Custom Tests** | `assert_positive_bid_amounts.sql` |
| **Asset Checks** | Dagster-native `@asset_check` for payment validation |
| **Quality Gates** | Quality checks run before report generation |

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

#### Scaling Considerations

This local demo uses DuckDB for simplicity, but the patterns translate directly to cloud:

```python
# Local (this demo)
duckdb.connect('warehouse/analytics.duckdb')

# Azure Synapse (production)
# Replace DuckDB resource with Synapse connector
# Same dbt models work with dbt-synapse adapter

# Partitioning example (would add to dbt models)
{{ config(
    materialized='incremental',
    partition_by={'field': 'payment_date', 'data_type': 'date'},
    cluster_by=['category']
) }}
```

---

## 📈 Sample Metrics Output

After running `make demo`, check `reports/metrics.md`:

```markdown
# Wedding Marketplace Analytics Report

## Executive Summary
| Metric | Value |
|--------|-------|
| Total GMV | $XXX,XXX.XX |
| Total Payments | XXX |
| Average Payment | $X,XXX.XX |
| Active Customers | 300 |
| Active Vendors | 100 |

## Bid Performance
| Metric | Value |
|--------|-------|
| Total Bids | 400 |
| Conversion Rate | ~40% |
```

---

## 🛠️ Development

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

### Running Tests

```bash
# dbt tests
make dbt-test

# View test results in Dagster UI
make dagster
# Navigate to Assets → dbt tests
```

---

## 📚 Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Microsoft DP-203 Study Guide](https://learn.microsoft.com/en-us/certifications/exams/dp-203)

---

## 📝 License

MIT License - feel free to use this as a template for your own projects.

---

*Built for IS 693R Independent Study at BYU Marriott School of Business*

