# Video Demo Walkthrough Script

**Total Time:** 8-10 minutes  
**Recording Software:** QuickTime, Loom, or OBS  
**Resolution:** 1920x1080 recommended

---

## Pre-Recording Setup (Do Before You Hit Record)

```bash
cd /Users/baylorjeppsen/Desktop/is693r-dagster-dbt-demo
make clean      # Start fresh
make setup      # Install dependencies
```

Have these ready:
- Terminal window (full screen or large)
- Browser ready (will open later)
- This script visible on a second monitor or printed

---

## PART 1: Introduction (0:00 - 1:00)

### What to Show
- Your terminal with the project folder open

### What to Say
> "Hi, I'm Baylor. This is my IS 693R independent study project demonstrating a modern ELT analytics pipeline using Dagster, dbt, and DuckDB."
>
> "I'll show you how I built a complete data pipeline for Bidi Contracting, an AI platform for construction blueprint takeoffs and cost estimation - covering orchestration, transformations, and data quality - concepts from DP-203 and the Dagster courses."

### Commands to Run
```bash
# Show the project structure
ls -la
```

### What to Point Out
- "Here you can see the main components: dagster_project for orchestration, dbt_project for transformations"
- "This pipeline processes blueprint pages, extracts takeoff quantities using AI, and generates cost estimates"

---

## PART 2: Run the Full Pipeline (1:00 - 3:00)

### What to Say
> "Let me run the complete pipeline with one command."

### Commands to Run
```bash
make demo
```

### While It Runs, Explain
> "Step 1 is extracting data - I'm generating realistic fake data using Python's Faker library. About 5,000 rows across 7 tables: projects, blueprint pages, cost library, takeoff items, estimates, estimate line items, and QA reviews."
>
> "Step 2 loads this into DuckDB, our local data warehouse."
>
> "Step 3 runs dbt transformations - staging models that clean the data, then mart models including fact tables for takeoffs and estimates, plus analytics marts for the dashboard."
>
> "Step 4 runs data quality checks including confidence score validation."
>
> "Step 5 generates this metrics report you see at the bottom."

### What to Point Out When It Finishes
- Total estimate value
- AI extraction percentage (~70%)
- Average confidence score
- "All dbt tests passed"

---

## PART 3: Dagster UI - Asset Graph (3:00 - 5:30)

### What to Say
> "Now let me show you the Dagster UI where you can visualize and manage the pipeline."

### Commands to Run
```bash
make dagster
```

### Wait for It to Start, Then
> "Opening localhost:3000 in my browser..."

Open browser to: **http://localhost:3000**

### Navigation Steps (Do These Slowly for the Camera)

1. **Click "Assets" in the left sidebar**
   > "Here's the asset catalog showing all our data assets."

2. **Click "View global asset lineage" (or the graph icon)**
   > "This is the asset graph - the visual DAG of our pipeline."
   
3. **Point out the flow**
   > "You can see the flow: raw_csv_files generates our blueprint and takeoff data, duckdb_raw_tables loads it into the warehouse, then the dbt models transform it through staging to marts, quality checks validate confidence scores and quantities, and finally the metrics report is generated."

4. **Click on one asset (e.g., "fct_takeoffs")**
   > "Clicking on an asset shows its details - when it was last materialized, metadata like row counts and confidence statistics, and its upstream dependencies."

5. **Click "Materialize all" button (optional - to show it running)**
   > "I can re-run the entire pipeline by clicking Materialize All. Dagster handles the dependency ordering automatically."

6. **Click "Runs" in the left sidebar**
   > "The Runs view shows execution history. I can see all my previous runs, their status, and drill into logs."

### Stop Dagster
Press `Ctrl+C` in terminal

---

## PART 4: dbt Documentation & Lineage (5:30 - 7:00)

### What to Say
> "dbt also generates documentation with its own lineage graph. Let me show you that."

### Commands to Run
```bash
make dbt-docs
```

### Wait for It to Start, Then
Open browser to: **http://localhost:8080**

### Navigation Steps

1. **You'll see the dbt docs homepage**
   > "This is auto-generated documentation for all our dbt models."

2. **Click on a model in the left sidebar (e.g., "fct_takeoffs")**
   > "Each model has documentation showing its columns, description, and the SQL that generates it."

3. **Click the blue "Lineage Graph" button (bottom right corner)**
   > "Here's the data lineage - you can see how fct_takeoffs depends on staging models, which depend on our raw sources."

4. **Click on "mart_estimation_dashboard" or another model to show the graph changing**
   > "The lineage updates to show dependencies for any model I select."

5. **Mention the tests**
   > "dbt also shows which tests are configured for each model - I have 50+ tests including uniqueness, not null, referential integrity, accepted values, and custom tests for quantity and confidence validation."

### Stop dbt docs
Press `Ctrl+C` in terminal

---

## PART 5: Code Walkthrough (7:00 - 8:30)

### What to Say
> "Let me quickly show you the key code components."

### Open These Files (Use VS Code or just `cat` in terminal)

**1. Dagster Asset Definition**
```bash
cat dagster_project/assets/extract.py | head -80
```
> "This is a Dagster asset that generates our fake construction data. You can see the CSI cost codes, disciplines like Architectural and Structural, and the confidence scores for AI extraction."

**2. dbt Model**
```bash
cat dbt_project/models/marts/fct_takeoffs.sql | head -50
```
> "This is a dbt mart model - a fact table joining takeoff items with pages, projects, and the cost library. It calculates extended costs and flags low-confidence extractions."

**3. dbt Tests**
```bash
cat dbt_project/models/sources.yml | head -60
```
> "Tests are defined in YAML. Here I'm testing uniqueness, not null constraints, relationships between tables, and accepted values like extraction method must be ai, manual, or hybrid - all aligned with DP-203 data quality concepts."

**4. Quality Checks**
```bash
cat dagster_project/assets/quality.py | head -50
```
> "I also have Dagster asset checks that validate confidence distributions and flag projects with too many low-confidence takeoffs."

---

## PART 6: Wrap-Up (8:30 - 9:30)

### What to Say
> "To summarize what this project demonstrates:"
>
> "From DP-203: Data ingestion, transformation, warehousing with fact tables and analytics marts, and data quality validation."
>
> "From Dagster Essentials: Software-defined assets, dependency management, and observable data pipelines."
>
> "From Dagster + dbt: Native integration where dbt models become Dagster assets with automatic lineage."
>
> "The Bidi Contracting use case shows a real-world AI estimation workflow: blueprints come in, AI extracts quantities with confidence scores, the system rolls up estimates, and analysts review quality issues."
>
> "This runs entirely locally with DuckDB, but the same patterns would scale to cloud with Azure Synapse or Databricks - blueprint files in Blob Storage, Azure AI for extraction, Synapse for the warehouse."
>
> "The code is on my GitHub - link in the README. Thanks for watching!"

### Final Command (Optional)
```bash
# Query the estimation dashboard
.venv/bin/python -c "
import duckdb
conn = duckdb.connect('warehouse/analytics.duckdb')
print(conn.execute('''
    SELECT project_name, estimate_total_mid, avg_confidence, readiness_status
    FROM main.mart_estimation_dashboard
    ORDER BY estimate_total_mid DESC
    LIMIT 5
''').fetchdf())
"
```

---

## Quick Reference Card

| Timestamp | Section | Key Command |
|-----------|---------|-------------|
| 0:00 | Intro | `ls -la` |
| 1:00 | Run Pipeline | `make demo` |
| 3:00 | Dagster UI | `make dagster` → localhost:3000 |
| 5:30 | dbt Docs | `make dbt-docs` → localhost:8080 |
| 7:00 | Code Tour | `cat` files |
| 8:30 | Wrap-Up | Summary |

---

## Troubleshooting

**If Dagster won't start:**
```bash
make clean
make setup
make dagster
```

**If port 3000 is busy:**
```bash
lsof -i :3000  # Find what's using it
kill -9 <PID>  # Kill it
```

**If dbt fails:**
```bash
make dbt-run   # Run models only
make dbt-test  # Run tests only
```

---

## Post-Recording Checklist

- [ ] Video shows terminal clearly
- [ ] All commands executed successfully
- [ ] Dagster UI asset graph visible
- [ ] dbt lineage graph visible
- [ ] Metrics report shown with estimation metrics
- [ ] Mentioned DP-203, Dagster Essentials, and dbt concepts
- [ ] Explained Bidi Contracting use case (blueprint takeoffs, AI extraction, estimates)
- [ ] GitHub URL mentioned
