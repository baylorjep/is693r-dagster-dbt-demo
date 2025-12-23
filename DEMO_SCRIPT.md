# Video Demo Script (~7 Minutes)

**Target Time:** 7 minutes  
**Recording Software:** QuickTime, Loom, or OBS  
**Resolution:** 1920x1080 recommended

---

## Pre-Recording Setup (Do This BEFORE You Hit Record)

```bash
cd /Users/baylorjeppsen/Desktop/is693r-dagster-dbt-demo
make clean
make demo
```

Have these tabs already open:
- Browser Tab 1: bidicontracting.com (homepage only)
- Browser Tab 2: localhost:3000 (Dagster — start with `make dagster`)
- VS Code with project open
- This script on second monitor

---

## PART 1: Hook & Context (45 sec)

### Show: Bidi homepage (DO NOT interact — just show for 10 seconds)

> "This is Bidi Contracting — we automate blueprint takeoffs and cost estimation using AI."
>
> "This is the product context this pipeline supports."
>
> "This project let me formalize work I was already passionate about and ground it in data engineering theory."
>
> **[COURSE TIE-IN]** "Each course shaped a different layer — Dagster for orchestration, dbt for transformations, and DP-203 for the overall ELT design."

**Action:** Switch to VS Code

---

## PART 2: Pipeline Code Walkthrough (2 min)

### File 1: `dagster_project/assets/extract.py`

**Show:** Lines ~1-50 (DATA_MODE toggle), briefly scroll to COST_CODES

> "This is the Extract step. Notice the DATA_MODE toggle — 'demo' for synthetic data, 'live' for production once our AI is integrated."
>
> "The pipeline generates representative project data with industry-standard CSI cost codes."

---

### File 2: `dbt_project/models/staging/stg_takeoff_items.sql`

**Show:** Full file (~58 lines)

> "Here's the Transform step. Staging models clean raw data — casting types, creating confidence tiers, flagging items for human review."
>
> "This is where messy data becomes analysis-ready."
>
> **[COURSE TIE-IN]** "The dbt course stressed separating raw data, staging, and marts. I applied that directly — staging cleans AI output, marts add business logic like readiness thresholds."

---

### File 3: `dbt_project/models/marts/mart_estimation_dashboard.sql`

**Show:** Scroll to final SELECT (~lines 100-140)

> "Mart models add business logic. One row per project with page counts, confidence metrics, and readiness status."
>
> "Analytics-ready data."

---

## PART 3: Dagster UI (1.5 min)

### Browser: localhost:3000 (already running)

**1. Show the global asset lineage**

> "Data flows left to right: raw files → DuckDB → staging → marts → quality checks → report."
>
> "Dagster determines execution order automatically."
>
> **[COURSE TIE-IN]** "The Dagster course emphasized thinking in data assets, not scheduled jobs. That's why this graph is asset-based — blueprint pages, takeoffs, estimates — rather than time-based scripts."

**2. Click on one asset (e.g., `fct_takeoffs`)**

> "Every asset has metadata — when it ran, row counts. This is observability."

**3. (If time) Point to the dbt assets**

> "When Dagster materializes these dbt assets, it runs `dbt build` — which includes all my dbt tests. Uniqueness, not-null, relationships, valid ranges. Data quality is baked into the orchestration."

**Action:** Move to next section (leave Dagster running or Ctrl+C)

---

## PART 4: dbt Docs (1 min)

### Terminal:
```bash
make dbt-docs
```

### Browser: localhost:8080

**1. Click `mart_estimation_dashboard` in sidebar**

> "Every column is documented. Analysts understand the data without asking me."

**2. Click Lineage Graph button**

> "Data lineage — if numbers look wrong, I trace back to source. This is production debugging."

**Action:** Ctrl+C to stop

---

## PART 5: Dashboard (1 min)

### Terminal:
```bash
make dashboard
```

### Browser: localhost:8888

> "Here's the payoff. This dashboard queries transformed data live."

**Point briefly to:**

> "Representative portfolio data — projects, estimates, AI confidence scores."
>
> "Projects needing attention — estimators know where to focus."
>
> "These metrics update automatically when the pipeline runs."

**That's it. Move to close.**

---

## PART 6: Closing (30 sec)

> **[COURSE TIE-IN]** "From DP-203 concepts — ELT design, analytical modeling, and data quality — to real implementation with Dagster, dbt, and DuckDB."
>
> "Even though this runs locally, the architecture mirrors DP-203 patterns exactly. Data Factory for orchestration, Synapse for the warehouse, Power BI for dashboards."
>
> "Thanks for watching."

---

## Quick Reference

| Time | Section | Duration |
|------|---------|----------|
| 0:00 | Hook | 45 sec |
| 0:45 | Code walkthrough | 2 min |
| 2:45 | Dagster UI | 1.5 min |
| 4:15 | dbt Docs | 1 min |
| 5:15 | Dashboard | 1 min |
| 6:15 | Close | 30 sec |
| **6:45** | **Done** | ✅ |

---

## Course → Application Mapping (What You're Showing)

| Course | Taught | You Applied |
|--------|--------|-------------|
| **Dagster Essentials** | Asset-based orchestration | Asset graph + dependency modeling |
| **dbt Fundamentals** | Staging → marts discipline | Clean transforms + business logic |
| **Microsoft DP-203** | ELT + analytics engineering | End-to-end pipeline design |

---

## dbt Tests (They Run Automatically!)

Your pipeline includes these tests (run by Dagster via `dbt build`):

| Test Type | What It Checks | Example |
|-----------|---------------|---------|
| `unique` | No duplicate IDs | `project_id`, `takeoff_id` |
| `not_null` | Required fields exist | `confidence`, `quantity` |
| `relationships` | Foreign keys valid | `takeoffs → projects` |
| `accepted_values` | Valid categories | `discipline in (A,S,C,M,E,P)` |
| Custom tests | Business rules | `quantity > 0`, `confidence between 0 and 1` |

**You don't need to run tests separately** — Dagster runs them as part of materialization.

---

## Troubleshooting

**If Dagster won't start:**
```bash
make clean && make demo && make dagster
```

**If port is busy:**
```bash
lsof -i :3000  # or :8080 or :8888
kill -9 <PID>
```

---

## Pre-Record Checklist

- [ ] `make demo` completed successfully
- [ ] Dagster running at localhost:3000
- [ ] Bidi homepage loads (don't log in)
- [ ] VS Code has 3 files ready to show
- [ ] Script accessible on second monitor

---

## What NOT to Say

❌ ~~"$323 million in estimates"~~ → ✅ "Representative portfolio data"  
❌ ~~"Maybe our AI needs more training"~~ → Just skip  
❌ ~~"Helps with bulk purchasing"~~ → Just skip  
❌ ~~Any interaction with live Bidi site~~ → Homepage only, 10 sec  

---

## The Key Lines That Justify This

**Part 1 (or Close):**
> "This project let me formalize work I was already passionate about and ground it in data engineering theory."

**Part 1:**
> "Each course shaped a different layer — Dagster for orchestration, dbt for transformations, and DP-203 for the overall ELT design."

**Part 2 (after staging model):**
> "The dbt course stressed separating raw data, staging, and marts."

**Part 3 (after asset graph):**
> "The Dagster course emphasized thinking in data assets, not scheduled jobs."

**Closing:**
> "From DP-203 concepts — ELT design, analytical modeling, and data quality — to real implementation."

---

## You're Ready 🎬

- Content: ✅ Excellent  
- Risk: ✅ De-risked  
- Time: ✅ ~7 minutes  
- Academic rigor: ✅ Strong  
- Course attribution: ✅ Airtight
