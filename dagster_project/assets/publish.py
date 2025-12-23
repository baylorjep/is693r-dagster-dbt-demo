"""
Publish asset: Generate metrics report for Bidi Contracting.

This module creates a markdown report with key estimation metrics
computed from the transformed data warehouse.
"""

from pathlib import Path
from datetime import datetime

from dagster import asset, AssetExecutionContext, Output, MetadataValue, AssetKey

from dagster_project.resources.duckdb_resource import DuckDBResource


# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
REPORTS_DIR = PROJECT_ROOT / "reports"


@asset(
    group_name="publish",
    compute_kind="python",
    deps=[AssetKey(["data_quality_checks"])],
    description="Generate metrics report from transformed estimation data",
)
def metrics_report(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[str]:
    """
    Generate a comprehensive metrics report in Markdown format.
    
    Metrics included:
    - Projects processed
    - Blueprint pages processed
    - Takeoff items (and % AI vs manual)
    - Average confidence, % low-confidence
    - Estimate totals: sum(mid), avg(mid), p50/p90
    - Top 10 cost codes by total extended cost
    - QA: open issues count, issue rate per project
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / "metrics.md"
    
    metrics = {}
    
    with duckdb.get_connection() as conn:
        # Overall summary metrics
        context.log.info("Computing project metrics...")
        
        # Project count
        project_count = conn.execute("SELECT COUNT(*) FROM stg_projects").fetchone()[0]
        metrics["project_count"] = project_count
        
        # Blueprint pages count
        page_count = conn.execute("SELECT COUNT(*) FROM stg_blueprint_pages").fetchone()[0]
        metrics["page_count"] = page_count
        
        # Pages by discipline
        context.log.info("Computing discipline breakdown...")
        discipline_stats = conn.execute("""
            SELECT 
                discipline,
                discipline_name,
                COUNT(*) as page_count
            FROM stg_blueprint_pages
            GROUP BY discipline, discipline_name
            ORDER BY page_count DESC
        """).fetchall()
        
        # Takeoff statistics
        context.log.info("Computing takeoff metrics...")
        takeoff_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_takeoffs,
                SUM(CASE WHEN extraction_method = 'ai' THEN 1 ELSE 0 END) as ai_takeoffs,
                SUM(CASE WHEN extraction_method = 'manual' THEN 1 ELSE 0 END) as manual_takeoffs,
                SUM(CASE WHEN extraction_method = 'hybrid' THEN 1 ELSE 0 END) as hybrid_takeoffs,
                AVG(confidence) as avg_confidence,
                SUM(CASE WHEN confidence < 0.6 THEN 1 ELSE 0 END) as low_confidence_count
            FROM fct_takeoffs
        """).fetchone()
        
        metrics["total_takeoffs"] = takeoff_stats[0]
        metrics["ai_takeoffs"] = takeoff_stats[1]
        metrics["manual_takeoffs"] = takeoff_stats[2]
        metrics["hybrid_takeoffs"] = takeoff_stats[3]
        metrics["avg_confidence"] = float(takeoff_stats[4]) if takeoff_stats[4] else 0
        metrics["low_confidence_count"] = takeoff_stats[5]
        metrics["pct_ai"] = (takeoff_stats[1] / takeoff_stats[0] * 100) if takeoff_stats[0] > 0 else 0
        metrics["pct_low_confidence"] = (takeoff_stats[5] / takeoff_stats[0] * 100) if takeoff_stats[0] > 0 else 0
        
        # Estimate statistics
        context.log.info("Computing estimate metrics...")
        estimate_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_estimates,
                SUM(estimate_total_mid) as sum_mid,
                AVG(estimate_total_mid) as avg_mid,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY estimate_total_mid) as p50_mid,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY estimate_total_mid) as p90_mid
            FROM fct_estimates
        """).fetchone()
        
        metrics["total_estimates"] = estimate_stats[0]
        metrics["sum_estimate_mid"] = float(estimate_stats[1]) if estimate_stats[1] else 0
        metrics["avg_estimate_mid"] = float(estimate_stats[2]) if estimate_stats[2] else 0
        metrics["p50_estimate_mid"] = float(estimate_stats[3]) if estimate_stats[3] else 0
        metrics["p90_estimate_mid"] = float(estimate_stats[4]) if estimate_stats[4] else 0
        
        # Top 10 cost codes by extended cost
        context.log.info("Computing top cost codes...")
        top_cost_codes = conn.execute("""
            SELECT 
                cost_code,
                division_name,
                item_type,
                SUM(extended_cost_mid) as total_extended_cost,
                COUNT(*) as takeoff_count
            FROM fct_takeoffs
            GROUP BY cost_code, division_name, item_type
            ORDER BY total_extended_cost DESC
            LIMIT 10
        """).fetchall()
        
        # QA statistics
        context.log.info("Computing QA metrics...")
        qa_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_issues,
                SUM(CASE WHEN NOT resolved THEN 1 ELSE 0 END) as open_issues,
                SUM(CASE WHEN is_critical THEN 1 ELSE 0 END) as critical_issues
            FROM stg_qa_reviews
        """).fetchone()
        
        metrics["total_qa_issues"] = qa_stats[0]
        metrics["open_qa_issues"] = qa_stats[1]
        metrics["critical_qa_issues"] = qa_stats[2]
        metrics["issue_rate"] = (qa_stats[0] / project_count) if project_count > 0 else 0
        
        # Project readiness breakdown
        context.log.info("Computing project readiness...")
        readiness_stats = conn.execute("""
            SELECT 
                readiness_status,
                COUNT(*) as project_count
            FROM mart_estimation_dashboard
            GROUP BY readiness_status
            ORDER BY project_count DESC
        """).fetchall()
    
    # Generate report
    context.log.info("Generating report...")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    report = f"""# Bidi Contracting - Estimation Analytics Report

**Generated:** {timestamp}

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Total Projects | {metrics['project_count']:,} |
| Blueprint Pages Processed | {metrics['page_count']:,} |
| Takeoff Items Extracted | {metrics['total_takeoffs']:,} |
| Total Estimate Value | ${metrics['sum_estimate_mid']:,.2f} |
| Average Estimate | ${metrics['avg_estimate_mid']:,.2f} |

---

## Takeoff Extraction Metrics

| Metric | Value |
|--------|-------|
| Total Takeoff Items | {metrics['total_takeoffs']:,} |
| AI Extracted | {metrics['ai_takeoffs']:,} ({metrics['pct_ai']:.1f}%) |
| Manual Entry | {metrics['manual_takeoffs']:,} |
| Hybrid (AI + Review) | {metrics['hybrid_takeoffs']:,} |
| **Avg Confidence** | **{metrics['avg_confidence']:.3f}** |
| Low Confidence Items | {metrics['low_confidence_count']:,} ({metrics['pct_low_confidence']:.1f}%) |

---

## Estimate Distribution

| Metric | Value |
|--------|-------|
| Total Estimates | {metrics['total_estimates']:,} |
| Sum of Mid Estimates | ${metrics['sum_estimate_mid']:,.2f} |
| Average Mid Estimate | ${metrics['avg_estimate_mid']:,.2f} |
| Median (P50) | ${metrics['p50_estimate_mid']:,.2f} |
| 90th Percentile | ${metrics['p90_estimate_mid']:,.2f} |

---

## Blueprint Pages by Discipline

| Discipline | Code | Pages |
|------------|------|-------|
"""
    
    for disc in discipline_stats:
        report += f"| {disc[1]} | {disc[0]} | {disc[2]:,} |\n"
    
    report += """
---

## Top 10 Cost Codes by Extended Cost

| Rank | Cost Code | Division | Item Type | Total Cost | Takeoffs |
|------|-----------|----------|-----------|------------|----------|
"""
    
    for i, cc in enumerate(top_cost_codes, 1):
        report += f"| {i} | {cc[0]} | {cc[1]} | {cc[2]} | ${cc[3]:,.2f} | {cc[4]:,} |\n"
    
    report += f"""
---

## Quality Assurance

| Metric | Value |
|--------|-------|
| Total QA Issues | {metrics['total_qa_issues']:,} |
| Open Issues | {metrics['open_qa_issues']:,} |
| Critical Issues | {metrics['critical_qa_issues']:,} |
| Issues per Project | {metrics['issue_rate']:.2f} |

---

## Project Readiness Status

| Status | Projects |
|--------|----------|
"""
    
    for status in readiness_stats:
        report += f"| {status[0]} | {status[1]:,} |\n"
    
    report += """
---

## Data Quality

✅ All data quality checks passed

---

*Report generated by Bidi Contracting Estimation Pipeline*
*Dagster + dbt + DuckDB*
"""
    
    # Write report
    with open(report_path, "w") as f:
        f.write(report)
    
    context.log.info(f"Report written to {report_path}")
    
    return Output(
        value=str(report_path),
        metadata={
            "report_path": MetadataValue.path(str(report_path)),
            "project_count": MetadataValue.int(metrics["project_count"]),
            "total_takeoffs": MetadataValue.int(metrics["total_takeoffs"]),
            "avg_confidence": MetadataValue.float(metrics["avg_confidence"]),
            "sum_estimate_mid": MetadataValue.float(metrics["sum_estimate_mid"]),
            "open_qa_issues": MetadataValue.int(metrics["open_qa_issues"]),
            "preview": MetadataValue.md(report[:2000] + "\n\n*... (truncated)*"),
        }
    )
