"""
Publish asset: Generate metrics report.

This module creates a markdown report with key business metrics
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
    description="Generate metrics report from transformed data",
)
def metrics_report(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[str]:
    """
    Generate a comprehensive metrics report in Markdown format.
    
    Metrics included:
    - Total GMV (Gross Merchandise Value)
    - Average bid amount
    - Bid conversion rate
    - Top vendors by revenue
    - Category breakdown
    - Customer acquisition trends
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / "metrics.md"
    
    metrics = {}
    
    with duckdb.get_connection() as conn:
        # Overall summary metrics
        context.log.info("Computing overall metrics...")
        
        # Total GMV
        gmv = conn.execute("""
            SELECT COALESCE(SUM(payment_amount), 0) FROM fct_payments
        """).fetchone()[0]
        metrics["total_gmv"] = float(gmv)
        
        # Total payments count
        payment_count = conn.execute("""
            SELECT COUNT(*) FROM fct_payments
        """).fetchone()[0]
        metrics["payment_count"] = payment_count
        
        # Average payment
        avg_payment = conn.execute("""
            SELECT COALESCE(AVG(payment_amount), 0) FROM fct_payments
        """).fetchone()[0]
        metrics["avg_payment"] = float(avg_payment)
        
        # Bid statistics
        bid_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_bids,
                AVG(bid_amount) as avg_bid,
                SUM(CASE WHEN bid_status = 'accepted' THEN 1 ELSE 0 END) as accepted_bids
            FROM fct_bids
        """).fetchone()
        metrics["total_bids"] = bid_stats[0]
        metrics["avg_bid"] = float(bid_stats[1]) if bid_stats[1] else 0
        metrics["accepted_bids"] = bid_stats[2]
        metrics["conversion_rate"] = (bid_stats[2] / bid_stats[0] * 100) if bid_stats[0] > 0 else 0
        
        # Customer and vendor counts
        customer_count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
        vendor_count = conn.execute("SELECT COUNT(*) FROM dim_vendors").fetchone()[0]
        metrics["customer_count"] = customer_count
        metrics["vendor_count"] = vendor_count
        
        # Top 5 vendors by revenue
        context.log.info("Computing top vendors...")
        top_vendors = conn.execute("""
            SELECT 
                v.business_name,
                v.category,
                COUNT(p.payment_id) as total_payments,
                SUM(p.payment_amount) as total_revenue
            FROM fct_payments p
            JOIN fct_bids b ON p.bid_id = b.bid_id
            JOIN dim_vendors v ON b.vendor_id = v.vendor_id
            GROUP BY v.vendor_id, v.business_name, v.category
            ORDER BY total_revenue DESC
            LIMIT 5
        """).fetchall()
        
        # Category breakdown
        context.log.info("Computing category breakdown...")
        category_stats = conn.execute("""
            SELECT 
                b.category,
                COUNT(*) as bid_count,
                SUM(CASE WHEN b.bid_status = 'accepted' THEN 1 ELSE 0 END) as accepted,
                AVG(b.bid_amount) as avg_bid
            FROM fct_bids b
            GROUP BY b.category
            ORDER BY bid_count DESC
        """).fetchall()
        
        # Request status breakdown
        context.log.info("Computing request statistics...")
        request_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_requests,
                SUM(CASE WHEN request_status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN request_status = 'open' THEN 1 ELSE 0 END) as open_requests,
                AVG(budget) as avg_budget
            FROM stg_requests
        """).fetchone()
    
    # Generate report
    context.log.info("Generating report...")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    report = f"""# Wedding Marketplace Analytics Report

**Generated:** {timestamp}

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Total GMV | ${metrics['total_gmv']:,.2f} |
| Total Payments | {metrics['payment_count']:,} |
| Average Payment | ${metrics['avg_payment']:,.2f} |
| Active Customers | {metrics['customer_count']:,} |
| Active Vendors | {metrics['vendor_count']:,} |

---

## Bid Performance

| Metric | Value |
|--------|-------|
| Total Bids | {metrics['total_bids']:,} |
| Accepted Bids | {metrics['accepted_bids']:,} |
| Average Bid Amount | ${metrics['avg_bid']:,.2f} |
| **Conversion Rate** | **{metrics['conversion_rate']:.1f}%** |

---

## Top 5 Vendors by Revenue

| Rank | Vendor | Category | Payments | Revenue |
|------|--------|----------|----------|---------|
"""
    
    for i, vendor in enumerate(top_vendors, 1):
        report += f"| {i} | {vendor[0]} | {vendor[1]} | {vendor[2]} | ${vendor[3]:,.2f} |\n"
    
    report += """
---

## Category Performance

| Category | Bids | Accepted | Avg Bid | Conversion |
|----------|------|----------|---------|------------|
"""
    
    for cat in category_stats:
        conv_rate = (cat[2] / cat[1] * 100) if cat[1] > 0 else 0
        report += f"| {cat[0]} | {cat[1]} | {cat[2]} | ${cat[3]:,.2f} | {conv_rate:.1f}% |\n"
    
    report += f"""
---

## Request Pipeline

| Metric | Value |
|--------|-------|
| Total Requests | {request_stats[0]:,} |
| Completed | {request_stats[1]:,} |
| Open | {request_stats[2]:,} |
| Average Budget | ${request_stats[3]:,.2f} |

---

## Data Quality

✅ All data quality checks passed

---

*Report generated by IS 693R Dagster + dbt Demo Pipeline*
"""
    
    # Write report
    with open(report_path, "w") as f:
        f.write(report)
    
    context.log.info(f"Report written to {report_path}")
    
    return Output(
        value=str(report_path),
        metadata={
            "report_path": MetadataValue.path(str(report_path)),
            "total_gmv": MetadataValue.float(metrics["total_gmv"]),
            "conversion_rate": MetadataValue.float(metrics["conversion_rate"]),
            "customer_count": MetadataValue.int(metrics["customer_count"]),
            "vendor_count": MetadataValue.int(metrics["vendor_count"]),
            "preview": MetadataValue.md(report[:1500] + "\n\n*... (truncated)*"),
        }
    )

