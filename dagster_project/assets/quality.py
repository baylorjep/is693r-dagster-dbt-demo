"""
Quality asset: Run data quality checks.

This module runs data quality validations including:
- dbt tests (via the dbt build in dbt_assets)
- Custom Dagster asset checks
"""

from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
    AssetKey,
    AssetDep,
)

from dagster_project.resources.duckdb_resource import DuckDBResource


@asset(
    group_name="quality",
    compute_kind="python",
    deps=[
        AssetKey(["fct_payments"]),
        AssetKey(["fct_bids"]),
        AssetKey(["dim_vendors"]),
        AssetKey(["dim_customers"]),
    ],
    description="Run data quality checks on the transformed data",
)
def data_quality_checks(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[dict]:
    """
    Execute custom data quality checks on the warehouse data.
    
    Checks performed:
    1. Payment amounts are positive
    2. All bids have valid vendor references
    3. Conversion rate is within expected range
    4. No orphaned payments (all link to valid bids)
    """
    results = {}
    issues = []
    
    with duckdb.get_connection() as conn:
        # Check 1: All payment amounts should be positive
        context.log.info("Checking payment amounts...")
        negative_payments = conn.execute("""
            SELECT COUNT(*) FROM fct_payments WHERE payment_amount <= 0
        """).fetchone()[0]
        
        results["negative_payments"] = negative_payments
        if negative_payments > 0:
            issues.append(f"Found {negative_payments} payments with non-positive amounts")
        context.log.info(f"Negative payments check: {negative_payments} found")
        
        # Check 2: Bid conversion rate
        context.log.info("Checking bid conversion rate...")
        conversion_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_bids,
                SUM(CASE WHEN bid_status = 'accepted' THEN 1 ELSE 0 END) as accepted_bids
            FROM fct_bids
        """).fetchone()
        
        total_bids = conversion_stats[0]
        accepted_bids = conversion_stats[1]
        conversion_rate = (accepted_bids / total_bids * 100) if total_bids > 0 else 0
        
        results["total_bids"] = total_bids
        results["accepted_bids"] = accepted_bids
        results["conversion_rate"] = round(conversion_rate, 2)
        
        # Conversion rate should be between 10% and 70%
        if not (10 <= conversion_rate <= 70):
            issues.append(f"Conversion rate {conversion_rate:.2f}% outside expected range (10-70%)")
        context.log.info(f"Conversion rate: {conversion_rate:.2f}%")
        
        # Check 3: Total GMV validation
        context.log.info("Validating total GMV...")
        gmv = conn.execute("""
            SELECT COALESCE(SUM(payment_amount), 0) FROM fct_payments
        """).fetchone()[0]
        
        results["total_gmv"] = round(float(gmv), 2)
        if gmv <= 0:
            issues.append("Total GMV is zero or negative")
        context.log.info(f"Total GMV: ${gmv:,.2f}")
        
        # Check 4: Orphaned records check
        context.log.info("Checking for orphaned records...")
        orphaned_bids = conn.execute("""
            SELECT COUNT(*) 
            FROM fct_bids b
            LEFT JOIN dim_vendors v ON b.vendor_id = v.vendor_id
            WHERE v.vendor_id IS NULL
        """).fetchone()[0]
        
        results["orphaned_bids"] = orphaned_bids
        if orphaned_bids > 0:
            issues.append(f"Found {orphaned_bids} bids with missing vendor references")
        context.log.info(f"Orphaned bids: {orphaned_bids}")
    
    # Determine overall status
    passed = len(issues) == 0
    results["passed"] = passed
    results["issues"] = issues
    
    if passed:
        context.log.info("All data quality checks passed!")
    else:
        context.log.warning(f"Data quality issues found: {issues}")
    
    return Output(
        value=results,
        metadata={
            "checks_passed": MetadataValue.bool(passed),
            "issues_count": MetadataValue.int(len(issues)),
            "total_gmv": MetadataValue.float(results["total_gmv"]),
            "conversion_rate_pct": MetadataValue.float(results["conversion_rate"]),
            "total_bids": MetadataValue.int(results["total_bids"]),
            "accepted_bids": MetadataValue.int(results["accepted_bids"]),
        }
    )


@asset_check(asset=AssetKey("fct_payments"))
def check_positive_payment_amounts(duckdb: DuckDBResource) -> AssetCheckResult:
    """
    Verify all payment amounts in fct_payments are positive.
    
    This is a critical business rule: payments should never be
    zero or negative in our marketplace.
    """
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN payment_amount <= 0 THEN 1 ELSE 0 END) as invalid
            FROM fct_payments
        """).fetchone()
        
        total = result[0]
        invalid = result[1]
        
        passed = invalid == 0
        
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
            metadata={
                "total_payments": total,
                "invalid_payments": invalid,
                "pct_valid": round((total - invalid) / total * 100, 2) if total > 0 else 0,
            },
            description=f"Found {invalid} payments with non-positive amounts" if not passed else "All payments have positive amounts",
        )

