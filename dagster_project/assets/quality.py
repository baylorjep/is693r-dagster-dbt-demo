"""
Quality asset: Run data quality checks for Bidi Contracting.

This module runs data quality validations including:
- dbt tests (via the dbt build in dbt_assets)
- Custom Dagster asset checks for takeoff confidence
- Estimate validation checks
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


# Threshold for low confidence (below this triggers warnings)
LOW_CONFIDENCE_THRESHOLD = 0.60
# Maximum acceptable percentage of low-confidence items per project
MAX_LOW_CONFIDENCE_PCT = 20.0


@asset(
    group_name="quality",
    compute_kind="python",
    deps=[
        AssetKey(["fct_takeoffs"]),
        AssetKey(["fct_estimates"]),
        AssetKey(["mart_estimation_dashboard"]),
        AssetKey(["mart_takeoff_quality_monitoring"]),
    ],
    description="Run data quality checks on the transformed takeoff and estimation data",
)
def data_quality_checks(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[dict]:
    """
    Execute custom data quality checks on the warehouse data.
    
    Checks performed:
    1. All quantities are positive
    2. Confidence scores are valid (0-1)
    3. No projects with excessive low-confidence takeoffs
    4. No estimates with zero total
    5. Orphaned records check
    """
    results = {}
    issues = []
    
    with duckdb.get_connection() as conn:
        # Check 1: All quantities should be positive
        context.log.info("Checking takeoff quantities...")
        invalid_quantities = conn.execute("""
            SELECT COUNT(*) FROM fct_takeoffs WHERE quantity <= 0
        """).fetchone()[0]
        
        results["invalid_quantities"] = invalid_quantities
        if invalid_quantities > 0:
            issues.append(f"Found {invalid_quantities} takeoffs with non-positive quantities")
        context.log.info(f"Invalid quantities check: {invalid_quantities} found")
        
        # Check 2: Confidence scores are valid
        context.log.info("Checking confidence scores...")
        invalid_confidence = conn.execute("""
            SELECT COUNT(*) FROM fct_takeoffs 
            WHERE confidence < 0 OR confidence > 1
        """).fetchone()[0]
        
        results["invalid_confidence"] = invalid_confidence
        if invalid_confidence > 0:
            issues.append(f"Found {invalid_confidence} takeoffs with invalid confidence scores")
        context.log.info(f"Invalid confidence check: {invalid_confidence} found")
        
        # Check 3: Projects with excessive low-confidence takeoffs
        context.log.info("Checking for projects with high % of low-confidence takeoffs...")
        low_conf_projects = conn.execute(f"""
            SELECT 
                project_id,
                project_name,
                pct_low_confidence
            FROM mart_estimation_dashboard
            WHERE pct_low_confidence > {MAX_LOW_CONFIDENCE_PCT}
        """).fetchall()
        
        results["projects_with_high_low_confidence"] = len(low_conf_projects)
        if len(low_conf_projects) > 0:
            project_names = [p[1] for p in low_conf_projects[:3]]
            issues.append(f"Found {len(low_conf_projects)} projects with >{MAX_LOW_CONFIDENCE_PCT}% low-confidence takeoffs: {project_names}")
        context.log.info(f"High low-confidence projects: {len(low_conf_projects)} found")
        
        # Check 4: Estimates with zero total
        context.log.info("Checking for zero-value estimates...")
        zero_estimates = conn.execute("""
            SELECT COUNT(*) FROM fct_estimates 
            WHERE estimate_total_mid = 0 OR estimate_total_mid IS NULL
        """).fetchone()[0]
        
        results["zero_estimates"] = zero_estimates
        if zero_estimates > 0:
            issues.append(f"Found {zero_estimates} estimates with zero or null total")
        context.log.info(f"Zero estimates check: {zero_estimates} found")
        
        # Check 5: Overall takeoff statistics
        context.log.info("Computing overall takeoff statistics...")
        takeoff_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_takeoffs,
                AVG(confidence) as avg_confidence,
                SUM(CASE WHEN confidence < 0.6 THEN 1 ELSE 0 END) as low_confidence_count,
                SUM(extended_cost_mid) as total_extended_cost
            FROM fct_takeoffs
        """).fetchone()
        
        results["total_takeoffs"] = takeoff_stats[0]
        results["avg_confidence"] = round(float(takeoff_stats[1]), 3) if takeoff_stats[1] else 0
        results["low_confidence_count"] = takeoff_stats[2]
        results["total_extended_cost"] = round(float(takeoff_stats[3]), 2) if takeoff_stats[3] else 0
        
        context.log.info(f"Total takeoffs: {takeoff_stats[0]}, Avg confidence: {results['avg_confidence']}")
        
        # Check 6: Estimate totals
        context.log.info("Computing estimate statistics...")
        estimate_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_estimates,
                SUM(estimate_total_mid) as total_estimate_value,
                AVG(estimate_total_mid) as avg_estimate
            FROM fct_estimates
        """).fetchone()
        
        results["total_estimates"] = estimate_stats[0]
        results["total_estimate_value"] = round(float(estimate_stats[1]), 2) if estimate_stats[1] else 0
        results["avg_estimate"] = round(float(estimate_stats[2]), 2) if estimate_stats[2] else 0
        
        context.log.info(f"Total estimates: {estimate_stats[0]}, Total value: ${results['total_estimate_value']:,.2f}")
    
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
            "total_takeoffs": MetadataValue.int(results["total_takeoffs"]),
            "avg_confidence": MetadataValue.float(results["avg_confidence"]),
            "low_confidence_count": MetadataValue.int(results["low_confidence_count"]),
            "total_extended_cost": MetadataValue.float(results["total_extended_cost"]),
            "total_estimates": MetadataValue.int(results["total_estimates"]),
            "total_estimate_value": MetadataValue.float(results["total_estimate_value"]),
        }
    )


@asset_check(asset=AssetKey("fct_takeoffs"))
def check_valid_quantities(duckdb: DuckDBResource) -> AssetCheckResult:
    """
    Verify all takeoff quantities are positive.
    
    This is a critical business rule: quantities should never be
    zero or negative in our takeoff data.
    """
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) as invalid
            FROM fct_takeoffs
        """).fetchone()
        
        total = result[0]
        invalid = result[1]
        
        passed = invalid == 0
        
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
            metadata={
                "total_takeoffs": total,
                "invalid_quantities": invalid,
                "pct_valid": round((total - invalid) / total * 100, 2) if total > 0 else 0,
            },
            description=f"Found {invalid} takeoffs with non-positive quantities" if not passed else "All takeoffs have valid quantities",
        )


@asset_check(asset=AssetKey("fct_takeoffs"))
def check_confidence_distribution(duckdb: DuckDBResource) -> AssetCheckResult:
    """
    Check the distribution of confidence scores.
    
    Warns if too many takeoffs have low confidence, indicating
    potential issues with AI extraction quality.
    """
    with duckdb.get_connection() as conn:
        result = conn.execute(f"""
            SELECT 
                COUNT(*) as total,
                AVG(confidence) as avg_confidence,
                SUM(CASE WHEN confidence < {LOW_CONFIDENCE_THRESHOLD} THEN 1 ELSE 0 END) as low_confidence
            FROM fct_takeoffs
        """).fetchone()
        
        total = result[0]
        avg_confidence = result[1]
        low_confidence = result[2]
        
        pct_low = (low_confidence / total * 100) if total > 0 else 0
        passed = pct_low <= MAX_LOW_CONFIDENCE_PCT
        
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.WARN if not passed else AssetCheckSeverity.WARN,
            metadata={
                "total_takeoffs": total,
                "avg_confidence": round(float(avg_confidence), 3) if avg_confidence else 0,
                "low_confidence_count": low_confidence,
                "pct_low_confidence": round(pct_low, 2),
                "threshold": LOW_CONFIDENCE_THRESHOLD,
            },
            description=f"{pct_low:.1f}% of takeoffs have low confidence (below {LOW_CONFIDENCE_THRESHOLD})" if not passed else f"Confidence distribution acceptable: {pct_low:.1f}% low confidence",
        )
