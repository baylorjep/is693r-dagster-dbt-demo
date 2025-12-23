{{
    config(
        materialized='table',
        description='Quality monitoring metrics by project and discipline'
    )
}}

with takeoffs as (
    select * from {{ ref('stg_takeoff_items') }}
),

pages as (
    select * from {{ ref('stg_blueprint_pages') }}
),

projects as (
    select * from {{ ref('stg_projects') }}
),

qa_reviews as (
    select * from {{ ref('stg_qa_reviews') }}
),

-- Aggregate takeoff metrics by project and discipline
takeoff_metrics as (
    select
        t.project_id,
        p.discipline,
        p.discipline_name,
        count(*) as total_takeoffs,
        sum(case when t.extraction_method = 'ai' then 1 else 0 end) as ai_takeoffs,
        sum(case when t.extraction_method = 'manual' then 1 else 0 end) as manual_takeoffs,
        sum(case when t.extraction_method = 'hybrid' then 1 else 0 end) as hybrid_takeoffs,
        avg(t.confidence) as avg_confidence,
        min(t.confidence) as min_confidence,
        max(t.confidence) as max_confidence,
        sum(case when t.is_low_confidence then 1 else 0 end) as low_confidence_count,
        sum(t.quantity) as total_quantity
    from takeoffs t
    inner join pages p on t.page_id = p.page_id
    group by t.project_id, p.discipline, p.discipline_name
),

-- Aggregate QA metrics by project
qa_metrics as (
    select
        project_id,
        count(*) as total_issues,
        sum(case when not resolved then 1 else 0 end) as open_issues,
        sum(case when resolved then 1 else 0 end) as resolved_issues,
        sum(case when is_critical then 1 else 0 end) as critical_issues,
        avg(severity_score) as avg_severity
    from qa_reviews
    group by project_id
)

select
    -- Grain: project + discipline
    tm.project_id,
    tm.discipline,
    tm.discipline_name,
    
    -- Project context
    pr.client_name,
    pr.project_name,
    pr.project_type,
    pr.location_state,
    pr.project_status,
    
    -- Takeoff volume metrics
    tm.total_takeoffs,
    tm.ai_takeoffs,
    tm.manual_takeoffs,
    tm.hybrid_takeoffs,
    round(tm.total_quantity, 2) as total_quantity,
    
    -- Extraction method percentages
    round(tm.ai_takeoffs::decimal / nullif(tm.total_takeoffs, 0) * 100, 1) as pct_ai_extracted,
    round(tm.manual_takeoffs::decimal / nullif(tm.total_takeoffs, 0) * 100, 1) as pct_manual,
    
    -- Confidence metrics
    round(tm.avg_confidence, 3) as avg_confidence,
    round(tm.min_confidence, 3) as min_confidence,
    round(tm.max_confidence, 3) as max_confidence,
    tm.low_confidence_count,
    round(tm.low_confidence_count::decimal / nullif(tm.total_takeoffs, 0) * 100, 1) as pct_low_confidence,
    
    -- QA metrics (project level)
    coalesce(qa.total_issues, 0) as total_qa_issues,
    coalesce(qa.open_issues, 0) as open_qa_issues,
    coalesce(qa.resolved_issues, 0) as resolved_qa_issues,
    coalesce(qa.critical_issues, 0) as critical_qa_issues,
    round(coalesce(qa.avg_severity, 0), 2) as avg_issue_severity,
    
    -- Issue rate (issues per 100 takeoffs at project level)
    round(coalesce(qa.total_issues, 0)::decimal / nullif(tm.total_takeoffs, 0) * 100, 2) as issue_rate_per_100,
    
    -- Quality flags
    case
        when tm.avg_confidence >= 0.85 and tm.low_confidence_count = 0 then 'Excellent'
        when tm.avg_confidence >= 0.75 then 'Good'
        when tm.avg_confidence >= 0.65 then 'Acceptable'
        else 'Needs Review'
    end as quality_tier,
    
    case
        when coalesce(qa.open_issues, 0) > 5 or coalesce(qa.critical_issues, 0) > 0 then true
        else false
    end as requires_attention,
    
    -- Metadata
    current_timestamp as _updated_at

from takeoff_metrics tm
inner join projects pr on tm.project_id = pr.project_id
left join qa_metrics qa on tm.project_id = qa.project_id

