{{
    config(
        materialized='table',
        description='One row per project with headline metrics for estimation dashboard'
    )
}}

with projects as (
    select * from {{ ref('stg_projects') }}
),

pages as (
    select
        project_id,
        count(*) as page_count,
        count(distinct discipline) as discipline_count
    from {{ ref('stg_blueprint_pages') }}
    group by project_id
),

takeoffs as (
    select
        project_id,
        count(*) as takeoff_count,
        sum(case when extraction_method = 'ai' then 1 else 0 end) as ai_takeoff_count,
        sum(case when extraction_method = 'manual' then 1 else 0 end) as manual_takeoff_count,
        avg(confidence) as avg_confidence,
        sum(case when is_low_confidence then 1 else 0 end) as low_confidence_count
    from {{ ref('stg_takeoff_items') }}
    group by project_id
),

-- Get the latest estimate for each project
latest_estimates as (
    select
        project_id,
        estimate_id,
        estimate_version,
        estimate_total_low,
        estimate_total_mid,
        estimate_total_high,
        estimation_method,
        generated_at,
        row_number() over (partition by project_id order by estimate_version desc) as rn
    from {{ ref('stg_estimates') }}
),

estimates as (
    select * from latest_estimates where rn = 1
),

qa_reviews as (
    select
        project_id,
        count(*) as total_issues,
        sum(case when not resolved then 1 else 0 end) as open_issues,
        sum(case when is_critical then 1 else 0 end) as critical_issues
    from {{ ref('stg_qa_reviews') }}
    group by project_id
)

select
    -- Project identification
    p.project_id,
    p.client_name,
    p.project_name,
    p.project_type,
    p.project_type_display,
    p.location_state,
    p.bid_date,
    p.plan_set_version,
    p.project_status,
    p.created_at as project_created_at,
    
    -- Blueprint page metrics
    coalesce(pg.page_count, 0) as page_count,
    coalesce(pg.discipline_count, 0) as discipline_count,
    
    -- Takeoff metrics
    coalesce(t.takeoff_count, 0) as takeoff_count,
    coalesce(t.ai_takeoff_count, 0) as ai_takeoff_count,
    coalesce(t.manual_takeoff_count, 0) as manual_takeoff_count,
    round(coalesce(t.ai_takeoff_count, 0)::decimal / nullif(t.takeoff_count, 0) * 100, 1) as pct_ai_extracted,
    
    -- Confidence metrics
    round(coalesce(t.avg_confidence, 0), 3) as avg_confidence,
    coalesce(t.low_confidence_count, 0) as low_confidence_count,
    round(coalesce(t.low_confidence_count, 0)::decimal / nullif(t.takeoff_count, 0) * 100, 1) as pct_low_confidence,
    
    -- Latest estimate metrics
    e.estimate_id as latest_estimate_id,
    coalesce(e.estimate_version, 0) as estimate_version,
    coalesce(e.estimate_total_low, 0) as estimate_total_low,
    coalesce(e.estimate_total_mid, 0) as estimate_total_mid,
    coalesce(e.estimate_total_high, 0) as estimate_total_high,
    e.estimation_method,
    e.generated_at as estimate_generated_at,
    
    -- Estimate range
    coalesce(e.estimate_total_high, 0) - coalesce(e.estimate_total_low, 0) as estimate_range,
    case
        when e.estimate_total_mid > 0
        then round((e.estimate_total_high - e.estimate_total_low) / e.estimate_total_mid * 100, 1)
        else 0
    end as estimate_range_pct,
    
    -- QA metrics
    coalesce(qa.total_issues, 0) as total_qa_issues,
    coalesce(qa.open_issues, 0) as open_qa_issues,
    coalesce(qa.critical_issues, 0) as critical_qa_issues,
    
    -- Derived status indicators
    case
        when e.estimate_total_mid is null or e.estimate_total_mid = 0 then 'No Estimate'
        when coalesce(qa.open_issues, 0) > 0 or coalesce(t.low_confidence_count, 0) > 5 then 'Needs Review'
        when coalesce(t.avg_confidence, 0) >= 0.80 then 'Ready'
        else 'In Progress'
    end as readiness_status,
    
    case
        when coalesce(t.avg_confidence, 0) >= 0.85 then 'High'
        when coalesce(t.avg_confidence, 0) >= 0.70 then 'Medium'
        else 'Low'
    end as confidence_level,
    
    -- Completeness score (0-100)
    round(
        (case when pg.page_count > 0 then 25 else 0 end +
         case when t.takeoff_count > 0 then 25 else 0 end +
         case when e.estimate_total_mid > 0 then 25 else 0 end +
         case when coalesce(t.avg_confidence, 0) >= 0.70 then 25 else 
              (coalesce(t.avg_confidence, 0) / 0.70 * 25) end)
    , 0) as completeness_score,
    
    -- Metadata
    current_timestamp as _updated_at

from projects p
left join pages pg on p.project_id = pg.project_id
left join takeoffs t on p.project_id = t.project_id
left join estimates e on p.project_id = e.project_id
left join qa_reviews qa on p.project_id = qa.project_id

