{{
    config(
        materialized='table',
        description='Estimate fact table - grain is one row per estimate version per project'
    )
}}

with estimates as (
    select * from {{ ref('stg_estimates') }}
),

projects as (
    select * from {{ ref('stg_projects') }}
),

-- Aggregate line items per estimate
estimate_details as (
    select
        estimate_id,
        count(*) as line_item_count,
        count(distinct cost_code) as unique_cost_codes,
        sum(extended_cost_mid) as calculated_total
    from {{ ref('stg_estimate_line_items') }}
    group by estimate_id
),

-- Count takeoffs per project
project_takeoffs as (
    select
        project_id,
        count(*) as total_takeoff_items,
        avg(confidence) as avg_confidence,
        sum(case when is_low_confidence then 1 else 0 end) as low_confidence_items
    from {{ ref('stg_takeoff_items') }}
    group by project_id
)

select
    -- Estimate attributes (grain)
    e.estimate_id,
    e.estimate_version,
    e.generated_at,
    e.estimation_method,
    e.estimation_method_display,
    e.estimate_size,
    
    -- Estimate totals
    e.estimate_total_low,
    e.estimate_total_mid,
    e.estimate_total_high,
    e.estimate_range_pct,
    
    -- Estimate details from line items
    coalesce(ed.line_item_count, 0) as line_item_count,
    coalesce(ed.unique_cost_codes, 0) as unique_cost_codes,
    coalesce(ed.calculated_total, 0) as calculated_total,
    
    -- Project context
    e.project_id,
    p.client_name,
    p.project_name,
    p.project_type,
    p.project_type_display,
    p.location_state,
    p.bid_date,
    p.plan_set_version,
    p.project_status,
    
    -- Takeoff context
    coalesce(pt.total_takeoff_items, 0) as total_takeoff_items,
    round(coalesce(pt.avg_confidence, 0), 3) as avg_takeoff_confidence,
    coalesce(pt.low_confidence_items, 0) as low_confidence_items,
    
    -- Calculated metrics
    case
        when pt.total_takeoff_items > 0 
        then round(pt.low_confidence_items::decimal / pt.total_takeoff_items * 100, 2)
        else 0
    end as pct_low_confidence,
    
    -- Estimate accuracy proxy (variance from calculated)
    case
        when ed.calculated_total > 0
        then round((e.estimate_total_mid - ed.calculated_total) / ed.calculated_total * 100, 2)
        else 0
    end as estimate_variance_pct,
    
    -- Flags
    case when e.estimation_method = 'ai' then true else false end as is_ai_estimate,
    case when e.estimate_version = 1 then true else false end as is_initial_estimate,
    case when e.estimate_total_mid = 0 then true else false end as is_zero_estimate,
    
    -- Metadata
    current_timestamp as _updated_at

from estimates e
inner join projects p on e.project_id = p.project_id
left join estimate_details ed on e.estimate_id = ed.estimate_id
left join project_takeoffs pt on e.project_id = pt.project_id

