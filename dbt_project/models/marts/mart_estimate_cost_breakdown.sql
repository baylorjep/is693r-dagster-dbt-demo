{{
    config(
        materialized='table',
        description='Cost breakdown by cost code and item type per project estimate'
    )
}}

with line_items as (
    select * from {{ ref('stg_estimate_line_items') }}
),

estimates as (
    select * from {{ ref('stg_estimates') }}
),

projects as (
    select * from {{ ref('stg_projects') }}
),

cost_library as (
    select * from {{ ref('stg_cost_library') }}
)

select
    -- Grain: estimate + cost_code
    li.estimate_id,
    li.cost_code,
    
    -- Cost classification
    c.division,
    c.division_name,
    li.item_type,
    li.uom,
    
    -- Quantities and costs
    li.quantity,
    li.unit_cost_mid,
    li.extended_cost_mid,
    li.cost_impact,
    
    -- Calculate low/high based on library
    round(li.quantity * c.unit_cost_low, 2) as extended_cost_low,
    round(li.quantity * c.unit_cost_high, 2) as extended_cost_high,
    
    -- Estimate context
    e.estimate_version,
    e.estimation_method,
    e.estimate_total_mid,
    
    -- Cost as percentage of total estimate
    case
        when e.estimate_total_mid > 0
        then round(li.extended_cost_mid / e.estimate_total_mid * 100, 2)
        else 0
    end as pct_of_estimate,
    
    -- Project context
    e.project_id,
    p.client_name,
    p.project_name,
    p.project_type,
    p.location_state,
    
    -- Metadata
    current_timestamp as _updated_at

from line_items li
inner join estimates e on li.estimate_id = e.estimate_id
inner join projects p on e.project_id = p.project_id
inner join cost_library c on li.cost_code = c.cost_code

