{{
    config(
        materialized='table',
        description='Takeoff fact table - grain is one row per extracted takeoff item'
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

cost_library as (
    select * from {{ ref('stg_cost_library') }}
)

select
    -- Takeoff attributes (grain)
    t.takeoff_id,
    t.created_at as takeoff_created_at,
    t.item_type,
    t.quantity,
    t.uom,
    t.confidence,
    t.confidence_tier,
    t.is_low_confidence,
    t.extraction_method,
    t.extraction_method_display,
    
    -- Cost context
    t.cost_code,
    c.division,
    c.division_name,
    c.unit_cost_low,
    c.unit_cost_mid,
    c.unit_cost_high,
    c.cost_tier,
    
    -- Extended costs (calculated)
    round(t.quantity * c.unit_cost_low, 2) as extended_cost_low,
    round(t.quantity * c.unit_cost_mid, 2) as extended_cost_mid,
    round(t.quantity * c.unit_cost_high, 2) as extended_cost_high,
    
    -- Page context
    t.page_id,
    p.sheet_number,
    p.discipline,
    p.discipline_name,
    p.page_title,
    p.page_version,
    
    -- Project context
    t.project_id,
    pr.client_name,
    pr.project_name,
    pr.project_type,
    pr.project_type_display,
    pr.location_state,
    pr.bid_date,
    pr.plan_set_version,
    pr.project_status,
    
    -- Calculated metrics
    case
        when c.unit_cost_mid > 0 
        then round(t.quantity * c.unit_cost_mid, 2)
        else 0
    end as line_total,
    
    -- Flags
    case when t.extraction_method = 'ai' then true else false end as is_ai_extracted,
    case when t.extraction_method = 'manual' then true else false end as is_manual_entry,
    
    -- Metadata
    current_timestamp as _updated_at

from takeoffs t
inner join pages p on t.page_id = p.page_id
inner join projects pr on t.project_id = pr.project_id
inner join cost_library c on t.cost_code = c.cost_code

