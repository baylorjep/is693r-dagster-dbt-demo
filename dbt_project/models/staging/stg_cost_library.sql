{{
    config(
        materialized='view',
        description='Staged cost library data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'cost_library') }}
),

staged as (
    select
        -- Primary key
        cost_code,
        
        -- CSI classification
        division,
        division_name,
        item_type,
        uom,
        
        -- Cost data
        cast(unit_cost_low as decimal(12,2)) as unit_cost_low,
        cast(unit_cost_mid as decimal(12,2)) as unit_cost_mid,
        cast(unit_cost_high as decimal(12,2)) as unit_cost_high,
        
        -- Derived fields
        round((unit_cost_high - unit_cost_low) / nullif(unit_cost_mid, 0) * 100, 2) as cost_variance_pct,
        
        case
            when unit_cost_mid < 50 then 'Low Cost'
            when unit_cost_mid < 200 then 'Medium Cost'
            else 'High Cost'
        end as cost_tier,
        
        -- Metadata
        cast(last_updated as date) as last_updated,
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

