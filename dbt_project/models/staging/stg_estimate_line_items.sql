{{
    config(
        materialized='view',
        description='Staged estimate line item data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'estimate_line_items') }}
),

staged as (
    select
        -- Primary key
        line_item_id,
        
        -- Foreign keys
        estimate_id,
        cost_code,
        
        -- Line item details
        item_type,
        cast(quantity as decimal(12,2)) as quantity,
        uom,
        cast(unit_cost_mid as decimal(12,2)) as unit_cost_mid,
        cast(extended_cost_mid as decimal(14,2)) as extended_cost_mid,
        
        -- Derived fields
        case
            when extended_cost_mid < 1000 then 'Minor'
            when extended_cost_mid < 10000 then 'Moderate'
            when extended_cost_mid < 50000 then 'Significant'
            else 'Major'
        end as cost_impact,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

