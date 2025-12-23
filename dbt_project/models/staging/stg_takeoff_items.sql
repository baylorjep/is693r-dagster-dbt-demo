{{
    config(
        materialized='view',
        description='Staged takeoff item data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'takeoff_items') }}
),

staged as (
    select
        -- Primary key
        takeoff_id,
        
        -- Foreign keys
        project_id,
        page_id,
        cost_code,
        
        -- Takeoff details
        item_type,
        cast(quantity as decimal(12,2)) as quantity,
        uom,
        cast(confidence as decimal(5,3)) as confidence,
        extraction_method,
        
        -- Derived fields
        case
            when confidence >= 0.90 then 'High'
            when confidence >= 0.75 then 'Medium'
            when confidence >= 0.60 then 'Low'
            else 'Very Low'
        end as confidence_tier,
        
        case
            when confidence < 0.60 then true
            else false
        end as is_low_confidence,
        
        case
            when extraction_method = 'ai' then 'AI Extracted'
            when extraction_method = 'manual' then 'Manual Entry'
            when extraction_method = 'hybrid' then 'AI + Manual Review'
            else 'Unknown'
        end as extraction_method_display,
        
        -- Metadata
        cast(created_at as timestamp) as created_at,
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

