{{
    config(
        materialized='view',
        description='Staged estimate data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'estimates') }}
),

staged as (
    select
        -- Primary key
        estimate_id,
        
        -- Foreign key
        project_id,
        
        -- Estimate details
        estimate_version,
        cast(estimate_total_low as decimal(14,2)) as estimate_total_low,
        cast(estimate_total_mid as decimal(14,2)) as estimate_total_mid,
        cast(estimate_total_high as decimal(14,2)) as estimate_total_high,
        estimation_method,
        
        -- Derived fields
        round((estimate_total_high - estimate_total_low) / nullif(estimate_total_mid, 0) * 100, 2) as estimate_range_pct,
        
        case
            when estimate_total_mid < 100000 then 'Small'
            when estimate_total_mid < 500000 then 'Medium'
            when estimate_total_mid < 2000000 then 'Large'
            else 'Enterprise'
        end as estimate_size,
        
        case
            when estimation_method = 'ai' then 'AI Generated'
            when estimation_method = 'manual' then 'Manual Estimate'
            when estimation_method = 'hybrid' then 'AI + Manual Review'
            else 'Unknown'
        end as estimation_method_display,
        
        -- Metadata
        cast(generated_at as timestamp) as generated_at,
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

