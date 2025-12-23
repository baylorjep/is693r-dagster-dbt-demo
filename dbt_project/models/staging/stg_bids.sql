{{
    config(
        materialized='view',
        description='Staged bid data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'bids') }}
),

staged as (
    select
        -- Primary key
        bid_id,
        
        -- Foreign keys
        request_id,
        vendor_id,
        
        -- Bid details
        cast(bid_amount as decimal(12,2)) as bid_amount,
        estimated_hours,
        message,
        status as bid_status,
        
        -- Derived fields
        case
            when bid_amount < 500 then 'Small'
            when bid_amount < 2000 then 'Medium'
            when bid_amount < 5000 then 'Large'
            else 'Enterprise'
        end as bid_size,
        
        -- Calculate implied hourly rate
        case
            when estimated_hours > 0 
            then round(cast(bid_amount as decimal(12,2)) / estimated_hours, 2)
            else null
        end as implied_hourly_rate,
        
        -- Metadata
        cast(created_at as timestamp) as created_at,
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

