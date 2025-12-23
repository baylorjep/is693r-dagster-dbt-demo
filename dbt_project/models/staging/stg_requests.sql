{{
    config(
        materialized='view',
        description='Staged service request data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'requests') }}
),

staged as (
    select
        -- Primary key
        request_id,
        
        -- Foreign key
        customer_id,
        
        -- Request details
        category,
        cast(event_date as date) as event_date,
        event_city,
        upper(event_state) as event_state,
        cast(budget as decimal(12,2)) as budget,
        guest_count,
        status as request_status,
        description,
        
        -- Derived fields
        case
            when budget < 1000 then 'Budget'
            when budget < 5000 then 'Mid-Range'
            when budget < 10000 then 'Premium'
            else 'Luxury'
        end as budget_tier,
        
        case
            when guest_count < 50 then 'Intimate'
            when guest_count < 150 then 'Medium'
            when guest_count < 250 then 'Large'
            else 'Grand'
        end as event_size,
        
        -- Metadata
        cast(created_at as timestamp) as created_at,
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

