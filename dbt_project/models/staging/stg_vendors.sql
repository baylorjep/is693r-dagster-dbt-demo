{{
    config(
        materialized='view',
        description='Staged vendor data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'vendors') }}
),

staged as (
    select
        -- Primary key
        vendor_id,
        
        -- Business details
        business_name,
        contact_name,
        lower(email) as email,
        phone,
        category,
        
        -- Location
        city,
        upper(state) as state,
        
        -- Performance metrics
        cast(rating as decimal(3,2)) as rating,
        cast(hourly_rate as decimal(10,2)) as hourly_rate,
        years_experience,
        
        -- Derived fields
        case
            when rating >= 4.5 then 'Premium'
            when rating >= 4.0 then 'Standard'
            else 'Basic'
        end as vendor_tier,
        
        -- Metadata
        cast(created_at as timestamp) as created_at,
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

