{{
    config(
        materialized='view',
        description='Staged customer data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'customers') }}
),

staged as (
    select
        -- Primary key
        customer_id,
        
        -- Customer details
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        lower(email) as email,
        phone,
        
        -- Location
        city,
        upper(state) as state,
        
        -- Metadata
        cast(created_at as timestamp) as created_at,
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

