{{
    config(
        materialized='view',
        description='Staged payment data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'payments') }}
),

staged as (
    select
        -- Primary key
        payment_id,
        
        -- Foreign key
        bid_id,
        
        -- Payment details
        cast(amount as decimal(12,2)) as payment_amount,
        payment_method,
        cast(payment_date as date) as payment_date,
        cast(processed_at as timestamp) as processed_at,
        
        -- Derived fields
        case
            when payment_method = 'credit_card' then 'Card'
            when payment_method = 'bank_transfer' then 'Bank'
            when payment_method = 'paypal' then 'Digital'
            when payment_method = 'check' then 'Check'
            else 'Other'
        end as payment_category,
        
        -- Extract date parts for analysis
        extract(year from cast(payment_date as date)) as payment_year,
        extract(month from cast(payment_date as date)) as payment_month,
        extract(dow from cast(payment_date as date)) as payment_day_of_week,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

