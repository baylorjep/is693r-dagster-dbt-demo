{{
    config(
        materialized='table',
        description='Bid fact table with denormalized context'
    )
}}

with bids as (
    select * from {{ ref('stg_bids') }}
),

requests as (
    select * from {{ ref('stg_requests') }}
),

vendors as (
    select * from {{ ref('stg_vendors') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    -- Bid attributes (grain)
    b.bid_id,
    b.created_at as bid_created_at,
    b.bid_amount,
    b.estimated_hours,
    b.bid_status,
    b.bid_size,
    b.implied_hourly_rate,
    b.message as bid_message,
    
    -- Request context
    b.request_id,
    r.category,
    r.event_date,
    r.event_city,
    r.event_state,
    r.budget as request_budget,
    r.guest_count,
    r.request_status,
    r.budget_tier,
    r.event_size,
    r.created_at as request_created_at,
    
    -- Vendor context
    b.vendor_id,
    v.business_name as vendor_name,
    v.vendor_tier,
    v.rating as vendor_rating,
    v.hourly_rate as vendor_hourly_rate,
    v.city as vendor_city,
    v.state as vendor_state,
    
    -- Customer context
    r.customer_id,
    c.full_name as customer_name,
    c.city as customer_city,
    c.state as customer_state,
    
    -- Calculated metrics
    case
        when r.budget > 0 
        then round(b.bid_amount / r.budget * 100, 2)
        else null
    end as bid_to_budget_pct,
    
    case
        when b.implied_hourly_rate is not null and v.hourly_rate > 0
        then round(b.implied_hourly_rate / v.hourly_rate * 100, 2)
        else null
    end as rate_variance_pct,
    
    -- Time metrics
    datediff('day', r.created_at, b.created_at) as days_to_bid,
    datediff('day', b.created_at, r.event_date) as days_until_event,
    
    -- Flags
    case when b.bid_status = 'accepted' then true else false end as is_accepted,
    case when v.state = r.event_state then true else false end as is_local_vendor,
    case 
        when b.bid_amount <= r.budget then true 
        else false 
    end as is_within_budget,
    
    -- Metadata
    current_timestamp as _updated_at

from bids b
inner join requests r on b.request_id = r.request_id
inner join vendors v on b.vendor_id = v.vendor_id
inner join customers c on r.customer_id = c.customer_id

