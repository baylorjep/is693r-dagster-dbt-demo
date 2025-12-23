{{
    config(
        materialized='table',
        description='Payment fact table with full transaction lineage'
    )
}}

with payments as (
    select * from {{ ref('stg_payments') }}
),

bids as (
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
    -- Payment attributes (grain)
    p.payment_id,
    p.payment_date,
    p.processed_at,
    p.payment_amount,
    p.payment_method,
    p.payment_category,
    p.payment_year,
    p.payment_month,
    p.payment_day_of_week,
    
    -- Bid context
    p.bid_id,
    b.bid_amount,
    b.estimated_hours,
    b.bid_status,
    b.bid_size,
    b.created_at as bid_created_at,
    
    -- Request context
    b.request_id,
    r.category as service_category,
    r.event_date,
    r.event_city,
    r.event_state,
    r.budget as request_budget,
    r.guest_count,
    r.budget_tier,
    r.event_size,
    
    -- Vendor context
    b.vendor_id,
    v.business_name as vendor_name,
    v.category as vendor_category,
    v.vendor_tier,
    v.rating as vendor_rating,
    v.city as vendor_city,
    v.state as vendor_state,
    
    -- Customer context
    r.customer_id,
    c.full_name as customer_name,
    c.city as customer_city,
    c.state as customer_state,
    
    -- Financial metrics
    p.payment_amount - b.bid_amount as payment_variance,
    case
        when b.bid_amount > 0 
        then round((p.payment_amount - b.bid_amount) / b.bid_amount * 100, 2)
        else 0
    end as payment_variance_pct,
    
    case
        when r.budget > 0 
        then round(p.payment_amount / r.budget * 100, 2)
        else null
    end as payment_to_budget_pct,
    
    -- Time metrics
    datediff('day', b.created_at, p.payment_date) as days_bid_to_payment,
    datediff('day', p.payment_date, r.event_date) as days_payment_to_event,
    
    -- Flags
    case 
        when p.payment_amount >= b.bid_amount then true 
        else false 
    end as is_full_payment,
    case 
        when v.state = r.event_state then true 
        else false 
    end as is_local_vendor,
    
    -- Metadata
    current_timestamp as _updated_at

from payments p
inner join bids b on p.bid_id = b.bid_id
inner join requests r on b.request_id = r.request_id
inner join vendors v on b.vendor_id = v.vendor_id
inner join customers c on r.customer_id = c.customer_id

