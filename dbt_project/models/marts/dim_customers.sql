{{
    config(
        materialized='table',
        description='Customer dimension with lifetime metrics'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

requests as (
    select * from {{ ref('stg_requests') }}
),

bids as (
    select * from {{ ref('stg_bids') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

-- Calculate customer request metrics
customer_requests as (
    select
        customer_id,
        count(*) as total_requests,
        count(case when request_status = 'completed' then 1 end) as completed_requests,
        count(case when request_status = 'open' then 1 end) as open_requests,
        sum(budget) as total_budget,
        avg(budget) as avg_budget,
        min(created_at) as first_request_at,
        max(created_at) as last_request_at
    from requests
    group by customer_id
),

-- Calculate customer payment metrics (via accepted bids)
customer_payments as (
    select
        r.customer_id,
        count(p.payment_id) as total_payments,
        coalesce(sum(p.payment_amount), 0) as lifetime_value
    from requests r
    inner join bids b on r.request_id = b.request_id and b.bid_status = 'accepted'
    inner join payments p on b.bid_id = p.bid_id
    group by r.customer_id
),

-- Calculate customer bid metrics
customer_bids as (
    select
        r.customer_id,
        count(b.bid_id) as total_bids_received,
        count(case when b.bid_status = 'accepted' then 1 end) as accepted_bids
    from requests r
    left join bids b on r.request_id = b.request_id
    group by r.customer_id
)

select
    -- Customer attributes
    c.customer_id,
    c.first_name,
    c.last_name,
    c.full_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.created_at as customer_since,
    
    -- Request metrics
    coalesce(cr.total_requests, 0) as total_requests,
    coalesce(cr.completed_requests, 0) as completed_requests,
    coalesce(cr.open_requests, 0) as open_requests,
    coalesce(cr.total_budget, 0) as total_budget,
    coalesce(cr.avg_budget, 0) as avg_budget,
    cr.first_request_at,
    cr.last_request_at,
    
    -- Bid metrics
    coalesce(cb.total_bids_received, 0) as total_bids_received,
    coalesce(cb.accepted_bids, 0) as accepted_bids,
    
    -- Payment/value metrics
    coalesce(cp.total_payments, 0) as total_payments,
    coalesce(cp.lifetime_value, 0) as lifetime_value,
    
    -- Derived customer segments
    case
        when cp.lifetime_value >= 10000 then 'VIP'
        when cp.lifetime_value >= 5000 then 'Gold'
        when cp.lifetime_value >= 1000 then 'Silver'
        when cp.lifetime_value > 0 then 'Bronze'
        else 'Prospect'
    end as customer_segment,
    
    case
        when cr.total_requests >= 5 then 'Frequent'
        when cr.total_requests >= 2 then 'Repeat'
        when cr.total_requests = 1 then 'Single'
        else 'Inactive'
    end as activity_level,
    
    -- Metadata
    current_timestamp as _updated_at

from customers c
left join customer_requests cr on c.customer_id = cr.customer_id
left join customer_payments cp on c.customer_id = cp.customer_id
left join customer_bids cb on c.customer_id = cb.customer_id

