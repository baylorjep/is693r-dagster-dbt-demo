{{
    config(
        materialized='table',
        description='Vendor dimension with performance statistics'
    )
}}

with vendors as (
    select * from {{ ref('stg_vendors') }}
),

bids as (
    select * from {{ ref('stg_bids') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

-- Calculate vendor bid metrics
vendor_bids as (
    select
        vendor_id,
        count(*) as total_bids,
        count(case when bid_status = 'accepted' then 1 end) as accepted_bids,
        count(case when bid_status = 'rejected' then 1 end) as rejected_bids,
        count(case when bid_status = 'pending' then 1 end) as pending_bids,
        avg(bid_amount) as avg_bid_amount,
        min(bid_amount) as min_bid_amount,
        max(bid_amount) as max_bid_amount,
        min(created_at) as first_bid_at,
        max(created_at) as last_bid_at
    from bids
    group by vendor_id
),

-- Calculate vendor revenue metrics
vendor_revenue as (
    select
        b.vendor_id,
        count(p.payment_id) as total_payments,
        coalesce(sum(p.payment_amount), 0) as total_revenue,
        coalesce(avg(p.payment_amount), 0) as avg_payment
    from bids b
    inner join payments p on b.bid_id = p.bid_id
    where b.bid_status = 'accepted'
    group by b.vendor_id
)

select
    -- Vendor attributes
    v.vendor_id,
    v.business_name,
    v.contact_name,
    v.email,
    v.phone,
    v.category,
    v.city,
    v.state,
    v.rating,
    v.hourly_rate,
    v.years_experience,
    v.vendor_tier,
    v.created_at as vendor_since,
    
    -- Bid metrics
    coalesce(vb.total_bids, 0) as total_bids,
    coalesce(vb.accepted_bids, 0) as accepted_bids,
    coalesce(vb.rejected_bids, 0) as rejected_bids,
    coalesce(vb.pending_bids, 0) as pending_bids,
    coalesce(vb.avg_bid_amount, 0) as avg_bid_amount,
    coalesce(vb.min_bid_amount, 0) as min_bid_amount,
    coalesce(vb.max_bid_amount, 0) as max_bid_amount,
    vb.first_bid_at,
    vb.last_bid_at,
    
    -- Win rate calculation
    case
        when vb.total_bids > 0 
        then round(cast(vb.accepted_bids as decimal) / vb.total_bids * 100, 2)
        else 0
    end as win_rate_pct,
    
    -- Revenue metrics
    coalesce(vr.total_payments, 0) as total_payments,
    coalesce(vr.total_revenue, 0) as total_revenue,
    coalesce(vr.avg_payment, 0) as avg_payment,
    
    -- Derived vendor segments
    case
        when vr.total_revenue >= 25000 then 'Platinum Partner'
        when vr.total_revenue >= 10000 then 'Gold Partner'
        when vr.total_revenue >= 5000 then 'Silver Partner'
        when vr.total_revenue > 0 then 'Bronze Partner'
        else 'New Vendor'
    end as partner_level,
    
    case
        when vb.total_bids >= 20 then 'Very Active'
        when vb.total_bids >= 10 then 'Active'
        when vb.total_bids >= 5 then 'Moderate'
        when vb.total_bids > 0 then 'Low Activity'
        else 'Inactive'
    end as activity_status,
    
    -- Metadata
    current_timestamp as _updated_at

from vendors v
left join vendor_bids vb on v.vendor_id = vb.vendor_id
left join vendor_revenue vr on v.vendor_id = vr.vendor_id

