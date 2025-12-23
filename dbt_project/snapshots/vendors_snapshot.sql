{% snapshot vendors_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='vendor_id',
        strategy='check',
        check_cols=['rating', 'hourly_rate', 'category', 'vendor_tier'],
        invalidate_hard_deletes=True
    )
}}

/*
    Vendor Profile Snapshot (SCD Type 2)
    
    This snapshot tracks changes to vendor profiles over time, specifically:
    - Rating changes (as customers leave reviews)
    - Hourly rate adjustments
    - Category changes
    - Tier changes (derived from rating)
    
    This enables historical analysis of vendor performance and pricing trends.
    
    DP-203 Concept: Slowly Changing Dimensions (SCD Type 2)
    - Preserves historical state of dimension attributes
    - Enables point-in-time analysis
    - Uses valid_from/valid_to pattern for time-based queries
*/

select
    vendor_id,
    business_name,
    contact_name,
    email,
    phone,
    category,
    city,
    state,
    rating,
    hourly_rate,
    years_experience,
    -- Derived tier (tracked for changes)
    case
        when rating >= 4.5 then 'Premium'
        when rating >= 4.0 then 'Standard'
        else 'Basic'
    end as vendor_tier,
    created_at
from {{ source('raw', 'vendors') }}

{% endsnapshot %}

