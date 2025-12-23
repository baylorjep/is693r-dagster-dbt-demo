{% snapshot cost_library_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='cost_code',
        strategy='check',
        check_cols=['unit_cost_low', 'unit_cost_mid', 'unit_cost_high'],
        invalidate_hard_deletes=True
    )
}}

/*
    Cost Library Snapshot (SCD Type 2)
    
    This snapshot tracks changes to cost data over time, specifically:
    - Unit cost changes (low/mid/high)
    - Material and labor pricing updates
    
    This enables historical analysis of cost trends and estimate accuracy
    when pricing changes over time.
    
    DP-203 Concept: Slowly Changing Dimensions (SCD Type 2)
    - Preserves historical state of cost data
    - Enables point-in-time cost analysis
    - Uses valid_from/valid_to pattern for time-based queries
*/

select
    cost_code,
    division,
    division_name,
    item_type,
    uom,
    unit_cost_low,
    unit_cost_mid,
    unit_cost_high,
    -- Derived tier for tracking
    case
        when unit_cost_mid < 50 then 'Low Cost'
        when unit_cost_mid < 200 then 'Medium Cost'
        else 'High Cost'
    end as cost_tier,
    last_updated
from {{ source('raw', 'cost_library') }}

{% endsnapshot %}

