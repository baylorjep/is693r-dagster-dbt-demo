{{
    config(
        materialized='view',
        description='Staged QA review data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'qa_reviews') }}
),

staged as (
    select
        -- Primary key
        review_id,
        
        -- Foreign keys (nullable for page_id and takeoff_id)
        project_id,
        page_id,
        takeoff_id,
        
        -- Review details
        issue_type,
        severity,
        resolved,
        
        -- Derived fields
        case issue_type
            when 'missing_item' then 'Missing Item'
            when 'quantity_mismatch' then 'Quantity Mismatch'
            when 'wrong_uom' then 'Wrong Unit of Measure'
            when 'duplicate' then 'Duplicate Entry'
            when 'low_confidence' then 'Low Confidence Score'
            when 'cost_outlier' then 'Cost Outlier'
            else 'Other'
        end as issue_type_display,
        
        case severity
            when 'low' then 1
            when 'medium' then 2
            when 'high' then 3
            when 'critical' then 4
            else 0
        end as severity_score,
        
        case
            when severity in ('high', 'critical') then true
            else false
        end as is_critical,
        
        -- Status display
        case
            when resolved then 'Resolved'
            else 'Open'
        end as status,
        
        -- Metadata
        cast(reviewed_at as timestamp) as reviewed_at,
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

