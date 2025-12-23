{{
    config(
        materialized='view',
        description='Staged project data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'projects') }}
),

staged as (
    select
        -- Primary key
        project_id,
        
        -- Project details
        client_name,
        project_name,
        project_type,
        
        -- Location
        upper(location_state) as location_state,
        
        -- Dates
        cast(bid_date as date) as bid_date,
        
        -- Version tracking
        plan_set_version,
        status as project_status,
        
        -- Derived fields
        case
            when project_type = 'residential' then 'Residential'
            when project_type = 'commercial' then 'Commercial'
            when project_type = 'industrial' then 'Industrial'
            when project_type = 'mixed_use' then 'Mixed Use'
            else 'Other'
        end as project_type_display,
        
        -- Metadata
        cast(created_at as timestamp) as created_at,
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

