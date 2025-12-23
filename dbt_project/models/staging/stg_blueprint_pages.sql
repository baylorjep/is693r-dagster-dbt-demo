{{
    config(
        materialized='view',
        description='Staged blueprint page data with standardized column names and types'
    )
}}

with source as (
    select * from {{ source('raw', 'blueprint_pages') }}
),

staged as (
    select
        -- Primary key
        page_id,
        
        -- Foreign key
        project_id,
        
        -- Page details
        sheet_number,
        upper(discipline) as discipline,
        page_title,
        image_path,
        page_version,
        
        -- Derived fields - discipline full name
        case discipline
            when 'A' then 'Architectural'
            when 'S' then 'Structural'
            when 'C' then 'Civil'
            when 'M' then 'Mechanical'
            when 'E' then 'Electrical'
            when 'P' then 'Plumbing'
            else 'Unknown'
        end as discipline_name,
        
        -- Metadata
        cast(created_at as timestamp) as created_at,
        current_timestamp as _loaded_at
        
    from source
)

select * from staged

