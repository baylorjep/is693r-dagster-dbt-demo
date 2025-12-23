-- Custom test: All takeoff quantities must be positive
-- This is a critical business rule validation

select
    takeoff_id,
    quantity
from {{ ref('stg_takeoff_items') }}
where quantity <= 0

