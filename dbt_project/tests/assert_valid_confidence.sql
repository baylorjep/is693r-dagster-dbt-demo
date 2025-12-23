-- Custom test: All confidence scores must be between 0 and 1
-- This validates AI extraction quality metrics

select
    takeoff_id,
    confidence
from {{ ref('stg_takeoff_items') }}
where confidence < 0 or confidence > 1

