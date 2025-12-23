-- Custom test: All bid amounts must be positive
-- This is a critical business rule validation

select
    bid_id,
    bid_amount
from {{ ref('stg_bids') }}
where bid_amount <= 0

