-- Custom test: assert_order_total_positive
--
-- Business rule: every order must have a strictly positive grand total.
-- A zero or negative total indicates a data-entry error, a full refund
-- recorded as a separate credit note, or a failed cast in the staging layer.
--
-- Test passes when this query returns zero rows.

select
    order_id,
    total
from {{ ref('stg_orders') }}
where total <= 0
