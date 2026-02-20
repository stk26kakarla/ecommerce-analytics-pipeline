-- Custom test: assert_revenue_share_sums_to_100
--
-- Business rule: for any given day, the revenue_share_pct values across all
-- (category, sub_category) combinations must sum to approximately 100 %.
-- A tolerance of ± 1.0 is allowed to accommodate floating-point rounding
-- when many sub-categories share a single day.
--
-- Test passes when this query returns zero rows.

with daily_sums as (

    select
        order_date,
        sum(revenue_share_pct) as total_share

    from {{ ref('fct_daily_revenue') }}
    group by order_date

)

select
    order_date,
    round(total_share, 4) as total_share
from daily_sums
where abs(total_share - 100) > 1.0
