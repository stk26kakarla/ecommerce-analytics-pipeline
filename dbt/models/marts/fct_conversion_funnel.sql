-- fct_conversion_funnel: monthly order conversion funnel.
--
-- Classifies every order into a funnel stage and aggregates by
-- (order_year, order_month, funnel_stage).  Also provides monthly totals
-- so a BI tool can compute conversion rates without a sub-query.
--
-- Funnel stage mapping
--   converted   → completed or delivered  (revenue recognised)
--   in_transit  → shipped                 (revenue probable)
--   pending     → pending                 (not yet committed)
--   lost        → cancelled               (revenue lost)
--   returned    → returned                (revenue reversed)
--   refunded    → refunded                (revenue reversed)
--   other       → any other status value  (catch-all)
--
-- Grain: one row per (order_year, order_month, funnel_stage).

with orders as (

    select * from {{ ref('stg_orders') }}

),

classified as (

    select
        order_id,
        customer_id,
        order_date,
        order_year,
        order_month,
        total,
        case
            when status in ('completed', 'delivered') then 'converted'
            when status = 'shipped'                   then 'in_transit'
            when status = 'pending'                   then 'pending'
            when status = 'cancelled'                 then 'lost'
            when status = 'returned'                  then 'returned'
            when status = 'refunded'                  then 'refunded'
            else                                           'other'
        end                                         as funnel_stage

    from orders

),

monthly_by_stage as (

    select
        order_year,
        order_month,
        funnel_stage,
        count(distinct order_id)                    as order_count,
        count(distinct customer_id)                 as customer_count,
        sum(total)                                  as stage_revenue

    from classified
    group by order_year, order_month, funnel_stage

),

monthly_totals as (

    select
        order_year,
        order_month,
        sum(order_count)                                                    as total_orders,
        sum(case when funnel_stage = 'converted' then order_count  else 0 end)
                                                                            as converted_orders,
        sum(case when funnel_stage = 'converted' then stage_revenue else 0 end)
                                                                            as converted_revenue

    from monthly_by_stage
    group by order_year, order_month

),

final as (

    select
        ms.order_year,
        ms.order_month,
        ms.funnel_stage,
        ms.order_count,
        ms.customer_count,
        ms.stage_revenue,
        mt.total_orders,
        mt.converted_orders,
        mt.converted_revenue,

        -- This stage's share of all orders in the month
        round(
            ms.order_count / nullif(mt.total_orders, 0) * 100, 2
        )                                           as stage_share_pct,

        -- Month-level conversion rate (same for every stage in the month)
        round(
            mt.converted_orders / nullif(mt.total_orders, 0) * 100, 2
        )                                           as monthly_conversion_rate_pct

    from monthly_by_stage ms
    inner join monthly_totals mt
        on  ms.order_year  = mt.order_year
        and ms.order_month = mt.order_month

)

select * from final
order by order_year, order_month, funnel_stage
