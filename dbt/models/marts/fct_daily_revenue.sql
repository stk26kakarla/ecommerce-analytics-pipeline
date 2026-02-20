-- fct_daily_revenue: daily gross revenue, cost, and profit by category.
--
-- Aggregates revenue-generating orders (completed, delivered, shipped)
-- at the (order_date, category, sub_category) grain.
-- Includes per-day revenue share so a BI tool can draw a 100%-stacked chart
-- without a secondary query.
--
-- Grain: one row per (order_date, category, sub_category).

with enriched as (

    select * from {{ ref('int_order_enriched') }}
    where status in ('completed', 'delivered', 'shipped')

),

daily_category as (

    select
        order_date,
        order_year,
        order_month,
        category,
        sub_category,
        count(distinct order_id)            as order_count,
        sum(quantity)                       as units_sold,
        sum(line_total)                     as gross_revenue,
        sum(item_cost)                      as total_cost,
        sum(item_gross_profit)              as gross_profit,
        round(avg(discount_pct), 4)         as avg_discount_pct

    from enriched
    group by
        order_date,
        order_year,
        order_month,
        category,
        sub_category

),

-- Window the daily total once so revenue_share_pct is a single-pass calc
with_daily_total as (

    select
        *,
        sum(gross_revenue) over (partition by order_date) as daily_total_revenue

    from daily_category

),

final as (

    select
        order_date,
        order_year,
        order_month,
        category,
        sub_category,
        order_count,
        units_sold,
        gross_revenue,
        total_cost,
        gross_profit,
        avg_discount_pct,
        daily_total_revenue,

        -- Derived ratios
        round(
            gross_profit / nullif(gross_revenue, 0) * 100, 2
        )                                   as margin_pct,

        round(
            gross_revenue / nullif(daily_total_revenue, 0) * 100, 2
        )                                   as revenue_share_pct

    from with_daily_total

)

select * from final
