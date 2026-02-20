-- int_customer_360: full customer profile with lifetime order statistics.
--
-- Enriches every customer with:
--   - Revenue order stats (completed / shipped / delivered only)
--   - Preferred product category (highest unit volume)
--   - Recency-based engagement_status
--   - Revenue-based value_segment
--
-- Customers who have never placed a qualifying order are preserved with
-- zeroed metrics and engagement_status = 'never_purchased'.
--
-- Grain: one row per customer_id.

with customers as (

    select * from {{ ref('stg_customers') }}

),

-- Only count orders that generated revenue
revenue_orders as (

    select * from {{ ref('stg_orders') }}
    where status in ('completed', 'delivered', 'shipped')

),

-- Per-customer order-level aggregations
customer_order_stats as (

    select
        customer_id,
        count(distinct order_id)                                        as total_orders,
        min(order_date)                                                 as first_order_date,
        max(order_date)                                                 as last_order_date,
        sum(total)                                                      as total_revenue,
        avg(total)                                                      as avg_order_value,
        datediff('day', min(order_date), max(order_date))               as customer_tenure_days,

        -- Average days between consecutive orders (NULL for single-order customers)
        case
            when count(distinct order_id) > 1
            then datediff('day', min(order_date), max(order_date))
                 / (count(distinct order_id) - 1)::double
            else null
        end                                                             as avg_days_between_orders

    from revenue_orders
    group by customer_id

),

-- Category-level unit counts per customer to derive preferred_category
category_units as (

    select
        o.customer_id,
        p.category,
        sum(oi.quantity)            as units_bought
    from {{ ref('stg_order_items') }} oi
    inner join revenue_orders o
        on oi.order_id = o.order_id
    inner join {{ ref('stg_products') }} p
        on oi.product_id = p.product_id
    group by o.customer_id, p.category

),

-- Rank categories per customer; pick the top-volume one
preferred_category as (

    select
        customer_id,
        category as preferred_category
    from (
        select
            customer_id,
            category,
            row_number() over (
                partition by customer_id
                order by units_bought desc, category   -- deterministic tie-break
            ) as rn
        from category_units
    ) ranked
    where rn = 1

),

final as (

    select
        -- ── Identity ──────────────────────────────────────────────────────────
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.city,
        c.state,
        c.country,
        c.gender,
        c.income_bracket,
        c.registration_date,
        c.birth_date,
        c.is_churned,
        c.churn_date,

        -- ── Order metrics (zero-filled for no-purchase customers) ─────────────
        coalesce(cos.total_orders, 0)               as total_orders,
        cos.first_order_date,
        cos.last_order_date,
        coalesce(cos.total_revenue, 0.0)            as total_revenue,
        coalesce(cos.avg_order_value, 0.0)          as avg_order_value,
        cos.avg_days_between_orders,
        cos.customer_tenure_days,

        -- ── Behavioural ───────────────────────────────────────────────────────
        pc.preferred_category,
        datediff('day', c.registration_date, current_date)
                                                    as days_since_registration,

        -- ── Engagement status (recency-based) ─────────────────────────────────
        case
            when cos.last_order_date >= current_date - interval '90 days'
                then 'active'
            when cos.last_order_date >= current_date - interval '180 days'
                then 'at_risk'
            when cos.total_orders is null
                then 'never_purchased'
            else
                'lapsed'
        end                                         as engagement_status,

        -- ── Value segment (revenue-based) ─────────────────────────────────────
        case
            when coalesce(cos.total_revenue, 0) >= 1000 then 'high_value'
            when coalesce(cos.total_revenue, 0) >= 300  then 'mid_value'
            else                                              'low_value'
        end                                         as value_segment

    from customers c
    left join customer_order_stats cos
        on c.customer_id = cos.customer_id
    left join preferred_category pc
        on c.customer_id = pc.customer_id

)

select * from final
