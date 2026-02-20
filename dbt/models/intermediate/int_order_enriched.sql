-- int_order_enriched: denormalised order line items with product context.
--
-- Joins order headers → line items → products into one wide table so that
-- downstream mart models can aggregate without performing additional joins.
-- Cost and gross-profit are pre-computed here to centralise the margin logic.
--
-- Grain: one row per order_item_id.

with orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

enriched as (

    select
        -- ── Keys ──────────────────────────────────────────────────────────────
        oi.order_item_id,
        o.order_id,
        o.customer_id,

        -- ── Order context ─────────────────────────────────────────────────────
        o.order_date,
        o.order_timestamp,
        o.status,
        o.payment_method,
        o.shipping_city,
        o.shipping_state,
        o.order_year,
        o.order_month,

        -- ── Product context ───────────────────────────────────────────────────
        oi.product_id,
        p.product_name,
        p.category,
        p.sub_category,
        p.brand,

        -- ── Line-item metrics ─────────────────────────────────────────────────
        oi.quantity,
        oi.unit_price,
        oi.discount_pct,
        oi.line_total,

        -- ── Cost / profit (sourced from product catalogue) ────────────────────
        p.cost                                      as unit_cost,
        oi.quantity * p.cost                        as item_cost,
        oi.line_total - (oi.quantity * p.cost)      as item_gross_profit,

        -- ── Order totals (denormalised for convenience) ───────────────────────
        o.subtotal,
        o.shipping_cost,
        o.tax,
        o.total                                     as order_total

    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    inner join products p
        on oi.product_id = p.product_id

)

select * from enriched
