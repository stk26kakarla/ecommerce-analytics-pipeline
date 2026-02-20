-- dim_products: enriched product dimension with derived tier classifications.
--
-- Products are not versioned (no SCD2) — the product catalogue is treated as
-- a current snapshot.  Adds unit_margin, margin_pct, price_tier, and
-- rating_tier to support segmentation in BI tools without custom expressions.
--
-- Grain: one row per product_id.

with products as (

    select * from {{ ref('stg_products') }}

),

final as (

    select
        -- ── Primary key ───────────────────────────────────────────────────────
        product_id,

        -- ── Descriptors ───────────────────────────────────────────────────────
        product_name,
        category,
        sub_category,
        brand,

        -- ── Pricing ───────────────────────────────────────────────────────────
        price,
        cost,
        round(price - cost, 2)                                  as unit_margin,
        round((price - cost) / nullif(price, 0) * 100, 2)       as margin_pct,

        -- ── Physical ──────────────────────────────────────────────────────────
        weight_kg,

        -- ── Social proof ──────────────────────────────────────────────────────
        rating,
        review_count,

        -- ── Availability ──────────────────────────────────────────────────────
        is_active,
        created_date,

        -- ── Derived tiers ─────────────────────────────────────────────────────
        case
            when price < 25  then 'budget'
            when price < 100 then 'mid_range'
            when price < 500 then 'premium'
            else                  'luxury'
        end                                                     as price_tier,

        case
            when rating >= 4.5 then 'top_rated'
            when rating >= 3.5 then 'well_rated'
            when rating >= 2.5 then 'average'
            else                    'poorly_rated'
        end                                                     as rating_tier

    from products

)

select * from final
