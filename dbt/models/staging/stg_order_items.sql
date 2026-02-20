-- stg_order_items: cleaned order line items.
--
-- One row per product per order.  line_total already reflects the applied
-- discount; downstream cost / profit calculations use the products' cost column.

with source as (

    select *
    from read_csv_auto(
        '{{ var("raw_samples_path") }}/order_items_sample.csv',
        header = true
    )

),

renamed as (

    select
        -- Primary key
        order_item_id::integer      as order_item_id,

        -- Foreign keys
        order_id::integer           as order_id,
        product_id::integer         as product_id,

        -- Line details
        quantity::integer           as quantity,
        unit_price::double          as unit_price,
        discount_pct::double        as discount_pct,
        line_total::double          as line_total

    from source

)

select * from renamed
