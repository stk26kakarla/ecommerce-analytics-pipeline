-- stg_orders: cleaned order headers.
--
-- Status and payment_method are lowercased for consistent downstream filtering.
-- Missing shipping fields are coalesced to 'UNKNOWN' rather than propagating
-- NULLs into the mart layer.

with source as (

    select *
    from read_csv_auto(
        '{{ var("raw_samples_path") }}/orders_sample.csv',
        header = true
    )

),

renamed as (

    select
        -- Primary key
        order_id::integer                           as order_id,

        -- Foreign key
        customer_id::integer                        as customer_id,

        -- Dates / timestamps
        order_date::date                            as order_date,
        order_timestamp::timestamp                  as order_timestamp,

        -- Status (normalised to lowercase)
        lower(trim(status))                         as status,
        lower(payment_method)                       as payment_method,

        -- Shipping (NULL → UNKNOWN so downstream joins don't silently drop rows)
        coalesce(shipping_city,  'UNKNOWN')         as shipping_city,
        coalesce(shipping_state, 'UNKNOWN')         as shipping_state,
        coalesce(shipping_zip,   'UNKNOWN')         as shipping_zip,

        -- Financials
        subtotal::double                            as subtotal,
        shipping_cost::double                       as shipping_cost,
        tax::double                                 as tax,
        total::double                               as total,

        -- Convenience partition columns (derived from order_date)
        year(order_date::date)                      as order_year,
        month(order_date::date)                     as order_month

    from source

)

select * from renamed
