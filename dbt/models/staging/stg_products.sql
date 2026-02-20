-- stg_products: cleaned, typed product catalogue.
--
-- Rating is clamped to [0.0, 5.0] to catch any generation drift.
-- All 5 000 SKUs are loaded (no sample truncation for products).

with source as (

    select *
    from read_csv_auto(
        '{{ var("raw_samples_path") }}/products_sample.csv',
        header = true
    )

),

renamed as (

    select
        -- Primary key
        product_id::integer                             as product_id,

        -- Descriptors
        trim(product_name)                              as product_name,
        category                                        as category,
        sub_category                                    as sub_category,
        brand                                           as brand,

        -- Pricing
        price::double                                   as price,
        cost::double                                    as cost,

        -- Physical
        weight_kg::double                               as weight_kg,

        -- Social proof — clamp to valid star range
        greatest(0.0, least(5.0, rating::double))       as rating,
        review_count::integer                           as review_count,

        -- Availability
        is_active::boolean                              as is_active,
        created_date::date                              as created_date

    from source

)

select * from renamed
