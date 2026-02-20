-- stg_customers: cleaned, typed customer records.
--
-- Reads directly from the committed sample CSV via DuckDB's read_csv_auto.
-- To switch to the silver-layer Parquet, replace the source CTE with:
--   select * from read_parquet('{{ var("silver_path") }}/customers/**/*.parquet')

with source as (

    select *
    from read_csv_auto(
        '{{ var("raw_samples_path") }}/customers_sample.csv',
        header = true
    )

),

renamed as (

    select
        -- Primary key
        customer_id::integer                        as customer_id,

        -- Name / contact
        trim(first_name)                            as first_name,
        trim(last_name)                             as last_name,
        lower(trim(email))                          as email,
        phone                                       as phone,

        -- Address
        address                                     as address,
        city                                        as city,
        state                                       as state,
        zip_code                                    as zip_code,
        country                                     as country,

        -- Dates
        registration_date::date                     as registration_date,
        birth_date::date                            as birth_date,

        -- Demographics
        lower(gender)                               as gender,
        income_bracket                              as income_bracket,

        -- Churn
        is_churned::boolean                         as is_churned,
        try_cast(churn_date as date)                as churn_date

    from source

)

select * from renamed
