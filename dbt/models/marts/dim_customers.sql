-- dim_customers: SCD Type 2 customer dimension.
--
-- Reads from the customers_snapshot dbt snapshot, which tracks changes across
-- eight slowly-changing attributes (email, phone, address, city, state,
-- zip_code, income_bracket, is_churned).  Each time one of those columns
-- changes, a new row is inserted and the previous row is closed.
--
-- customer_key is a surrogate built from (customer_id + effective_from) so
-- it is unique per customer version, not per customer.  Use is_current = true
-- to retrieve the latest snapshot of each customer.
--
-- Prerequisites
--   Run `dbt snapshot --project-dir dbt --profiles-dir dbt` before this model
--   (or use `make dbt-all` which handles ordering automatically).
--
-- Grain: one row per customer version (customer_id + effective_from).

with snapshot as (

    select * from {{ ref('customers_snapshot') }}

),

final as (

    select
        -- ── Surrogate key (unique per version) ────────────────────────────────
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }}
                                                    as customer_key,

        -- ── Natural key ───────────────────────────────────────────────────────
        customer_id,

        -- ── Attributes at this point in history ───────────────────────────────
        first_name,
        last_name,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        country,
        gender,
        income_bracket,
        registration_date,
        birth_date,
        is_churned,
        churn_date,

        -- ── SCD2 validity window ──────────────────────────────────────────────
        dbt_valid_from                              as effective_from,
        dbt_valid_to                                as effective_to,
        (dbt_valid_to is null)                      as is_current,
        dbt_updated_at                              as last_updated_at

    from snapshot

)

select * from final
