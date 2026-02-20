{% snapshot customers_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        strategy       = 'check',
        unique_key     = 'customer_id',

        -- Columns whose change triggers a new SCD2 row.
        -- These mirror the PySpark silver layer's SCD2_TRACKED_COLS.
        check_cols = [
            'email',
            'phone',
            'address',
            'city',
            'state',
            'zip_code',
            'income_bracket',
            'is_churned',
        ],

        -- When a customer is deleted from the source, close their record
        -- rather than leaving the effective_to window open.
        invalidate_hard_deletes = true,
    )
}}

-- Select the full current customer record as the snapshot source.
-- dbt will diff this against the previous snapshot on each run and
-- insert a new row (with updated dbt_valid_from) whenever a tracked
-- column changes, closing the previous row with dbt_valid_to set.

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
