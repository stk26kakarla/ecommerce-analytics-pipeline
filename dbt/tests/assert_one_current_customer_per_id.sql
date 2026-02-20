-- Custom test: assert_one_current_customer_per_id
--
-- Business rule: SCD2 integrity — exactly one row in dim_customers must be
-- marked is_current = true for each customer_id.  More than one current row
-- indicates a snapshot merge bug; zero current rows means the customer record
-- was incorrectly closed.
--
-- Test passes when this query returns zero rows.

with current_versions as (

    select
        customer_id,
        count(*) as current_count

    from {{ ref('dim_customers') }}
    where is_current = true
    group by customer_id

)

select
    customer_id,
    current_count
from current_versions
where current_count <> 1
