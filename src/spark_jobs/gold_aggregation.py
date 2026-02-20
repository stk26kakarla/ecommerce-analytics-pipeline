#!/usr/bin/env python3
"""PySpark Gold Aggregation Job

Reads cleaned silver Parquet and builds four business-level aggregates,
writing each to the gold S3 layer (Parquet) and to data/processed/ (CSV).

Aggregates
----------
1. daily_revenue_by_category
   Daily revenue, order count, item count, average order value, average
   discount, and revenue share (%) broken down by product category and
   sub-category.  Only "revenue" orders are counted (completed / shipped /
   delivered).

2. customer_lifetime_value
   One row per current customer with total spend, order frequency,
   recency, average order value, days between orders, and a CLV segment
   (high / medium / low) assigned by revenue tercile.  Customers with no
   qualifying orders are included with zero-spend metrics so the table
   covers every customer.

3. conversion_funnel
   Monthly view of the order funnel: how many orders entered each status
   (placed → completed, cancelled, returned, etc.), the revenue captured
   at each stage, and the month-level conversion rate.

4. cohort_retention
   Tidy cohort × period retention matrix.  Each customer is assigned to
   the calendar month of their first order (cohort_month).  For every
   subsequent month we measure how many of the original cohort placed at
   least one further order, yielding a retention_rate_pct.

Silver sources
--------------
    silver/orders         → order_id, customer_id, order_date, status, total, …
    silver/order_items    → order_item_id, order_id, product_id, quantity, line_total, …
    silver/products       → product_id, category, sub_category, price, cost, …
    silver/customers      → customer_id, …, is_current  (SCD2 current rows only)

Usage
-----
    spark-submit src/spark_jobs/gold_aggregation.py
    spark-submit src/spark_jobs/gold_aggregation.py --aggregates daily_revenue clv
    spark-submit src/spark_jobs/gold_aggregation.py \\
        --silver-base data/silver --gold-base data/gold --csv-dir data/processed
"""

import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Environment ───────────────────────────────────────────────────────────────

S3_ENDPOINT  = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
SILVER_BUCKET = "silver-layer"
GOLD_BUCKET   = "gold-layer"
DEFAULT_CSV_DIR = "data/processed"

# Statuses considered "revenue" (payment was captured and not reversed)
REVENUE_STATUSES = {"completed", "delivered", "shipped"}


# ── Spark session ─────────────────────────────────────────────────────────────

def create_spark_session():
    return (
        SparkSession.builder
        .appName("GoldAggregation")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.sql.parquet.mergeSchema", "true")
        .getOrCreate()
    )


# ── IO helpers ────────────────────────────────────────────────────────────────

def read_silver(spark, entity, silver_base):
    path = f"{silver_base}/{entity}"
    return spark.read.option("mergeSchema", "true").parquet(path)


def write_gold(df, name, gold_base, csv_dir, partition_by=None):
    """Write a gold DataFrame to Parquet (S3) and CSV (local)."""
    # ── Parquet → gold layer ──────────────────────────────────────────────
    parquet_path = f"{gold_base}/{name}"
    writer = df.write.mode("overwrite").option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(parquet_path)

    # ── CSV → data/processed/ ─────────────────────────────────────────────
    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, f"{name}.csv")
    # toPandas is safe here: all gold tables are aggregated and compact
    df.toPandas().to_csv(csv_path, index=False)

    row_count = df.count()
    print(f"  [{name}]  {row_count:,} rows")
    print(f"    Parquet : {parquet_path}")
    print(f"    CSV     : {csv_path}")
    return row_count


# ── Shared base join ──────────────────────────────────────────────────────────

def build_revenue_line_items(orders, order_items, products):
    """Join order_items → products → revenue orders.

    Returns one row per line item on a revenue order with category,
    order_date, customer_id, and all monetary columns.
    Revenue orders = completed / shipped / delivered.
    """
    rev_orders = orders.filter(
        F.col("status").isin(list(REVENUE_STATUSES))
    ).select(
        "order_id", "customer_id", "order_date",
        "order_year", "order_month", "total",
    )

    return (
        order_items
        .join(products.select(
            "product_id", "category", "sub_category",
            "price", "cost",
        ), "product_id")
        .join(rev_orders, "order_id")
    )


# ── Aggregate 1: Daily revenue by category ────────────────────────────────────

def build_daily_revenue_by_category(orders, order_items, products):
    """Daily revenue, volume, and margin broken down by category.

    Columns
    -------
    order_date, category, sub_category,
    total_revenue, order_count, item_count,
    avg_order_value, avg_discount_pct,
    gross_profit, margin_pct,
    revenue_share_pct          -- % of that day's cross-category total
    order_year, order_month    -- partition keys
    """
    lines = build_revenue_line_items(orders, order_items, products)

    # Aggregate per day × category × sub-category
    daily_cat = (
        lines
        .groupBy(
            "order_date", "order_year", "order_month",
            "category", "sub_category",
        )
        .agg(
            F.round(F.sum("line_total"),   2).alias("total_revenue"),
            F.countDistinct("order_id")    .alias("order_count"),
            F.sum("quantity")              .alias("item_count"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
            F.round(
                F.sum(F.col("quantity") * (F.col("price") - F.col("cost"))), 2
            ).alias("gross_profit"),
        )
    )

    # Daily totals for revenue share
    daily_total = (
        daily_cat
        .groupBy("order_date")
        .agg(F.sum("total_revenue").alias("_day_total"))
    )

    result = (
        daily_cat
        .join(daily_total, "order_date")
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_revenue") / F.col("order_count"), 2),
        )
        .withColumn(
            "margin_pct",
            F.round(F.col("gross_profit") / F.nullif(F.col("total_revenue"), 0) * 100, 2),
        )
        .withColumn(
            "revenue_share_pct",
            F.round(F.col("total_revenue") / F.col("_day_total") * 100, 2),
        )
        .drop("_day_total")
        .orderBy("order_date", "category", "sub_category")
    )

    return result.select(
        "order_date", "category", "sub_category",
        "total_revenue", "order_count", "item_count",
        "avg_order_value", "avg_discount_pct",
        "gross_profit", "margin_pct", "revenue_share_pct",
        "order_year", "order_month",
    )


# ── Aggregate 2: Customer Lifetime Value ──────────────────────────────────────

def build_customer_lifetime_value(orders, order_items, customers):
    """Per-customer spend, frequency, recency, and LTV segment.

    CLV segment is assigned by revenue tercile (ntile 3):
      high   → top third of customers by total_revenue
      medium → middle third
      low    → bottom third

    Columns
    -------
    customer_id, first_name, last_name, email,
    city, state, income_bracket,
    registration_date, days_since_registration,
    order_count, total_revenue, avg_order_value,
    first_order_date, last_order_date,
    days_since_last_order, avg_days_between_orders,
    clv_segment, is_churned
    """
    # Current customer dimension only (SCD2 is_current = true)
    current_customers = customers.filter(F.col("is_current")).select(
        "customer_id", "first_name", "last_name", "email",
        "city", "state", "income_bracket",
        "registration_date", "is_churned",
    )

    # Revenue orders only
    rev_orders = orders.filter(
        F.col("status").isin(list(REVENUE_STATUSES))
    ).select("order_id", "customer_id", "order_date", "total")

    # Per-order totals from line items (handles multi-item orders correctly)
    order_revenue = (
        order_items
        .join(rev_orders.select("order_id", "customer_id", "order_date"), "order_id")
        .groupBy("order_id", "customer_id", "order_date")
        .agg(F.round(F.sum("line_total"), 2).alias("order_total"))
    )

    # Per-customer stats
    today = F.current_date()
    customer_stats = (
        order_revenue
        .groupBy("customer_id")
        .agg(
            F.count("order_id")                 .alias("order_count"),
            F.round(F.sum("order_total"), 2)    .alias("total_revenue"),
            F.round(F.avg("order_total"), 2)    .alias("avg_order_value"),
            F.min("order_date")                 .alias("first_order_date"),
            F.max("order_date")                 .alias("last_order_date"),
        )
        .withColumn(
            "days_since_last_order",
            F.datediff(today, F.col("last_order_date")),
        )
        .withColumn(
            "avg_days_between_orders",
            F.when(
                F.col("order_count") > 1,
                F.round(
                    F.datediff(F.col("last_order_date"), F.col("first_order_date"))
                    / (F.col("order_count") - 1),
                    1,
                ),
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    # Left-join so customers with no revenue orders still appear (zero spend)
    clv_base = (
        current_customers
        .join(customer_stats, "customer_id", "left")
        .fillna({
            "order_count": 0,
            "total_revenue": 0.0,
            "avg_order_value": 0.0,
        })
        .withColumn(
            "days_since_registration",
            F.datediff(today, F.col("registration_date")),
        )
    )

    # CLV segment via revenue tercile (ntile handles ties correctly)
    w_clv = Window.orderBy("total_revenue")
    result = (
        clv_base
        .withColumn("_tile", F.ntile(3).over(w_clv))
        .withColumn(
            "clv_segment",
            F.when(F.col("_tile") == 3, "high")
             .when(F.col("_tile") == 2, "medium")
             .otherwise("low"),
        )
        .drop("_tile")
        .orderBy(F.col("total_revenue").desc())
    )

    return result.select(
        "customer_id", "first_name", "last_name", "email",
        "city", "state", "income_bracket",
        "registration_date", "days_since_registration",
        "order_count", "total_revenue", "avg_order_value",
        "first_order_date", "last_order_date",
        "days_since_last_order", "avg_days_between_orders",
        "clv_segment", "is_churned",
    )


# ── Aggregate 3: Conversion funnel ────────────────────────────────────────────

def build_conversion_funnel(orders):
    """Monthly order funnel with conversion rates per status.

    Two levels of detail are stacked into one tidy table:
    - status_breakdown : rows per (year, month, status) with order counts
      and revenue at each funnel stage, plus pct_of_monthly_orders.
    - Conversion rate is derived columns per (year, month):
      converted_pct = (completed + delivered + shipped) / total_orders

    Columns
    -------
    order_year, order_month,
    status, funnel_stage,
    order_count, total_revenue,
    pct_of_monthly_orders,
    monthly_total_orders, monthly_converted_orders, monthly_conversion_rate_pct
    """
    # Classify each status into a funnel stage
    funnel_stage = (
        F.when(F.col("status").isin("completed", "delivered"), "converted")
         .when(F.col("status") == "shipped",                    "in_transit")
         .when(F.col("status") == "pending",                    "pending")
         .when(F.col("status").isin("cancelled", "refunded"),   "lost")
         .when(F.col("status") == "returned",                   "returned")
         .otherwise("other")
    )

    with_meta = (
        orders
        .withColumn("funnel_stage", funnel_stage)
        .withColumn("order_year",  F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
    )

    # Monthly × status breakdown
    status_monthly = (
        with_meta
        .groupBy("order_year", "order_month", "status", "funnel_stage")
        .agg(
            F.count("order_id")           .alias("order_count"),
            F.round(F.sum("total"), 2)    .alias("total_revenue"),
        )
    )

    # Monthly totals and conversion count
    monthly_summary = (
        with_meta
        .groupBy("order_year", "order_month")
        .agg(
            F.count("order_id").alias("monthly_total_orders"),
            F.sum(
                F.when(F.col("status").isin(list(REVENUE_STATUSES)), 1)
                 .otherwise(0)
            ).alias("monthly_converted_orders"),
        )
        .withColumn(
            "monthly_conversion_rate_pct",
            F.round(
                F.col("monthly_converted_orders")
                / F.col("monthly_total_orders") * 100,
                2,
            ),
        )
    )

    result = (
        status_monthly
        .join(monthly_summary, ["order_year", "order_month"])
        .withColumn(
            "pct_of_monthly_orders",
            F.round(
                F.col("order_count") / F.col("monthly_total_orders") * 100, 2,
            ),
        )
        .orderBy("order_year", "order_month", "status")
    )

    return result.select(
        "order_year", "order_month",
        "status", "funnel_stage",
        "order_count", "total_revenue", "pct_of_monthly_orders",
        "monthly_total_orders", "monthly_converted_orders",
        "monthly_conversion_rate_pct",
    )


# ── Aggregate 4: Cohort retention ─────────────────────────────────────────────

def build_cohort_retention(orders):
    """Customer cohort retention matrix (tidy format).

    Each customer is assigned to the calendar month of their first
    revenue order.  Period 0 = acquisition month; period N = N months
    later.  retention_rate_pct is retained_customers / cohort_size.

    Output is tidy (one row per cohort × period), suitable for pivoting
    in any BI tool.

    Columns
    -------
    cohort_month, cohort_size,
    period_number,
    retained_customers, retention_rate_pct,
    cohort_year, cohort_month_num   (partition keys)
    """
    rev_orders = orders.filter(
        F.col("status").isin(list(REVENUE_STATUSES))
    ).select("customer_id", "order_date")

    # Step 1 — assign each customer their cohort month
    first_order = (
        rev_orders
        .groupBy("customer_id")
        .agg(
            F.trunc(F.min("order_date"), "month").alias("cohort_month"),
        )
    )

    # Step 2 — cohort sizes
    cohort_sizes = (
        first_order
        .groupBy("cohort_month")
        .agg(F.countDistinct("customer_id").alias("cohort_size"))
    )

    # Step 3 — tag every revenue order with the customer's cohort month
    #          and compute period = months between cohort month and order month
    orders_tagged = (
        rev_orders
        .join(first_order, "customer_id")
        .withColumn(
            "order_month_trunc",
            F.trunc("order_date", "month"),
        )
        .withColumn(
            "period_number",
            F.months_between(
                F.col("order_month_trunc"),
                F.col("cohort_month"),
            ).cast("integer"),
        )
    )

    # Step 4 — distinct customers per cohort × period
    retention_raw = (
        orders_tagged
        .groupBy("cohort_month", "period_number")
        .agg(F.countDistinct("customer_id").alias("retained_customers"))
    )

    # Step 5 — join sizes, compute rate
    result = (
        retention_raw
        .join(cohort_sizes, "cohort_month")
        .withColumn(
            "retention_rate_pct",
            F.round(
                F.col("retained_customers") / F.col("cohort_size") * 100, 2,
            ),
        )
        .withColumn("cohort_year",      F.year("cohort_month"))
        .withColumn("cohort_month_num", F.month("cohort_month"))
        .orderBy("cohort_month", "period_number")
    )

    return result.select(
        "cohort_month", "cohort_size",
        "period_number",
        "retained_customers", "retention_rate_pct",
        "cohort_year", "cohort_month_num",
    )


# ── Orchestration ─────────────────────────────────────────────────────────────

def run_daily_revenue(spark, silver_base, gold_base, csv_dir):
    print("\n" + "=" * 60)
    print("  1. daily_revenue_by_category")
    print("=" * 60)
    orders      = read_silver(spark, "orders",      silver_base)
    order_items = read_silver(spark, "order_items", silver_base)
    products    = read_silver(spark, "products",    silver_base)

    df = build_daily_revenue_by_category(orders, order_items, products)
    return write_gold(
        df, "daily_revenue_by_category",
        gold_base, csv_dir,
        partition_by=["order_year", "order_month"],
    )


def run_clv(spark, silver_base, gold_base, csv_dir):
    print("\n" + "=" * 60)
    print("  2. customer_lifetime_value")
    print("=" * 60)
    orders      = read_silver(spark, "orders",      silver_base)
    order_items = read_silver(spark, "order_items", silver_base)
    customers   = read_silver(spark, "customers",   silver_base)

    df = build_customer_lifetime_value(orders, order_items, customers)
    return write_gold(df, "customer_lifetime_value", gold_base, csv_dir)


def run_funnel(spark, silver_base, gold_base, csv_dir):
    print("\n" + "=" * 60)
    print("  3. conversion_funnel")
    print("=" * 60)
    orders = read_silver(spark, "orders", silver_base)

    df = build_conversion_funnel(orders)
    return write_gold(
        df, "conversion_funnel",
        gold_base, csv_dir,
        partition_by=["order_year", "order_month"],
    )


def run_cohort(spark, silver_base, gold_base, csv_dir):
    print("\n" + "=" * 60)
    print("  4. cohort_retention")
    print("=" * 60)
    orders = read_silver(spark, "orders", silver_base)

    df = build_cohort_retention(orders)
    return write_gold(
        df, "cohort_retention",
        gold_base, csv_dir,
        partition_by=["cohort_year", "cohort_month_num"],
    )


RUNNERS = {
    "daily_revenue": run_daily_revenue,
    "clv":           run_clv,
    "funnel":        run_funnel,
    "cohort":        run_cohort,
}


# ── CLI & main ────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Gold aggregation: silver Parquet -> business aggregates",
    )
    p.add_argument(
        "--silver-base",
        default=None,
        help=f"Silver input base path (default: s3a://{SILVER_BUCKET})",
    )
    p.add_argument(
        "--gold-base",
        default=None,
        help=f"Gold output base path (default: s3a://{GOLD_BUCKET})",
    )
    p.add_argument(
        "--csv-dir",
        default=DEFAULT_CSV_DIR,
        help=f"Local directory for exported CSVs (default: {DEFAULT_CSV_DIR})",
    )
    p.add_argument(
        "--aggregates",
        nargs="+",
        default=list(RUNNERS.keys()),
        choices=list(RUNNERS.keys()),
        help="Aggregates to build (default: all). "
             "Choices: " + ", ".join(RUNNERS.keys()),
    )
    return p.parse_args()


def main():
    args   = parse_args()
    silver = args.silver_base or f"s3a://{SILVER_BUCKET}"
    gold   = args.gold_base   or f"s3a://{GOLD_BUCKET}"

    print("=" * 60)
    print("  GOLD AGGREGATION JOB")
    print("=" * 60)
    print(f"  Silver   : {silver}")
    print(f"  Gold     : {gold}")
    print(f"  CSV dir  : {args.csv_dir}")
    print(f"  Builds   : {', '.join(args.aggregates)}")
    print(f"  Time     : {datetime.utcnow().isoformat()}Z")

    spark   = create_spark_session()
    results = {}

    for name in args.aggregates:
        runner = RUNNERS[name]
        try:
            rows = runner(spark, silver, gold, args.csv_dir)
            results[name] = ("OK", rows)
        except Exception as e:
            print(f"\n  ERROR [{name}]: {e}")
            results[name] = ("FAIL", str(e))

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  GOLD AGGREGATION SUMMARY")
    print("=" * 60)
    all_ok = True
    for name, (status, detail) in results.items():
        if status == "OK":
            print(f"  {name:25s}  [OK]   {detail:>8,} rows")
        else:
            all_ok = False
            print(f"  {name:25s}  [FAIL] {detail}")
    print("=" * 60)

    spark.stop()

    if not all_ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
