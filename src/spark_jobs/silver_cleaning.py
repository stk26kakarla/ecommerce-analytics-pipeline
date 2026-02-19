#!/usr/bin/env python3
"""PySpark Silver Cleaning Job

Reads bronze Parquet, deduplicates across batches, applies business-rule
null handling, casts types, runs data-quality assertions, implements
SCD Type 2 for the customer dimension, and writes cleaned Parquet to
the silver S3 layer.

Pipeline per entity:
    1. Read all bronze Parquet (including late-arriving appends)
    2. Deduplicate on primary key (keep latest _ingested_at per key)
    3. Assert data quality (fail job if >5 % nulls in critical columns)
    4. Apply null-handling business rules
    5. Cast string dates to DateType / TimestampType
    6. Strip bronze metadata, add _silver_processed_at
    7. For customers: merge into SCD Type 2 dimension
    8. Write to silver layer

SCD Type 2 strategy (customers only):
    Tracked columns: email, phone, address, city, state, zip_code,
    income_bracket, is_churned.  On first run every record is inserted
    with effective_from = registration_date.  On subsequent runs an MD5
    hash of tracked columns detects changes; changed rows close the old
    version (effective_to = now, is_current = false) and open a new one.

Usage:
    spark-submit src/spark_jobs/silver_cleaning.py
    spark-submit src/spark_jobs/silver_cleaning.py --entities orders order_items
    spark-submit src/spark_jobs/silver_cleaning.py --null-threshold 10.0
"""

import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window


# ── Environment ───────────────────────────────────────────────────────────────

S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
BRONZE_BUCKET = "bronze-layer"
SILVER_BUCKET = "silver-layer"

DEFAULT_NULL_THRESHOLD = 5.0  # percent


# ── Bronze metadata columns to drop ──────────────────────────────────────────

BRONZE_META = {"_ingested_at", "_source_file", "_batch_id", "_is_late"}


# ── Data-quality checks  {column: max_null_%} ────────────────────────────────

QUALITY_CHECKS = {
    "orders": {
        "order_id": 5.0,
        "customer_id": 5.0,
        "order_date": 5.0,
        "total": 5.0,
    },
    "order_items": {
        "order_item_id": 5.0,
        "order_id": 5.0,
        "product_id": 5.0,
        "quantity": 5.0,
        "unit_price": 5.0,
    },
    "customers": {
        "customer_id": 5.0,
        "email": 5.0,
        "first_name": 5.0,
    },
    "products": {
        "product_id": 5.0,
        "product_name": 5.0,
        "price": 5.0,
        "category": 5.0,
    },
}


# ── SCD Type 2 configuration (customers) ─────────────────────────────────────

SCD2_NATURAL_KEY = "customer_id"
SCD2_TRACKED_COLS = [
    "email", "phone", "address", "city", "state",
    "zip_code", "income_bracket", "is_churned",
]


# ── Spark session ─────────────────────────────────────────────────────────────

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverCleaning")
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


# ── Data quality ──────────────────────────────────────────────────────────────

class DataQualityError(Exception):
    """Raised when data-quality assertions fail."""


def assert_quality(df, checks, entity_name, threshold_override=None):
    """Fail if any critical column exceeds its null-percentage threshold.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to check (post-dedup, pre-cleaning).
    checks : dict
        ``{column_name: max_null_percent}``
    entity_name : str
        Used in error messages.
    threshold_override : float or None
        If set, overrides per-column thresholds.
    """
    total = df.count()
    if total == 0:
        raise DataQualityError(f"{entity_name}: DataFrame is empty")

    # Build a single aggregation for all null counts
    agg_exprs = []
    check_cols = []
    for col_name in checks:
        if col_name in df.columns:
            agg_exprs.append(
                F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0))
                .alias(f"_null_{col_name}")
            )
            check_cols.append(col_name)

    if not agg_exprs:
        return  # nothing to check

    null_counts = df.agg(*agg_exprs).collect()[0]

    failures = []
    for col_name in check_cols:
        null_count = null_counts[f"_null_{col_name}"]
        null_pct = (null_count / total) * 100
        limit = threshold_override if threshold_override is not None else checks[col_name]
        if null_pct > limit:
            failures.append(
                f"{col_name}: {null_pct:.2f}% nulls "
                f"({null_count:,}/{total:,}, threshold {limit}%)"
            )

    if failures:
        header = f"Data quality failed for [{entity_name}]:"
        detail = "\n".join(f"  - {f}" for f in failures)
        raise DataQualityError(f"{header}\n{detail}")

    print(f"  Quality OK  ({len(check_cols)} checks passed, {total:,} rows)")


# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate(df, key_col, order_col="_ingested_at"):
    """Keep the most-recently-ingested record per primary key."""
    w = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ── Null-handling business rules ──────────────────────────────────────────────

def clean_orders(df):
    """Apply business-rule defaults for orders."""
    return (
        df
        # Shipping defaults
        .withColumn("shipping_city",
                     F.coalesce(F.col("shipping_city"), F.lit("UNKNOWN")))
        .withColumn("shipping_state",
                     F.coalesce(F.col("shipping_state"), F.lit("UNKNOWN")))
        .withColumn("shipping_zip",
                     F.coalesce(F.col("shipping_zip"), F.lit("00000")))
        # Payment / status defaults
        .withColumn("payment_method",
                     F.coalesce(F.col("payment_method"), F.lit("UNKNOWN")))
        .withColumn("status",
                     F.when(F.col("status").isNull(), "pending")
                     .otherwise(F.lower(F.trim(F.col("status")))))
        # Monetary defaults
        .withColumn("subtotal",
                     F.coalesce(F.col("subtotal"), F.lit(0.0)))
        .withColumn("shipping_cost",
                     F.coalesce(F.col("shipping_cost"), F.lit(0.0)))
        .withColumn("tax",
                     F.coalesce(F.col("tax"), F.lit(0.0)))
        # Recalculate total if missing
        .withColumn("total",
                     F.coalesce(
                         F.col("total"),
                         F.col("subtotal") + F.col("shipping_cost") + F.col("tax"),
                     ))
        # Cast dates
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        # Partition columns
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
    )


def clean_order_items(df):
    """Apply business-rule defaults for order items."""
    return (
        df
        .withColumn("discount_pct",
                     F.coalesce(F.col("discount_pct"), F.lit(0.0)))
        # Recalculate line_total if missing
        .withColumn("line_total",
                     F.coalesce(
                         F.col("line_total"),
                         F.round(
                             F.col("quantity")
                             * F.col("unit_price")
                             * (1 - F.col("discount_pct") / 100),
                             2,
                         ),
                     ))
    )


def clean_customers(df):
    """Apply business-rule defaults for customers."""
    return (
        df
        .withColumn("phone",
                     F.coalesce(F.col("phone"), F.lit("UNKNOWN")))
        .withColumn("city",
                     F.coalesce(F.col("city"), F.lit("UNKNOWN")))
        .withColumn("state",
                     F.coalesce(F.col("state"), F.lit("UNKNOWN")))
        .withColumn("zip_code",
                     F.coalesce(F.col("zip_code"), F.lit("00000")))
        .withColumn("country",
                     F.coalesce(F.col("country"), F.lit("US")))
        .withColumn("gender",
                     F.coalesce(F.col("gender"), F.lit("UNKNOWN")))
        .withColumn("income_bracket",
                     F.coalesce(F.col("income_bracket"), F.lit("UNKNOWN")))
        .withColumn("is_churned",
                     F.coalesce(F.col("is_churned"), F.lit(False)))
        # Cast dates
        .withColumn("registration_date", F.to_date("registration_date"))
        .withColumn("birth_date", F.to_date("birth_date"))
        .withColumn("churn_date", F.to_date("churn_date"))
    )


def clean_products(df):
    """Apply business-rule defaults for products."""
    return (
        df
        .withColumn("sub_category",
                     F.coalesce(F.col("sub_category"), F.lit("General")))
        .withColumn("brand",
                     F.coalesce(F.col("brand"), F.lit("UNKNOWN")))
        .withColumn("weight_kg",
                     F.coalesce(F.col("weight_kg"), F.lit(0.0)))
        .withColumn("rating",
                     F.coalesce(
                         F.least(F.greatest(F.col("rating"), F.lit(0.0)), F.lit(5.0)),
                         F.lit(0.0),
                     ))
        .withColumn("review_count",
                     F.coalesce(F.col("review_count"), F.lit(0)))
        .withColumn("is_active",
                     F.coalesce(F.col("is_active"), F.lit(True)))
        # Estimate cost if missing: 50 % of price
        .withColumn("cost",
                     F.coalesce(F.col("cost"),
                                F.round(F.col("price") * 0.5, 2)))
        # Cast date
        .withColumn("created_date", F.to_date("created_date"))
    )


# ── SCD Type 2 (customer dimension) ──────────────────────────────────────────

def _scd2_hash(df, tracked_cols):
    """Add a deterministic hash column over the tracked attributes."""
    parts = [
        F.coalesce(F.col(c).cast("string"), F.lit("__NULL__"))
        for c in tracked_cols
    ]
    return df.withColumn("_scd2_hash", F.md5(F.concat_ws("||", *parts)))


def build_customer_scd2(spark, incoming_df, silver_path):
    """Merge incoming customers into an SCD Type 2 dimension.

    Returns the full dimension DataFrame (historical + current rows).
    On the first run (no existing silver) every record is inserted as-is.
    """
    now = F.current_timestamp()
    end_of_time = F.to_timestamp(F.lit("9999-12-31 00:00:00"))
    key = SCD2_NATURAL_KEY
    tracked = SCD2_TRACKED_COLS

    incoming = _scd2_hash(incoming_df, tracked)

    # ── First run? ────────────────────────────────────────────────────────
    try:
        existing = spark.read.parquet(silver_path)
        _ = existing.columns  # force path resolution
    except AnalysisException:
        return (
            incoming_df
            .withColumn("effective_from",
                         F.coalesce(F.to_timestamp("registration_date"), now))
            .withColumn("effective_to", end_of_time)
            .withColumn("is_current", F.lit(True))
        )

    historical = existing.filter(~F.col("is_current"))
    current = _scd2_hash(existing.filter(F.col("is_current")), tracked)

    # ── 1. Not in batch: existing customers absent from incoming ──────────
    not_in_batch = (
        current
        .join(incoming.select(F.col(key)), key, "left_anti")
        .drop("_scd2_hash")
    )

    # ── 2. New customers: in incoming but not in existing ─────────────────
    new_records = (
        incoming
        .join(current.select(F.col(key)), key, "left_anti")
        .drop("_scd2_hash")
        .withColumn("effective_from",
                     F.coalesce(F.to_timestamp("registration_date"), now))
        .withColumn("effective_to", end_of_time)
        .withColumn("is_current", F.lit(True))
    )

    # ── 3. Matched: same key exists in both ───────────────────────────────
    matched = (
        incoming.alias("inc")
        .join(current.alias("cur"),
              F.col(f"inc.{key}") == F.col(f"cur.{key}"))
    )

    hashes_match = F.col("inc._scd2_hash") == F.col("cur._scd2_hash")

    # 3a. Unchanged — keep existing current record
    unchanged = (
        matched
        .filter(hashes_match)
        .select("cur.*")
        .drop("_scd2_hash")
    )

    # 3b. Changed — close old record
    changed_closed = (
        matched
        .filter(~hashes_match)
        .select("cur.*")
        .drop("_scd2_hash")
        .withColumn("effective_to", now)
        .withColumn("is_current", F.lit(False))
    )

    # 3c. Changed — open new record with incoming data
    changed_new = (
        matched
        .filter(~hashes_match)
        .select("inc.*")
        .drop("_scd2_hash")
        .withColumn("effective_from", now)
        .withColumn("effective_to", end_of_time)
        .withColumn("is_current", F.lit(True))
    )

    # ── Union all ─────────────────────────────────────────────────────────
    result = (
        historical
        .unionByName(not_in_batch, allowMissingColumns=True)
        .unionByName(unchanged, allowMissingColumns=True)
        .unionByName(changed_closed, allowMissingColumns=True)
        .unionByName(changed_new, allowMissingColumns=True)
        .unionByName(new_records, allowMissingColumns=True)
    )

    return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def drop_bronze_meta(df):
    """Remove bronze ingestion metadata columns."""
    to_drop = [c for c in df.columns if c in BRONZE_META]
    return df.drop(*to_drop)


def add_silver_meta(df):
    """Add silver processing timestamp."""
    return df.withColumn("_silver_processed_at", F.current_timestamp())


def read_bronze(spark, entity, bronze_base):
    """Read bronze Parquet for a given entity, merging schemas."""
    path = f"{bronze_base}/{entity}"
    return spark.read.option("mergeSchema", "true").parquet(path)


# ── Per-entity processing ────────────────────────────────────────────────────

def process_orders(spark, bronze_base, silver_base, threshold):
    """Clean and write orders to silver."""
    print("\n" + "=" * 55)
    print("  orders")
    print("=" * 55)

    df = read_bronze(spark, "orders", bronze_base)
    total_raw = df.count()
    print(f"  Bronze rows:  {total_raw:,}")

    # Dedup
    df = deduplicate(df, "order_id")
    print(f"  After dedup:  {df.count():,}")

    # Quality gate (run on deduped data BEFORE cleaning fills nulls)
    assert_quality(df, QUALITY_CHECKS["orders"], "orders", threshold)

    # Clean
    df = clean_orders(df)
    df = drop_bronze_meta(df)
    df = add_silver_meta(df)

    # Select final columns in explicit order
    df = df.select(
        "order_id", "customer_id",
        "order_date", "order_timestamp",
        "status", "payment_method",
        "shipping_city", "shipping_state", "shipping_zip",
        "subtotal", "shipping_cost", "tax", "total",
        "order_year", "order_month",
        "_silver_processed_at",
    )

    out = f"{silver_base}/orders"
    df.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(out)
    print(f"  Written:      {out}")
    return df


def process_order_items(spark, bronze_base, silver_base, threshold):
    """Clean and write order items to silver."""
    print("\n" + "=" * 55)
    print("  order_items")
    print("=" * 55)

    df = read_bronze(spark, "order_items", bronze_base)
    total_raw = df.count()
    print(f"  Bronze rows:  {total_raw:,}")

    df = deduplicate(df, "order_item_id")
    print(f"  After dedup:  {df.count():,}")

    assert_quality(df, QUALITY_CHECKS["order_items"], "order_items", threshold)

    df = clean_order_items(df)
    df = drop_bronze_meta(df)
    df = add_silver_meta(df)

    df = df.select(
        "order_item_id", "order_id", "product_id",
        "quantity", "unit_price", "discount_pct", "line_total",
        "_silver_processed_at",
    )

    out = f"{silver_base}/order_items"
    df.write.mode("overwrite").parquet(out)
    print(f"  Written:      {out}")
    return df


def process_products(spark, bronze_base, silver_base, threshold):
    """Clean and write products to silver."""
    print("\n" + "=" * 55)
    print("  products")
    print("=" * 55)

    df = read_bronze(spark, "products", bronze_base)
    total_raw = df.count()
    print(f"  Bronze rows:  {total_raw:,}")

    df = deduplicate(df, "product_id")
    print(f"  After dedup:  {df.count():,}")

    assert_quality(df, QUALITY_CHECKS["products"], "products", threshold)

    df = clean_products(df)
    df = drop_bronze_meta(df)
    df = add_silver_meta(df)

    df = df.select(
        "product_id", "product_name",
        "category", "sub_category", "brand",
        "price", "cost", "weight_kg",
        "rating", "review_count",
        "is_active", "created_date",
        "_silver_processed_at",
    )

    out = f"{silver_base}/products"
    df.write.mode("overwrite").parquet(out)
    print(f"  Written:      {out}")
    return df


def process_customers(spark, bronze_base, silver_base, threshold):
    """Clean customers and merge into SCD Type 2 dimension."""
    print("\n" + "=" * 55)
    print("  customers (SCD Type 2)")
    print("=" * 55)

    df = read_bronze(spark, "customers", bronze_base)
    total_raw = df.count()
    print(f"  Bronze rows:  {total_raw:,}")

    df = deduplicate(df, "customer_id")
    deduped_count = df.count()
    print(f"  After dedup:  {deduped_count:,}")

    assert_quality(df, QUALITY_CHECKS["customers"], "customers", threshold)

    # Clean BEFORE SCD2 so hashes are computed on clean values
    df = clean_customers(df)
    df = drop_bronze_meta(df)

    # SCD Type 2 merge
    silver_path = f"{silver_base}/customers"
    result = build_customer_scd2(spark, df, silver_path)
    result = add_silver_meta(result)

    # Count current vs historical
    current_count = result.filter(F.col("is_current")).count()
    hist_count = result.filter(~F.col("is_current")).count()
    print(f"  SCD2 current:  {current_count:,}")
    print(f"  SCD2 history:  {hist_count:,}")

    result.write.mode("overwrite").parquet(silver_path)
    print(f"  Written:       {silver_path}")
    return result


# ── Entity dispatcher ─────────────────────────────────────────────────────────

PROCESSORS = {
    "customers": process_customers,
    "products": process_products,
    "orders": process_orders,
    "order_items": process_order_items,
}


# ── CLI & main ────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Silver cleaning: bronze Parquet -> cleaned Parquet",
    )
    p.add_argument(
        "--bronze-base",
        default=None,
        help=(
            f"Bronze input base path (default: s3a://{BRONZE_BUCKET}).  "
            "Use a local path for offline testing."
        ),
    )
    p.add_argument(
        "--silver-base",
        default=None,
        help=(
            f"Silver output base path (default: s3a://{SILVER_BUCKET}).  "
            "Use a local path for offline testing."
        ),
    )
    p.add_argument(
        "--entities",
        nargs="+",
        default=list(PROCESSORS.keys()),
        choices=list(PROCESSORS.keys()),
        help="Entities to process (default: all)",
    )
    p.add_argument(
        "--null-threshold",
        type=float,
        default=None,
        help=(
            f"Override max null %% for all quality checks "
            f"(default: per-column, typically {DEFAULT_NULL_THRESHOLD}%%)"
        ),
    )
    return p.parse_args()


def main():
    args = parse_args()

    bronze_base = args.bronze_base or f"s3a://{BRONZE_BUCKET}"
    silver_base = args.silver_base or f"s3a://{SILVER_BUCKET}"

    print("=" * 60)
    print("  SILVER CLEANING JOB")
    print("=" * 60)
    print(f"  Bronze:    {bronze_base}")
    print(f"  Silver:    {silver_base}")
    print(f"  Entities:  {', '.join(args.entities)}")
    threshold_label = (
        f"{args.null_threshold}% (override)"
        if args.null_threshold is not None
        else "per-column defaults"
    )
    print(f"  Null gate: {threshold_label}")
    print(f"  Time:      {datetime.utcnow().isoformat()}Z")

    spark = create_spark_session()

    results = {}
    for name in args.entities:
        processor = PROCESSORS[name]
        try:
            processor(spark, bronze_base, silver_base, args.null_threshold)
            results[name] = "OK"
        except DataQualityError as e:
            print(f"\n  QUALITY GATE FAILED:\n{e}")
            results[name] = f"QUALITY FAIL: {e}"
        except AnalysisException as e:
            print(f"\n  ERROR [{name}]: {e}")
            results[name] = f"ERROR: {e}"

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  SILVER CLEANING SUMMARY")
    print("=" * 60)
    all_ok = True
    for name, status in results.items():
        flag = "OK" if status == "OK" else "FAIL"
        if flag == "FAIL":
            all_ok = False
        print(f"  {name:15s}  [{flag}]  {status}")
    print("=" * 60)

    spark.stop()

    if not all_ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
