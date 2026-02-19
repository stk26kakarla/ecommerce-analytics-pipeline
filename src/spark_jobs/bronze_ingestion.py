#!/usr/bin/env python3
"""PySpark Bronze Ingestion Job

Reads raw JSON from local filesystem or S3 (via LocalStack), validates
schemas, adds ingestion metadata, and writes partitioned Parquet to the
bronze S3 layer.

Features:
    - Schema validation with quarantine for corrupt / incomplete records
    - Ingestion metadata: _ingested_at, _source_file, _batch_id
    - Orders partitioned by order_year / order_month
    - Append mode for idempotent handling of late-arriving data
    - mergeSchema for forward-compatible schema evolution
    - Late-arriving data flagged with _is_late for downstream monitoring

Late-arriving data strategy:
    Every write uses Spark's *append* mode so new records land alongside
    existing partitions without overwriting them.  Each record carries a
    _batch_id, letting the silver layer de-duplicate across batches.
    Records whose order_date is more than 7 days before ingestion time
    are flagged _is_late = true for monitoring.

Schema evolution strategy:
    Source JSON is read with schema *inference* (not a fixed StructType)
    so that new columns added upstream are preserved automatically.
    Known columns are cast to expected types; unknown columns pass
    through as-is.  Parquet writes use mergeSchema = true so the target
    dataset absorbs new columns without breaking readers of the old schema.

Usage:
    # Ingest everything from local data/raw/
    spark-submit src/spark_jobs/bronze_ingestion.py

    # Ingest from S3 via LocalStack
    spark-submit src/spark_jobs/bronze_ingestion.py --source s3

    # Ingest only orders and order_items
    spark-submit src/spark_jobs/bronze_ingestion.py --entities orders order_items

    # Custom batch ID (for replay / backfill)
    spark-submit src/spark_jobs/bronze_ingestion.py --batch-id backfill-2024-01
"""

import argparse
import os
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
)


# ── Environment ───────────────────────────────────────────────────────────────

S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
BRONZE_BUCKET = "bronze-layer"
LOCAL_RAW_DIR = os.environ.get("RAW_DATA_DIR", "data/raw")
LATE_ARRIVAL_DAYS = 7  # orders older than this at ingestion time are flagged


# ── Expected column types ─────────────────────────────────────────────────────
# Dicts of {column_name: SparkType}.  Only columns listed here are cast;
# any *extra* columns found in the source are kept with their inferred types
# (schema evolution).

CUSTOMERS_TYPES = {
    "customer_id": IntegerType(),
    "first_name": StringType(),
    "last_name": StringType(),
    "email": StringType(),
    "phone": StringType(),
    "address": StringType(),
    "city": StringType(),
    "state": StringType(),
    "zip_code": StringType(),
    "country": StringType(),
    "registration_date": StringType(),
    "birth_date": StringType(),
    "gender": StringType(),
    "income_bracket": StringType(),
    "is_churned": BooleanType(),
    "churn_date": StringType(),
}

PRODUCTS_TYPES = {
    "product_id": IntegerType(),
    "product_name": StringType(),
    "category": StringType(),
    "sub_category": StringType(),
    "brand": StringType(),
    "price": DoubleType(),
    "cost": DoubleType(),
    "weight_kg": DoubleType(),
    "rating": DoubleType(),
    "review_count": IntegerType(),
    "is_active": BooleanType(),
    "created_date": StringType(),
}

ORDERS_TYPES = {
    "order_id": IntegerType(),
    "customer_id": IntegerType(),
    "order_date": StringType(),
    "order_timestamp": StringType(),
    "status": StringType(),
    "payment_method": StringType(),
    "shipping_city": StringType(),
    "shipping_state": StringType(),
    "shipping_zip": StringType(),
    "subtotal": DoubleType(),
    "shipping_cost": DoubleType(),
    "tax": DoubleType(),
    "total": DoubleType(),
}

ORDER_ITEMS_TYPES = {
    "order_item_id": IntegerType(),
    "order_id": IntegerType(),
    "product_id": IntegerType(),
    "quantity": IntegerType(),
    "unit_price": DoubleType(),
    "discount_pct": DoubleType(),
    "line_total": DoubleType(),
}


# ── Entity registry ──────────────────────────────────────────────────────────

ENTITIES = {
    "customers": {
        "expected_types": CUSTOMERS_TYPES,
        "required_cols": ["customer_id", "email"],
        "partition_by": None,
        "date_col": None,
        "file": "customers.json",
    },
    "products": {
        "expected_types": PRODUCTS_TYPES,
        "required_cols": ["product_id", "price"],
        "partition_by": None,
        "date_col": None,
        "file": "products.json",
    },
    "orders": {
        "expected_types": ORDERS_TYPES,
        "required_cols": ["order_id", "customer_id", "order_date"],
        "partition_by": ["order_year", "order_month"],
        "date_col": "order_date",
        "file": "orders.json",
    },
    "order_items": {
        "expected_types": ORDER_ITEMS_TYPES,
        "required_cols": ["order_item_id", "order_id", "product_id"],
        "partition_by": None,
        "date_col": None,
        "file": "order_items.json",
    },
}


# ── Spark session ─────────────────────────────────────────────────────────────

def create_spark_session():
    """Build a SparkSession wired to LocalStack S3 with merge-schema on."""
    return (
        SparkSession.builder
        .appName("BronzeIngestion")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# ── Core helpers ──────────────────────────────────────────────────────────────

def add_metadata(df, source_file, batch_id):
    """Append _ingested_at, _source_file, and _batch_id columns."""
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def cast_known_columns(df, expected_types):
    """Cast columns present in both the DataFrame and the type map.

    Columns in the source but *not* in expected_types pass through
    unchanged -- this is how schema evolution works.  A cast failure
    (e.g. "abc" -> IntegerType) produces NULL, which the required-column
    check will catch downstream.
    """
    for col_name, col_type in expected_types.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
    return df


def validate_required(df, required_cols):
    """Split a DataFrame into (valid, quarantine) based on required columns.

    A row is quarantined when any required column is NULL after type casting
    (NULL can result from missing data *or* from a failed cast).
    """
    condition = F.lit(True)
    for col_name in required_cols:
        if col_name in df.columns:
            condition = condition & F.col(col_name).isNotNull()
        else:
            # Entire required column missing from source -- quarantine all
            condition = F.lit(False)
            break

    valid = df.filter(condition)
    quarantine = df.filter(~condition)
    return valid, quarantine


def add_partition_columns(df, date_col):
    """Derive order_year / order_month from a date-string column."""
    parsed = F.to_date(F.col(date_col))
    return (
        df
        .withColumn("order_year", F.year(parsed))
        .withColumn("order_month", F.month(parsed))
    )


def flag_late_arrivals(df, date_col, threshold_days=LATE_ARRIVAL_DAYS):
    """Add _is_late = true when the record's date is older than threshold."""
    age = F.datediff(F.current_date(), F.to_date(F.col(date_col)))
    return df.withColumn("_is_late", age > threshold_days)


# ── Per-entity ingestion ──────────────────────────────────────────────────────

def ingest_entity(spark, name, config, source_path, output_base, batch_id):
    """Read one entity's JSON, validate, enrich, and write bronze Parquet.

    Returns (total_count, valid_count, quarantine_count).
    """
    print(f"\n{'='*50}")
    print(f"  Entity: {name}")
    print(f"  Source: {source_path}")
    print(f"{'='*50}")

    # ── Read ──────────────────────────────────────────────────────────────
    # multiLine=True because our generator writes JSON arrays ([ ... ]).
    # Schema is *inferred* so that extra columns are preserved (evolution).
    raw = spark.read.option("multiLine", "true").json(source_path)

    # Cache to avoid re-reading for valid + quarantine splits
    raw_count = raw.count()
    if raw_count == 0:
        print("  SKIP: source file is empty")
        return 0, 0, 0
    print(f"  Raw records read: {raw_count:,}")

    # ── Schema validation ─────────────────────────────────────────────────
    casted = cast_known_columns(raw, config["expected_types"])
    valid, quarantine = validate_required(
        casted, config["required_cols"],
    )

    # Cache quarantine to get its count without re-scanning
    quarantine.cache()
    quarantine_count = quarantine.count()
    valid_count = raw_count - quarantine_count

    # ── Enrich valid records ──────────────────────────────────────────────
    if config["date_col"]:
        valid = add_partition_columns(valid, config["date_col"])
        valid = flag_late_arrivals(valid, config["date_col"])

    valid = add_metadata(valid, config["file"], batch_id)

    # ── Write valid -> bronze ─────────────────────────────────────────────
    output_path = f"{output_base}/{name}"
    writer = (
        valid.write
        .mode("append")
        .option("mergeSchema", "true")
    )
    if config["partition_by"]:
        writer = writer.partitionBy(*config["partition_by"])
    writer.parquet(output_path)
    print(f"  Valid:       {valid_count:,}  ->  {output_path}")

    # ── Write quarantine (if any) ─────────────────────────────────────────
    if quarantine_count > 0:
        quarantine_enriched = add_metadata(
            quarantine, config["file"], batch_id,
        )
        q_path = f"{output_base}/_quarantine/{name}"
        quarantine_enriched.write.mode("append").parquet(q_path)
        print(f"  Quarantined: {quarantine_count:,}  ->  {q_path}")
    else:
        print(f"  Quarantined: 0")

    quarantine.unpersist()
    return raw_count, valid_count, quarantine_count


# ── CLI & main ────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Bronze ingestion: raw JSON -> validated Parquet",
    )
    p.add_argument(
        "--source",
        choices=["local", "s3"],
        default="local",
        help="Read from local filesystem or S3 (default: local)",
    )
    p.add_argument(
        "--local-dir",
        default=LOCAL_RAW_DIR,
        help=f"Local raw-data directory (default: {LOCAL_RAW_DIR})",
    )
    p.add_argument(
        "--s3-input-prefix",
        default="raw",
        help="S3 key prefix under bronze-layer for raw JSON (default: raw)",
    )
    p.add_argument(
        "--output-base",
        default=None,
        help=(
            "Output base path (default: s3a://bronze-layer).  "
            "Set to a local path like data/bronze for offline testing."
        ),
    )
    p.add_argument(
        "--entities",
        nargs="+",
        default=list(ENTITIES.keys()),
        choices=list(ENTITIES.keys()),
        help="Entities to ingest (default: all)",
    )
    p.add_argument(
        "--batch-id",
        default=None,
        help="Batch identifier (default: auto-generated UUID)",
    )
    return p.parse_args()


def main():
    args = parse_args()

    batch_id = args.batch_id or str(uuid.uuid4())
    output_base = args.output_base or f"s3a://{BRONZE_BUCKET}"

    print("=" * 60)
    print("  BRONZE INGESTION JOB")
    print("=" * 60)
    print(f"  Batch ID : {batch_id}")
    print(f"  Source   : {args.source}")
    print(f"  Entities : {', '.join(args.entities)}")
    print(f"  Output   : {output_base}")
    print(f"  Time     : {datetime.utcnow().isoformat()}Z")

    spark = create_spark_session()

    results = {}
    for name in args.entities:
        config = ENTITIES[name]

        # Resolve source path
        if args.source == "local":
            source_path = os.path.join(args.local_dir, config["file"])
        else:
            source_path = (
                f"s3a://{BRONZE_BUCKET}/"
                f"{args.s3_input_prefix}/{config['file']}"
            )

        try:
            total, valid, quarantined = ingest_entity(
                spark, name, config, source_path, output_base, batch_id,
            )
            results[name] = {
                "total": total,
                "valid": valid,
                "quarantined": quarantined,
            }
        except Exception as e:
            print(f"\n  ERROR [{name}]: {e}")
            results[name] = {"error": str(e)}

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  INGESTION SUMMARY")
    print("=" * 60)
    for name, stats in results.items():
        if "error" in stats:
            print(f"  {name:15s}  ERROR: {stats['error']}")
        else:
            print(
                f"  {name:15s}  "
                f"total={stats['total']:>9,}  "
                f"valid={stats['valid']:>9,}  "
                f"quarantined={stats['quarantined']:>6,}"
            )
    print(f"\n  Batch ID : {batch_id}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
