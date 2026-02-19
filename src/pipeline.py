"""E-commerce analytics pipeline: Bronze -> Silver -> Gold layers."""

import json
import os

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "analytics")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "analytics_pass")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ecommerce")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def get_spark():
    return (
        SparkSession.builder.appName("EcommerceAnalytics")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def read_json_from_s3(s3, bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def bronze_to_silver(spark, s3):
    """Read raw JSON from bronze, clean and structure into silver as Parquet."""
    print("--- Bronze to Silver ---")

    # Customers
    customers_raw = read_json_from_s3(s3, "bronze-layer", "customers/customers.json")
    customers_df = spark.createDataFrame(customers_raw)
    customers_clean = customers_df.dropDuplicates(["customer_id"]).filter(
        F.col("email").isNotNull()
    )
    customers_clean.write.mode("overwrite").parquet("s3a://silver-layer/customers/")
    print(f"  Silver customers: {customers_clean.count()} rows")

    # Products
    products_raw = read_json_from_s3(s3, "bronze-layer", "products/products.json")
    products_df = spark.createDataFrame(products_raw)
    products_clean = products_df.dropDuplicates(["product_id"]).filter(
        F.col("price") > 0
    )
    products_clean.write.mode("overwrite").parquet("s3a://silver-layer/products/")
    print(f"  Silver products: {products_clean.count()} rows")

    # Orders — flatten items
    orders_raw = read_json_from_s3(s3, "bronze-layer", "orders/orders.json")
    orders_df = spark.createDataFrame(orders_raw)
    orders_exploded = orders_df.withColumn("item", F.explode("items")).select(
        "order_id",
        "customer_id",
        "order_date",
        "status",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").alias("quantity"),
    )
    orders_exploded.write.mode("overwrite").parquet("s3a://silver-layer/orders/")
    print(f"  Silver order lines: {orders_exploded.count()} rows")

    return customers_clean, products_clean, orders_exploded


def silver_to_gold(spark):
    """Aggregate silver data into gold-layer analytics tables."""
    print("--- Silver to Gold ---")

    orders = spark.read.parquet("s3a://silver-layer/orders/")
    products = spark.read.parquet("s3a://silver-layer/products/")
    customers = spark.read.parquet("s3a://silver-layer/customers/")

    # Revenue by product category
    revenue_by_category = (
        orders.join(products, "product_id")
        .withColumn("line_total", F.col("quantity") * F.col("price"))
        .groupBy("category")
        .agg(
            F.sum("line_total").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
        )
    )
    revenue_by_category.write.mode("overwrite").parquet(
        "s3a://gold-layer/revenue_by_category/"
    )
    print(f"  Gold revenue_by_category: {revenue_by_category.count()} rows")

    # Customer order summary
    customer_summary = (
        orders.groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("num_orders"),
            F.sum("quantity").alias("total_items"),
        )
        .join(customers, "customer_id")
        .select("customer_id", "name", "city", "state", "num_orders", "total_items")
    )
    customer_summary.write.mode("overwrite").parquet(
        "s3a://gold-layer/customer_summary/"
    )
    print(f"  Gold customer_summary: {customer_summary.count()} rows")

    return revenue_by_category, customer_summary


def main():
    spark = get_spark()
    s3 = get_s3_client()

    bronze_to_silver(spark, s3)
    silver_to_gold(spark)

    print("Pipeline complete.")
    spark.stop()


if __name__ == "__main__":
    main()
