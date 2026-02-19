"""Generate synthetic e-commerce data and upload to the bronze S3 layer."""

import json
import os
import random
from datetime import datetime, timedelta

import boto3
from faker import Faker

fake = Faker()

S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
BRONZE_BUCKET = "bronze-layer"
NUM_CUSTOMERS = 100
NUM_PRODUCTS = 50
NUM_ORDERS = 500


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def generate_customers(n):
    return [
        {
            "customer_id": i,
            "name": fake.name(),
            "email": fake.email(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "created_at": fake.date_between(start_date="-2y").isoformat(),
        }
        for i in range(1, n + 1)
    ]


def generate_products(n):
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Toys"]
    return [
        {
            "product_id": i,
            "name": fake.catch_phrase(),
            "category": random.choice(categories),
            "price": round(random.uniform(5.0, 500.0), 2),
        }
        for i in range(1, n + 1)
    ]


def generate_orders(n, num_customers, num_products):
    statuses = ["completed", "pending", "cancelled", "returned"]
    orders = []
    base_date = datetime.now() - timedelta(days=365)
    for i in range(1, n + 1):
        order_date = base_date + timedelta(days=random.randint(0, 365))
        num_items = random.randint(1, 5)
        items = [
            {
                "product_id": random.randint(1, num_products),
                "quantity": random.randint(1, 3),
            }
            for _ in range(num_items)
        ]
        orders.append(
            {
                "order_id": i,
                "customer_id": random.randint(1, num_customers),
                "order_date": order_date.strftime("%Y-%m-%d"),
                "status": random.choice(statuses),
                "items": items,
            }
        )
    return orders


def upload_to_s3(s3, data, key):
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json",
    )
    print(f"Uploaded {key} ({len(data)} records)")


def main():
    s3 = get_s3_client()

    customers = generate_customers(NUM_CUSTOMERS)
    products = generate_products(NUM_PRODUCTS)
    orders = generate_orders(NUM_ORDERS, NUM_CUSTOMERS, NUM_PRODUCTS)

    upload_to_s3(s3, customers, "customers/customers.json")
    upload_to_s3(s3, products, "products/products.json")
    upload_to_s3(s3, orders, "orders/orders.json")

    print("Data generation complete.")


if __name__ == "__main__":
    main()
