"""Tests for data generation module."""

from src.generate_data import generate_customers, generate_orders, generate_products


def test_generate_customers_count():
    customers = generate_customers(10)
    assert len(customers) == 10


def test_generate_customers_fields():
    customers = generate_customers(1)
    c = customers[0]
    assert "customer_id" in c
    assert "name" in c
    assert "email" in c
    assert c["customer_id"] == 1


def test_generate_products_count():
    products = generate_products(5)
    assert len(products) == 5


def test_generate_products_fields():
    products = generate_products(1)
    p = products[0]
    assert "product_id" in p
    assert "price" in p
    assert p["price"] > 0


def test_generate_orders_count():
    orders = generate_orders(20, num_customers=10, num_products=5)
    assert len(orders) == 20


def test_generate_orders_valid_references():
    orders = generate_orders(50, num_customers=10, num_products=5)
    for o in orders:
        assert 1 <= o["customer_id"] <= 10
        for item in o["items"]:
            assert 1 <= item["product_id"] <= 5
            assert item["quantity"] >= 1
