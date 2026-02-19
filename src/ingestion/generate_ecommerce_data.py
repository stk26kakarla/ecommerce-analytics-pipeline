#!/usr/bin/env python3
"""Generate realistic e-commerce data with seasonal patterns, customer churn,
and product category distributions.

Scale (full mode):
    50,000 customers · 5,000 products · 500,000 orders · ~1.2 M line items
    2-year window  2023-01-01 to 2024-12-31

Realistic patterns:
    - Seasonal spikes: Black Friday, Christmas, Valentine's Day, Back-to-School
    - Customer churn: ~22 % exponential-decay churn rate
    - Product popularity: Pareto / power-law (80/20 rule)
    - Category-specific seasonal boosts (Toys spike in Dec, etc.)

Outputs (full mode):
    data/raw/customers.json, products.json, orders.json, order_items.json
    data/raw/samples/customers_sample.csv  (10 K rows)
    data/raw/samples/products_sample.csv   (up to 10 K rows)
    data/raw/samples/orders_sample.csv     (10 K rows)
    data/raw/samples/order_items_sample.csv(10 K rows)

Usage:
    python -m src.ingestion.generate_ecommerce_data               # full
    python -m src.ingestion.generate_ecommerce_data --samples-only # fast
"""

import argparse
import bisect
import csv
import itertools
import json
import os
import random
import time
from datetime import datetime, timedelta

from faker import Faker

# ── Configuration ─────────────────────────────────────────────────────────────

SEED = 42
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)
TOTAL_DAYS = (END_DATE - START_DATE).days + 1  # 731

FULL = {"customers": 50_000, "products": 5_000, "orders": 500_000}
SAMPLE = {"customers": 12_000, "products": 5_000, "orders": 35_000}
CSV_SAMPLE_ROWS = 10_000

CHURN_RATE = 0.22

CATEGORIES = {
    "Electronics": {
        "subs": [
            "Smartphones", "Laptops", "Tablets",
            "Headphones", "Cameras", "Smart Home",
        ],
        "share": 0.12,
        "price": (29.99, 1999.99),
        "cost_ratio": (0.55, 0.75),
        "popularity": 1.8,
        "seasonal": {11: 1.5, 12: 1.8},
    },
    "Clothing": {
        "subs": [
            "Men's Wear", "Women's Wear", "Kids' Clothing",
            "Shoes", "Accessories",
        ],
        "share": 0.22,
        "price": (9.99, 249.99),
        "cost_ratio": (0.35, 0.55),
        "popularity": 2.5,
        "seasonal": {8: 1.3, 11: 2.0, 12: 2.5},
    },
    "Books": {
        "subs": [
            "Fiction", "Non-Fiction", "Textbooks",
            "Children's", "Comics & Graphic Novels",
        ],
        "share": 0.15,
        "price": (4.99, 79.99),
        "cost_ratio": (0.40, 0.60),
        "popularity": 1.5,
        "seasonal": {9: 1.3, 12: 1.4},
    },
    "Home & Garden": {
        "subs": [
            "Furniture", "Kitchen", "Bedding",
            "Decor", "Garden Tools",
        ],
        "share": 0.14,
        "price": (12.99, 899.99),
        "cost_ratio": (0.45, 0.65),
        "popularity": 1.2,
        "seasonal": {4: 1.2, 5: 1.3, 6: 1.2},
    },
    "Sports & Outdoors": {
        "subs": [
            "Exercise Equipment", "Camping & Hiking", "Team Sports",
            "Water Sports", "Cycling",
        ],
        "share": 0.10,
        "price": (14.99, 599.99),
        "cost_ratio": (0.50, 0.70),
        "popularity": 0.9,
        "seasonal": {1: 1.4, 5: 1.2, 6: 1.3, 7: 1.3},
    },
    "Beauty & Personal Care": {
        "subs": [
            "Skincare", "Makeup", "Hair Care",
            "Fragrance", "Bath & Body",
        ],
        "share": 0.10,
        "price": (5.99, 149.99),
        "cost_ratio": (0.25, 0.45),
        "popularity": 1.6,
        "seasonal": {2: 1.5, 5: 1.4, 12: 1.6},
    },
    "Toys & Games": {
        "subs": [
            "Action Figures", "Board Games", "Dolls",
            "Educational", "Outdoor Play",
        ],
        "share": 0.08,
        "price": (7.99, 199.99),
        "cost_ratio": (0.40, 0.60),
        "popularity": 0.8,
        "seasonal": {11: 2.5, 12: 3.5},
    },
    "Food & Gourmet": {
        "subs": [
            "Snacks", "Beverages", "Organic",
            "Pantry Staples", "Gourmet & Specialty",
        ],
        "share": 0.09,
        "price": (2.99, 59.99),
        "cost_ratio": (0.55, 0.75),
        "popularity": 1.2,
        "seasonal": {11: 1.3, 12: 1.5},
    },
}

PAYMENT_METHODS = [
    "credit_card", "debit_card", "paypal",
    "apple_pay", "google_pay", "gift_card",
]
PAYMENT_WEIGHTS = [40, 20, 20, 10, 7, 3]

STATUSES = [
    "completed", "shipped", "delivered",
    "cancelled", "returned", "refunded", "pending",
]
STATUS_WEIGHTS = [35, 15, 25, 10, 8, 4, 3]

INCOME_BRACKETS = ["low", "medium", "high", "premium"]
INCOME_WEIGHTS = [25, 45, 22, 8]

ITEMS_PER_ORDER = [1, 2, 3, 4, 5]
ITEMS_WEIGHTS = [35, 30, 20, 10, 5]

# Hourly shopping distribution (peak in evenings, low overnight)
HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2,   # 00-05
    3, 5, 7, 8, 9, 10,  # 06-11
    10, 9, 8, 7, 8, 9,  # 12-17
    10, 10, 8, 5, 3, 2,  # 18-23
]

# Quantity distribution: mostly 1, occasionally 2-3
QTY_POOL = [1, 1, 1, 2, 2, 3]

# Discount distribution: ~54 % no discount, rest 5-30 %
DISCOUNT_POOL = [0, 0, 0, 0, 0, 0, 0, 5, 10, 15, 20, 25, 30]


# ── Helpers ───────────────────────────────────────────────────────────────────

def seasonal_multiplier(dt):
    """Return order-volume multiplier for a given calendar date."""
    m, d = dt.month, dt.day

    base = {
        1: 0.70, 2: 0.85, 3: 0.90, 4: 0.95,
        5: 1.00, 6: 0.95, 7: 1.05, 8: 1.00,
        9: 0.95, 10: 1.10, 11: 1.80, 12: 2.00,
    }[m]

    # ── Event-level spikes ──
    if m == 11 and 20 <= d <= 30:        # Black Friday week
        base *= 2.8
    elif m == 12 and 1 <= d <= 3:       # Cyber Monday
        base *= 2.0
    elif m == 12 and 4 <= d <= 20:      # Christmas rush
        base *= 1.6
    elif m == 12 and d > 25:            # Post-Christmas lull
        base *= 0.3
    elif m == 2 and 10 <= d <= 14:      # Valentine's Day
        base *= 1.6
    elif m == 5 and 5 <= d <= 12:       # Mother's Day
        base *= 1.3
    elif (m == 8 and d >= 15) or (m == 9 and d <= 5):  # Back-to-School
        base *= 1.2
    elif m == 7 and 10 <= d <= 12:      # Prime Day
        base *= 1.8

    # Weekend boost
    if dt.weekday() >= 5:
        base *= 1.15

    return base


def _weighted_pick(cum_weights, total, population):
    """Pick one item using pre-computed cumulative weights (bisect)."""
    idx = bisect.bisect_left(cum_weights, random.random() * total)
    return min(idx, len(population) - 1)


def strip_internal(records):
    """Remove fields prefixed with '_'."""
    return [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]


def write_json(path, records):
    """Stream-write records as a JSON array (compact, one object per line)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, rec in enumerate(records):
            if i:
                f.write(",\n")
            json.dump(rec, f, default=str)
        f.write("\n]\n")
    size_mb = os.path.getsize(path) / 1_048_576
    print(f"  {path}  ({len(records):,} records, {size_mb:.1f} MB)")


def write_csv(path, records, max_rows=None):
    """Write records to CSV, optionally capped at max_rows."""
    if not records:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    subset = records[:max_rows] if max_rows else records
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=subset[0].keys())
        writer.writeheader()
        writer.writerows(subset)
    size_kb = os.path.getsize(path) / 1024
    print(f"  {path}  ({len(subset):,} rows, {size_kb:.1f} KB)")


# ── Generators ────────────────────────────────────────────────────────────────

def generate_customers(n, fake):
    """Generate customers with registration dates, churn modeling, and demographics."""
    print(f"Generating {n:,} customers ...")
    t0 = time.time()
    customers = []

    for i in range(1, n + 1):
        # Registration skewed toward start of window (beta distribution)
        reg_offset = int(random.betavariate(2, 5) * TOTAL_DAYS)
        reg_date = START_DATE + timedelta(days=reg_offset)

        # Churn modeling — exponential decay, mean 180 days active
        is_churned = random.random() < CHURN_RATE
        churn_date = None
        if is_churned:
            days_active = max(30, int(random.expovariate(1 / 180)))
            churn_dt = reg_date + timedelta(days=days_active)
            if churn_dt < END_DATE:
                churn_date = churn_dt.strftime("%Y-%m-%d")
            else:
                is_churned = False

        gender = random.choices(["M", "F", "Other"], weights=[48, 48, 4], k=1)[0]
        if gender == "M":
            first = fake.first_name_male()
        elif gender == "F":
            first = fake.first_name_female()
        else:
            first = fake.first_name()

        customers.append({
            "customer_id": i,
            "first_name": first,
            "last_name": fake.last_name(),
            "email": fake.ascii_email(),
            "phone": fake.phone_number(),
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "country": "US",
            "registration_date": reg_date.strftime("%Y-%m-%d"),
            "birth_date": fake.date_of_birth(
                minimum_age=18, maximum_age=80,
            ).strftime("%Y-%m-%d"),
            "gender": gender,
            "income_bracket": random.choices(
                INCOME_BRACKETS, weights=INCOME_WEIGHTS, k=1,
            )[0],
            "is_churned": is_churned,
            "churn_date": churn_date,
            # internal fields (stripped before output)
            "_reg_dt": reg_date,
            "_end_dt": (
                datetime.strptime(churn_date, "%Y-%m-%d")
                if churn_date
                else END_DATE + timedelta(days=1)
            ),
            "_weight": random.paretovariate(1.2),  # purchase propensity
        })

        if i % 10_000 == 0:
            print(f"    {i:,} / {n:,}")

    print(f"    done in {time.time() - t0:.1f}s")
    return customers


def generate_products(n, fake):
    """Generate products across categories with Pareto-distributed popularity."""
    print(f"Generating {n:,} products ...")
    t0 = time.time()
    products = []

    cat_names = list(CATEGORIES.keys())
    cat_shares = [CATEGORIES[c]["share"] for c in cat_names]

    for i in range(1, n + 1):
        cat = random.choices(cat_names, weights=cat_shares, k=1)[0]
        cfg = CATEGORIES[cat]
        sub = random.choice(cfg["subs"])

        lo, hi = cfg["price"]
        price = round(random.uniform(lo, hi), 2)
        cr_lo, cr_hi = cfg["cost_ratio"]
        cost = round(price * random.uniform(cr_lo, cr_hi), 2)

        popularity = random.paretovariate(1.5)

        products.append({
            "product_id": i,
            "product_name": fake.catch_phrase(),
            "category": cat,
            "sub_category": sub,
            "brand": fake.company(),
            "price": price,
            "cost": cost,
            "weight_kg": round(random.uniform(0.1, 25.0), 2),
            "rating": round(min(5.0, max(1.0, random.gauss(3.8, 0.8))), 1),
            "review_count": max(0, int(random.gauss(popularity * 50, 30))),
            "is_active": random.random() > 0.05,
            "created_date": (
                START_DATE - timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d"),
            # internal
            "_popularity": popularity,
        })

    print(f"    done in {time.time() - t0:.1f}s")
    return products


def generate_orders(n, customers, products):
    """Generate orders with seasonal weighting and line items.

    Uses pre-computed cumulative weights + bisect for O(log N) sampling
    instead of rebuilding weights on every call.
    """
    print(f"Generating {n:,} orders ...")
    t0 = time.time()

    # ── Pre-compute date weights and bulk-pick all order dates ──
    dates = [START_DATE + timedelta(days=d) for d in range(TOTAL_DAYS)]
    date_weights = [seasonal_multiplier(d) for d in dates]
    order_dates = random.choices(dates, weights=date_weights, k=n)

    # ── Pre-compute base product weights ──
    base_pw = []
    for p in products:
        cat_pop = CATEGORIES[p["category"]]["popularity"]
        w = p["_popularity"] * cat_pop
        if not p["is_active"]:
            w *= 0.01
        base_pw.append(w)

    # ── Pre-compute per-month cumulative product weights ──
    monthly_cum = {}
    monthly_total = {}
    for m in range(1, 13):
        weights = []
        for j, p in enumerate(products):
            boost = CATEGORIES[p["category"]].get("seasonal", {}).get(m, 1.0)
            weights.append(base_pw[j] * boost)
        monthly_cum[m] = list(itertools.accumulate(weights))
        monthly_total[m] = monthly_cum[m][-1]

    # ── Pre-compute customer cumulative weights ──
    cust_weights = [c["_weight"] for c in customers]
    cust_cum = list(itertools.accumulate(cust_weights))
    cust_total = cust_cum[-1]

    orders = []
    order_items = []
    item_id = 1

    for oid in range(n):
        order_id = oid + 1
        dt = order_dates[oid]

        # Pick an active customer (rejection-sample, ≤15 tries)
        cust = None
        for _ in range(15):
            cidx = _weighted_pick(cust_cum, cust_total, customers)
            c = customers[cidx]
            if c["_reg_dt"] <= dt < c["_end_dt"]:
                cust = c
                break
        if cust is None:
            cust = c  # fallback: use last candidate

        # Pick products for this order (category-seasonal boosted)
        month = dt.month
        cum = monthly_cum[month]
        tot = monthly_total[month]
        num_items = random.choices(ITEMS_PER_ORDER, weights=ITEMS_WEIGHTS, k=1)[0]

        subtotal = 0.0
        items_buf = []
        for _ in range(num_items):
            pidx = _weighted_pick(cum, tot, products)
            prod = products[pidx]

            qty = random.choice(QTY_POOL)
            disc = random.choice(DISCOUNT_POOL)
            line = round(qty * prod["price"] * (1 - disc / 100), 2)
            subtotal += line

            items_buf.append({
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": prod["price"],
                "discount_pct": float(disc),
                "line_total": line,
            })
            item_id += 1

        order_items.extend(items_buf)

        subtotal = round(subtotal, 2)
        shipping = round(
            0.0 if subtotal >= 50 else random.uniform(3.99, 9.99), 2,
        )
        tax = round(subtotal * random.uniform(0.05, 0.10), 2)
        total = round(subtotal + shipping + tax, 2)

        hour = random.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]
        ts = dt.replace(
            hour=hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        )

        orders.append({
            "order_id": order_id,
            "customer_id": cust["customer_id"],
            "order_date": dt.strftime("%Y-%m-%d"),
            "order_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "status": random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
            "payment_method": random.choices(
                PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1,
            )[0],
            "shipping_city": cust["city"],
            "shipping_state": cust["state"],
            "shipping_zip": cust["zip_code"],
            "subtotal": subtotal,
            "shipping_cost": shipping,
            "tax": tax,
            "total": total,
        })

        if order_id % 50_000 == 0:
            elapsed = time.time() - t0
            rate = order_id / elapsed
            eta = (n - order_id) / rate
            print(f"    {order_id:,} / {n:,}  ({rate:,.0f} orders/s, ETA {eta:.0f}s)")

    print(
        f"    done in {time.time() - t0:.1f}s  "
        f"— {len(order_items):,} line items"
    )
    return orders, order_items


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic e-commerce data",
    )
    parser.add_argument(
        "--samples-only",
        action="store_true",
        help="Generate a smaller dataset and only write sample CSVs (fast)",
    )
    parser.add_argument(
        "--output-dir",
        default="data/raw",
        help="Root output directory (default: data/raw)",
    )
    args = parser.parse_args()

    random.seed(SEED)
    Faker.seed(SEED)
    fake = Faker("en_US")

    counts = SAMPLE if args.samples_only else FULL
    out = args.output_dir
    samples_dir = os.path.join(out, "samples")

    mode = "samples-only" if args.samples_only else "full"
    print(f"Mode:  {mode}")
    print(
        f"Scale: {counts['customers']:,} customers, "
        f"{counts['products']:,} products, "
        f"{counts['orders']:,} orders"
    )
    print(f"Window: {START_DATE.date()} to {END_DATE.date()}")
    print()

    # ── Generate ──
    customers = generate_customers(counts["customers"], fake)
    products = generate_products(counts["products"], fake)
    orders, order_items = generate_orders(counts["orders"], customers, products)

    customers_out = strip_internal(customers)
    products_out = strip_internal(products)

    # ── Write JSON (full mode only) ──
    if not args.samples_only:
        print("\nWriting JSON files ...")
        write_json(os.path.join(out, "customers.json"), customers_out)
        write_json(os.path.join(out, "products.json"), products_out)
        write_json(os.path.join(out, "orders.json"), orders)
        write_json(os.path.join(out, "order_items.json"), order_items)

    # ── Write sample CSVs (always) ──
    print("\nWriting sample CSVs ...")
    write_csv(
        os.path.join(samples_dir, "customers_sample.csv"),
        customers_out, CSV_SAMPLE_ROWS,
    )
    write_csv(
        os.path.join(samples_dir, "products_sample.csv"),
        products_out, CSV_SAMPLE_ROWS,
    )
    write_csv(
        os.path.join(samples_dir, "orders_sample.csv"),
        orders, CSV_SAMPLE_ROWS,
    )
    write_csv(
        os.path.join(samples_dir, "order_items_sample.csv"),
        order_items, CSV_SAMPLE_ROWS,
    )

    print("\nDone!")


if __name__ == "__main__":
    main()
