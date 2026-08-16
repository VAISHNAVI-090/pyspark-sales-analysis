"""
generate_data.py
-----------------
Creates two small CSV files used by the PySpark pipeline:

  1. data/raw/orders.csv    -> order-level transactions (has some messy data
                                on purpose: nulls, duplicates, mixed status casing)
  2. data/raw/products.csv  -> product catalog (used for the join)

This uses pandas only to generate sample data quickly. The actual pipeline
(basic_transform.py / optimized_transform.py) uses PySpark, not pandas.

Run once:
    python src/generate_data.py
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

CATEGORIES = ["Electronics", "Accessories", "Office", "Furniture"]

PRODUCTS = [
    ("P001", "Wireless Mouse", "Electronics", 799.0),
    ("P002", "Mechanical Keyboard", "Electronics", 2499.0),
    ("P003", "USB-C Hub", "Accessories", 1299.0),
    ("P004", "Laptop Stand", "Furniture", 1599.0),
    ("P005", "Webcam 1080p", "Electronics", 1899.0),
    ("P006", "Bluetooth Speaker", "Electronics", 2299.0),
    ("P007", "Desk Lamp", "Office", 999.0),
    ("P008", "Noise Cancelling Headset", "Electronics", 4999.0),
    ("P009", "Office Chair", "Furniture", 6999.0),
    ("P010", "Notebook Set", "Office", 249.0),
]

REGIONS = ["North", "South", "East", "West"]
STATUSES = ["completed", "Completed", "COMPLETED", "cancelled", "pending"]


def generate_products():
    df = pd.DataFrame(PRODUCTS, columns=["product_id", "product_name", "category", "unit_price"])
    df.to_csv(RAW_DIR / "products.csv", index=False)
    print(f"Wrote {len(df)} rows -> {RAW_DIR / 'products.csv'}")


def generate_orders(n=20000):
    start, end = datetime(2026, 1, 1), datetime(2026, 6, 30)
    rows = []

    for i in range(1, n + 1):
        product_id = random.choice(PRODUCTS)[0]
        order_date = start + timedelta(days=random.randint(0, (end - start).days))

        rows.append(
            {
                "order_id": f"ORD-{100000 + i}",
                "product_id": product_id,
                "region": random.choice(REGIONS),
                # ~3% missing quantity to simulate real messiness
                "quantity": random.randint(1, 5) if random.random() > 0.03 else None,
                "order_date": order_date.strftime("%Y-%m-%d"),
                # inconsistent casing on purpose -> cleaned in transform step
                "status": random.choice(STATUSES),
            }
        )

    df = pd.DataFrame(rows)

    # Inject ~200 duplicate rows to simulate export retries
    dupes = df.sample(200, random_state=1)
    df = pd.concat([df, dupes], ignore_index=True)

    df.to_csv(RAW_DIR / "orders.csv", index=False)
    print(f"Wrote {len(df)} rows -> {RAW_DIR / 'orders.csv'}")


if __name__ == "__main__":
    generate_products()
    generate_orders()
