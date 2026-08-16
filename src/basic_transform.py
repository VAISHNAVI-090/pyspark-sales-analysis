"""
basic_transform.py
-------------------
The "naive" version of the pipeline: no caching, no partitioning tuning.
Reads CSVs, cleans, filters, joins, and aggregates using default Spark
settings only.

This exists to be timed and compared against optimized_transform.py, so we
have a real before/after to document in the README instead of just
claiming "this is optimized."

Run:
    python src/basic_transform.py
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, round as spark_round, sum as spark_sum, trim


def main():
    spark = SparkSession.builder.appName("SalesAnalysis-Basic").getOrCreate()

    start = time.time()

    orders = spark.read.csv("data/raw/orders.csv", header=True, inferSchema=True)
    products = spark.read.csv("data/raw/products.csv", header=True, inferSchema=True)

    # --- Clean ---
    orders_clean = (
        orders.dropDuplicates()
        .dropna(subset=["quantity"])
        .withColumn("status", lower(trim(col("status"))))
    )

    # --- Filter ---
    completed_orders = orders_clean.filter(col("status") == "completed")

    # --- Join ---
    joined = completed_orders.join(products, on="product_id", how="inner")

    # --- Aggregate ---
    joined = joined.withColumn("line_total", col("quantity") * col("unit_price"))

    revenue_by_category = (
        joined.groupBy("category")
        .agg(spark_round(spark_sum("line_total"), 2).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    revenue_by_region = (
        joined.groupBy("region")
        .agg(spark_round(spark_sum("line_total"), 2).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    # Actions that trigger execution (Spark is lazy until here)
    print("\n=== Revenue by Category (basic) ===")
    revenue_by_category.show()

    print("=== Revenue by Region (basic) ===")
    revenue_by_region.show()

    elapsed = round(time.time() - start, 2)
    print(f"[basic_transform] completed in {elapsed}s")

    spark.stop()
    return elapsed


if __name__ == "__main__":
    main()
