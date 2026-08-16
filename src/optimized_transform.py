"""
optimized_transform.py
------------------------
Same pipeline as basic_transform.py, but with two simple, well-known
optimization techniques applied:

  1. .cache() on the joined DataFrame, since it's reused for TWO
     aggregations (by category and by region). Without caching, Spark
     would re-read and re-compute the join from scratch for each action.
  2. .repartition() on the columns used for grouping before each aggregation.

This is intentionally a small, understandable optimization — not a
claim of deep Spark tuning expertise. The point is to show the before/after
difference and explain the purpose and trade-offs of each change.

Run:
    python src/optimized_transform.py
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, round as spark_round, sum as spark_sum, trim


def main():
    spark = SparkSession.builder.appName("SalesAnalysis-Optimized").getOrCreate()

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
    joined = joined.withColumn("line_total", col("quantity") * col("unit_price"))

    # --- Optimization 1: cache, because `joined` is used twice below ---
    joined = joined.cache()
    joined.count()  # materialize the cache now (forces the first full computation)

    # --- Optimization 2: repartition before each groupBy ---
    revenue_by_category = (
        joined.repartition("category")
        .groupBy("category")
        .agg(spark_round(spark_sum("line_total"), 2).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    revenue_by_region = (
        joined.repartition("region")
        .groupBy("region")
        .agg(spark_round(spark_sum("line_total"), 2).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    print("\n=== Revenue by Category (optimized) ===")
    revenue_by_category.show()

    print("=== Revenue by Region (optimized) ===")
    revenue_by_region.show()

    elapsed = round(time.time() - start, 2)
    print(f"[optimized_transform] completed in {elapsed}s")

    joined.unpersist()
    spark.stop()
    return elapsed


if __name__ == "__main__":
    main()
