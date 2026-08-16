"""
spark_sql_queries.py
----------------------
Demonstrates Spark SQL: registers the cleaned/joined data as a temporary
view and runs plain SQL queries against it, instead of the DataFrame API.

Run:
    python src/spark_sql_queries.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim


def main():
    spark = SparkSession.builder.appName("SalesAnalysis-SQL").getOrCreate()

    orders = spark.read.csv("data/raw/orders.csv", header=True, inferSchema=True)
    products = spark.read.csv("data/raw/products.csv", header=True, inferSchema=True)

    orders_clean = (
        orders.dropDuplicates()
        .dropna(subset=["quantity"])
        .withColumn("status", lower(trim(col("status"))))
        .filter(col("status") == "completed")
    )

    joined = orders_clean.join(products, on="product_id", how="inner")
    joined = joined.withColumn("line_total", col("quantity") * col("unit_price"))

    # Register as a temp view so we can query it with SQL
    joined.createOrReplaceTempView("sales")

    print("\n=== Top 5 products by total revenue (Spark SQL) ===")
    spark.sql(
        """
        SELECT product_name, category, ROUND(SUM(line_total), 2) AS total_revenue
        FROM sales
        GROUP BY product_name, category
        ORDER BY total_revenue DESC
        LIMIT 5
        """
    ).show()

    print("=== Revenue by region and category (Spark SQL) ===")
    spark.sql(
        """
        SELECT region, category, ROUND(SUM(line_total), 2) AS total_revenue
        FROM sales
        GROUP BY region, category
        ORDER BY region, total_revenue DESC
        """
    ).show(20)

    spark.stop()


if __name__ == "__main__":
    main()
