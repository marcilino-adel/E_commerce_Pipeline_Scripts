from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, current_date, datediff, max as max_, countDistinct, sum as sum_, ntile, monotonically_increasing_id, coalesce

def build_all_dimensions():
    """
    A single PySpark job to build both the Dim_Customer and SCD2 Dim_Products dimension tables.
    """
    spark = SparkSession.builder \
        .appName("Build All Final Dimensions") \
        .getOrCreate()

    print("SparkSession created for building all dimensions.")
    today = current_date()

    # ======================================================
    # ========== PART 1: BUILD DIM_CUSTOMER ==========
    # ======================================================
    print("\n### Starting Dim_Customer build... ###")

    customers_path = "/user/source/customers"
    order_lines_path = "/user/warehouse/e-commerce/order_lines"
    
    customers_df = spark.read.parquet(customers_path)
    order_lines_df = spark.read.parquet(order_lines_path)

    snapshot_date = lit("2025-10-01").cast("date")

    rfm_df = order_lines_df.groupBy("customer_id").agg(
        max_("order_date").alias("last_purchase_date"),
        countDistinct("order_id").alias("frequency"),
        sum_(col("sales_quantity") * col("price_per_unit")).alias("monetary")
    ).withColumn(
        "recency", datediff(snapshot_date, col("last_purchase_date"))
    )

    r_window = Window.orderBy(col("recency").asc())
    f_window = Window.orderBy(col("frequency").desc())
    m_window = Window.orderBy(col("monetary").desc())

    rfm_scores_df = rfm_df.withColumn("r_score", ntile(5).over(r_window)) \
                          .withColumn("f_score", ntile(5).over(f_window)) \
                          .withColumn("m_score", ntile(5).over(m_window))

    rfm_final_df = rfm_scores_df.withColumn("RFM_Score", (col("r_score") + col("f_score") + col("m_score")))

    dim_customer_df = customers_df.join(rfm_final_df, "customer_id", "left") \
        .withColumn("cust_key", monotonically_increasing_id()) \
        .select("cust_key", "customer_id", "age", "gender", "city", "registration_date", "RFM_Score")

    dim_customer_output_path = "/user/warehouse/e-commerce/Dim_Customer"
    dim_customer_df.write.mode("overwrite").parquet(dim_customer_output_path)
    print(f"Dim_Customer successfully written to {dim_customer_output_path}")
    dim_customer_df.show(5)

    # ======================================================
    # ========== PART 2: BUILD DIM_PRODUCTS (SCD2) =========
    # ======================================================
    print("\n### Starting Dim_Products (SCD2) build... ###")
    
    source_products_path = "/user/source/products"
    dim_products_path = "/user/warehouse/e-commerce/Dim_Products"
    
    new_source_df = spark.read.parquet(source_products_path) \
        .withColumnRenamed("current_price", "product_price") \
        .withColumnRenamed("description", "product_description")

    try:
        old_dim_df = spark.read.parquet(dim_products_path)
    except Exception:
        print("Dim_Products not found. This must be the first run.")
        old_dim_df = spark.createDataFrame([], schema=new_source_df.withColumn("product_key", lit(None).cast("long")) \
                                                                 .withColumn("dw_start_date", lit(None).cast("date")) \
                                                                 .withColumn("dw_end_date", lit(None).cast("date")).schema)

    # --- CORRECTED LOGIC WITH ALIASES ---
    # Alias the dataframes to resolve column ambiguity
    new_aliased = new_source_df.alias("new")
    old_aliased = old_dim_df.filter(col("dw_end_date") == "9999-12-31").alias("old")

    # Join using the aliases
    joined_df = new_aliased.join(
        old_aliased,
        col("new.product_id") == col("old.product_id"),
        "full_outer"
    )

    # Filter for unchanged records using aliases
    unchanged_df = joined_df.filter(
        col("new.product_id").isNotNull() &
        col("old.product_id").isNotNull() &
        (col("new.product_price") == col("old.product_price"))
    ).select("old.*")

    # Filter for new records using aliases
    new_records_df = joined_df.filter(col("old.product_id").isNull()) \
        .select("new.*") \
        .withColumn("dw_start_date", today) \
        .withColumn("dw_end_date", lit("9999-12-31").cast("date"))

    # Filter for changed records using aliases
    changed_records = joined_df.filter(
        col("new.product_id").isNotNull() &
        col("old.product_id").isNotNull() &
        (col("new.product_price") != col("old.product_price"))
    )
    
    expired_records_df = changed_records.select("old.*").withColumn("dw_end_date", today)
    new_active_records_df = changed_records.select("new.*") \
        .withColumn("dw_start_date", today) \
        .withColumn("dw_end_date", lit("9999-12-31").cast("date"))

    # Union all parts and generate surrogate keys
    final_dim_products_df = unchanged_df.unionByName(new_records_df, allowMissingColumns=True) \
                                        .unionByName(expired_records_df, allowMissingColumns=True) \
                                        .unionByName(new_active_records_df, allowMissingColumns=True) \
                                        .withColumn("product_key", coalesce(col("product_key"), monotonically_increasing_id()))

    final_dim_products_df.write.mode("overwrite").parquet(dim_products_path)
    print(f"Dim_Products successfully written to {dim_products_path}")
    final_dim_products_df.orderBy("product_id", "dw_start_date").show(truncate=False)

    spark.stop()
    print("\nAll dimension builds complete. SparkSession stopped.")

if __name__ == "__main__":
    build_all_dimensions()
