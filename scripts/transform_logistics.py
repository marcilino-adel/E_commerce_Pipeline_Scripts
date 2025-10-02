from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, when

def transform_logistics_data():
    spark = SparkSession.builder \
        .appName("Transform Logistics Data") \
        .getOrCreate()

    print("SparkSession created for logistics transformation.")

    input_path = "/user/source/logistics"
    output_path = "/user/warehouse/e-commerce/fact_logistics"

    print(f"Reading logistics source data from: {input_path}")
    logistics_df = spark.read.parquet(input_path)

    # --- FIX: Convert BIGINT timestamps (milliseconds) to actual date types ---
    logistics_with_dates_df = logistics_df.withColumn(
        "actual_delivery_date_casted", (col("actual_delivery_date") / 1000).cast("timestamp")
    ).withColumn(
        "estimated_delivery_date_casted", (col("estimated_delivery_date") / 1000).cast("timestamp")
    )
    # -------------------------------------------------------------------------

    print("Calculating delivery performance KPIs...")
    fact_df = logistics_with_dates_df.withColumn(
        "delivery_time_delta_days",
        # Use the newly casted date columns for the calculation
        datediff(col("actual_delivery_date_casted"), col("estimated_delivery_date_casted"))
    ).withColumn(
        "is_late_delivery",
        when(col("delivery_time_delta_days") > 0, True).otherwise(False)
    ).select(
        "order_id",
        "delivery_time_delta_days",
        "is_late_delivery",
        "shipping_cost",
        "warehouse_id"
    )

    print(f"Writing transformed fact table to HDFS: {output_path}")
    fact_df.write.mode("overwrite").parquet(output_path)
    
    print("\n--- Project Deliverable: Average Delivery Time Delta per Warehouse ---")
    avg_delta_per_warehouse = fact_df.groupBy("warehouse_id") \
        .avg("delivery_time_delta_days") \
        .withColumnRenamed("avg(delivery_time_delta_days)", "avg_delivery_delta") \
        .orderBy("warehouse_id")
    
    avg_delta_per_warehouse.show()
    print("--------------------------------------------------------------------")

    spark.stop()
    print("Logistics transformation complete. SparkSession stopped.")

if __name__ == "__main__":
    transform_logistics_data()
