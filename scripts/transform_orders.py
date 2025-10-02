from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, concat, lit

def transform_staged_orders():
    """
    Reads staged order data from HDFS, flattens the nested item array,
    and writes the transformed data to a warehouse directory.
    """
    spark = SparkSession.builder \
        .appName("Transform Staged Orders") \
        .getOrCreate()

    print("SparkSession created for transformation.")

    # 1. Define HDFS paths
    input_path = "/user/staging/e-commerce/orders"
    output_path = "/user/warehouse/e-commerce/order_lines"

    # 2. Read the staged Parquet data
    print(f"Reading staged Parquet data from: {input_path}")
    staged_df = spark.read.parquet(input_path)

    # 3. Transform the data: Explode the 'items' array
    # The explode function creates a new row for each element in the 'items' array.
    print("Exploding the 'items' array...")
    exploded_df = staged_df.withColumn("item", explode(col("items")))

    # 4. Select and restructure the columns for the final format
    # We'll pull the nested fields out of the 'item' struct using dot notation.
    # We also create a unique 'order_line_id' as described in the project document.
    final_df = exploded_df.select(
        concat(col("order_id"), lit("_"), col("item.product_id")).alias("order_line_id"),
        col("order_id"),
        col("customer_id"),
        col("order_date"),
        col("payment_status"),
        col("item.product_id").alias("product_id"),
        col("item.sales_quantity").alias("sales_quantity"),
        col("item.price_per_unit").alias("price_per_unit")
    )
    
    print("Final DataFrame schema:")
    final_df.printSchema()
    final_df.show(10)

    # 5. Write the final DataFrame to the warehouse directory
    print(f"Writing transformed data to HDFS warehouse: {output_path}")
    final_df.write.mode("overwrite").parquet(output_path)

    spark.stop()
    print("Transformation complete. SparkSession stopped.")

if __name__ == "__main__":
    transform_staged_orders()
