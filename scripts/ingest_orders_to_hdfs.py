from pyspark.sql import SparkSession

def ingest_raw_orders():
    """
    A PySpark job to ingest raw JSON orders from a local file
    and write them to a staging area in HDFS as Parquet.
    """
    spark = SparkSession.builder \
        .appName("Ingest Raw Orders to HDFS") \
        .getOrCreate()

    print("SparkSession created successfully.")

    # CORRECTED: Added "file:///" to specify the local filesystem
    input_path = "file:///root/airflow/dags/data/raw_orders.json"
    
    output_path = "/user/staging/e-commerce/orders"

    try:
        print(f"Reading JSON data from: {input_path}")
        orders_df = spark.read.option("multiline", "true").json(input_path)
        print("Successfully read JSON data into DataFrame.")
        orders_df.printSchema()
        orders_df.show(5)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        spark.stop()
        return

    try:
        print(f"Writing data as Parquet to HDFS: {output_path}")
        orders_df.write.mode("overwrite").parquet(output_path)
        print("Successfully wrote data to HDFS.")
    except Exception as e:
        print(f"Error writing to HDFS: {e}")
        spark.stop()
        return

    spark.stop()
    print("Ingestion complete. SparkSession stopped.")

if __name__ == "__main__":
    ingest_raw_orders()
