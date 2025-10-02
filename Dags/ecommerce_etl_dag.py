from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
}

# The Hive DDL query is now part of the bash command
HQL_QUERY = """
CREATE EXTERNAL TABLE IF NOT EXISTS staging_orderlines (
  order_line_id   STRING,
  order_id        BIGINT,
  customer_id     BIGINT,
  order_date      STRING,
  payment_status  STRING,
  product_id      BIGINT,
  sales_quantity  BIGINT,
  price_per_unit  DOUBLE
)
STORED AS PARQUET
LOCATION '/user/warehouse/e-commerce/order_lines';
"""

# Define the DAG
with DAG(
    dag_id='ecommerce_etl_pipeline',
    default_args=default_args,
    description='A complete ETL pipeline for e-commerce orders.',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Run the ingestion Spark job
    ingestion_task = SparkSubmitOperator(
        task_id='ingest_orders_to_hdfs',
        application='/root/airflow/dags/scripts/ingest_orders_to_hdfs.py',
        conn_id='spark_default',
        verbose=True
    )

    # Task 2: Run the transformation Spark job
    transformation_task = SparkSubmitOperator(
        task_id='transform_staged_orders',
        application='/root/airflow/dags/scripts/transform_orders.py',
        conn_id='spark_default',
        verbose=True
    )

    # Task 3: Create the Hive table using beeline
    create_hive_table_task = BashOperator(
        task_id='create_hive_external_table',
        bash_command=f'beeline -u jdbc:hive2://hive-server:10000 -e "{HQL_QUERY}"'
    )

    # Set the dependency chain for the tasks
    ingestion_task >> transformation_task >> create_hive_table_task
