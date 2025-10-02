from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# --- Task 1: Sqoop Import Commands ---
# CORRECTED: Added --as-parquetfile to the end of the commands
customers_import_cmd = (
    "sqoop import --connect jdbc:postgresql://external_postgres_db/external "
    "--username external --password external "
    "--table customers "
    "--target-dir /user/source/customers "
    "--delete-target-dir --m 1 "
    "--as-parquetfile"
)

products_import_cmd = (
    "sqoop import --connect jdbc:postgresql://external_postgres_db/external "
    "--username external --password external "
    "--table products "
    "--target-dir /user/source/products "
    "--delete-target-dir --m 1 "
    "--as-parquetfile"
)

# --- DAG DEFINITION ---
with DAG(
    dag_id='master_ecommerce_etl_pipeline',
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ### Master E-commerce ETL Pipeline
    This pipeline orchestrates the entire workflow:
    1. Imports source data from PostgreSQL using Sqoop (in parallel).
    2. Builds dimension tables using a Spark job.
    3. Creates the final Hive tables using a beeline script.
    """
) as dag:

    # --- STAGE 1: SQOOP IMPORTS (run in parallel) ---
    import_customers = SSHOperator(
        task_id='import_customers_from_postgres',
        ssh_conn_id='ssh_default',
        command=customers_import_cmd
    )

    import_products = SSHOperator(
        task_id='import_products_from_postgres',
        ssh_conn_id='ssh_default',
        command=products_import_cmd
    )

    # --- STAGE 2: BUILD DIMENSIONS (Spark job) ---
    build_dimensions = SparkSubmitOperator(
        task_id='build_dimension_tables',
        application='/root/airflow/dags/scripts/build_dimensions_final.py',
        conn_id='spark_default',
        verbose=True
    )

    # --- STAGE 3: CREATE HIVE TABLES (Hive DDL) ---
    create_hive_tables = BashOperator(
        task_id='create_hive_dimension_tables',
        bash_command='beeline -u jdbc:hive2://hive-server:10000 -f /root/airflow/dags/scripts/create_dims_in_hive.hql'
    )

    # --- SETTING DEPENDENCIES ---
    [import_customers, import_products] >> build_dimensions >> create_hive_tables

