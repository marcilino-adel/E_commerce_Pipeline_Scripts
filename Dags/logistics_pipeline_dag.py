from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

logistics_import_cmd = (
    "sqoop import --connect jdbc:postgresql://external_postgres_db/external "
    "--username external --password external "
    "--table logistics --target-dir /user/source/logistics "
    "--delete-target-dir --m 1 --as-parquetfile"
)

with DAG(
    dag_id='logistics_etl_pipeline',
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
    doc_md="### Logistics ETL Pipeline (Project 3.3)"
) as dag:

    # Task 1: Import the logistics table using Sqoop
    import_logistics = SSHOperator(
        task_id='import_logistics_from_postgres',
        ssh_conn_id='ssh_default',
        command=logistics_import_cmd
    )

    # Task 2: Transform logistics data with Spark
    transform_logistics = SparkSubmitOperator(
        task_id='transform_logistics_data',
        application='/root/airflow/dags/scripts/transform_logistics.py',
        conn_id='spark_default'
    )

    # Task 3: Create the final Hive table
    create_hive_table = BashOperator(
        task_id='create_hive_fact_table',
        bash_command='beeline -u jdbc:hive2://hive-server:10000 -f /root/airflow/dags/scripts/create_fact_logistics.hql'
    )

    # Define the dependencies to run the tasks in series
    import_logistics >> transform_logistics >> create_hive_table
