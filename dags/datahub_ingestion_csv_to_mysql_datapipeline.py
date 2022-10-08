from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_app_name = "test-avro-to-mongo"
file_path = "/usr/local/spark/resources/data/ingestion-configuration-avro-to-mongodb.yml"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="datahub_ingestion_csv_to_mysql_datapipeline", 
        description="This DAG runs a simple Pyspark app.",
        default_args=default_args, 
        schedule_interval=None
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/usr/local/spark/app/datahub-generic-ingestion-1.0-SNAPSHOT.jar",
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    files=file_path,
    application_args=["ingestion-configuration-avro-to-mongodb.yml"],
    dag=dag,
    java_class="com.br.datahub.generic.ingestion.Main",
    packages="org.apache.spark:spark-avro_2.12:3.0.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end