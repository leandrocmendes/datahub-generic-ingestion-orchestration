from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.hooks.mongo_hook import MongoHook
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator

spark_app_name = "datahub-ingestion-avro-to-mongodb"
file_path = "/usr/local/spark/resources/data/configuration/ingestion-configuration-avro-to-mongodb.yml"

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


def fetch_records():
    mongo_hook = MongoHook(
        conn_id="mongo_default",
    )
    sources = mongo_hook.get_collection("OrdersAvro", "test")
    print("Documents count: " + str(sources.estimated_document_count()))


dag = DAG(
        dag_id="datahub_ingestion_avro_to_mongodb_datapipeline",
        description="This DAG call datahub with a source avro and destination mongodb",
        default_args=default_args, 
        schedule_interval=None
    )

start = DummyOperator(task_id="start", dag=dag)

datahub_job = SparkSubmitOperator(
    task_id="datahub-ingestion-avro-to-mongodb",
    application="/usr/local/spark/app/datahub-generic-ingestion-1.0-SNAPSHOT.jar",
    name=spark_app_name,
    conn_id="spark_default",
    files=file_path,
    application_args=["ingestion-configuration-avro-to-mongodb.yml"],
    dag=dag,
    java_class="com.br.datahub.generic.ingestion.Main",
    packages="org.apache.spark:spark-avro_2.12:3.0.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
)

fetch_records_from_mongodb = PythonOperator(
    task_id="fetch_records_mongo",
    dag=dag,
    python_callable=fetch_records,
)

end = DummyOperator(task_id="end", dag=dag)

start >> datahub_job >> fetch_records_from_mongodb >> end
