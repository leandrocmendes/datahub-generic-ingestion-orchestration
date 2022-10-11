from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.hooks.mongo_hook import MongoHook
from datetime import datetime, timedelta

from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator

spark_app_name = "datahub-ingestion-mysql-to-mongodb"
file_path = "/usr/local/spark/resources/data/configuration/ingestion-configuration-mysql-to-mongodb.yml"
mysql_jar_path = "/usr/local/spark/resources/jars/mysql-connector-java-8.0.30.jar"
mysql_connection = "mysql_default"

now = datetime.now()

insert_sql_query = """ INSERT INTO test.Sales(empid, empname, price) VALUES(1,'VAMSHI',30000.0),(2,'chandu',50000.0); """


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
    sources = mongo_hook.get_collection("SalesMySql", "test")
    print("Documents count: " + str(sources.estimated_document_count()))


dag = DAG(
        dag_id="datahub_ingestion_mysql_to_mongodb_datapipeline",
        description="This DAG call datahub with a source mysql and destination mongodb",
        default_args=default_args, 
        schedule_interval=None
    )

start = DummyOperator(task_id="start", dag=dag)

create_database = MySqlOperator(
    task_id="create_database_task",
    sql="CREATE DATABASE IF NOT EXISTS test",
    dag=dag,
    mysql_conn_id=mysql_connection,
)

create_table = MySqlOperator(
    task_id="create_table_task",
    sql="CREATE TABLE IF NOT EXISTS test.Sales(empid int, empname VARCHAR(25), price float);",
    dag=dag,
    mysql_conn_id=mysql_connection,
)

populate_mysql = MySqlOperator(
    dag=dag,
    task_id="populate_mysql_table",
    sql=insert_sql_query,
    mysql_conn_id=mysql_connection,
)

datahub_job = SparkSubmitOperator(
    task_id="datahub-ingestion-mysql-to-mongodb",
    application="/usr/local/spark/app/datahub-generic-ingestion-1.0-SNAPSHOT.jar",
    name=spark_app_name,
    conn_id="spark_default",
    files=file_path,
    application_args=["ingestion-configuration-mysql-to-mongodb.yml"],
    dag=dag,
    java_class="com.br.datahub.generic.ingestion.Main",
    packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
    jars=mysql_jar_path
)

fetch_records_from_mongodb = PythonOperator(
    task_id="fetch_records_mongo",
    dag=dag,
    python_callable=fetch_records,
)

end = DummyOperator(task_id="end", dag=dag)

start >> create_database >> create_table >> populate_mysql >> datahub_job >> fetch_records_from_mongodb >> end
