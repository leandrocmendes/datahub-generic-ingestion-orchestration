from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator

spark_app_name = "datahub-ingestion-parquet-to-mysql"
file_path = "/usr/local/spark/resources/data/configuration/ingestion-configuration-parquet-to-mysql.yml"
mysql_jar_path = "/usr/local/spark/resources/jars/mysql-connector-java-8.0.30.jar"
mysql_connection = "mysql_default"

now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def fetch_records():
    query = "SELECT * FROM OrdersParquet Limit 10"
    mysql_hook = MySqlHook(
        mysql_conn_id=mysql_connection,
        schema="test"
    )
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    sources = cursor.fetchall()
    print(sources)


dag = DAG(
    dag_id="datahub_ingestion_parquet_to_mysql_datapipeline",
    description="This DAG call datahub with a source parquet and mysql destination",
    default_args=default_args,
    schedule_interval=None,
)

start = DummyOperator(task_id="start", dag=dag)

create_database = MySqlOperator(
    task_id="create_database_task",
    sql="CREATE DATABASE IF NOT EXISTS test",
    dag=dag,
    mysql_conn_id=mysql_connection,
)

datahub_job = SparkSubmitOperator(
    task_id="datahub-ingestion-parquet-to-mysql",
    application="/usr/local/spark/app/datahub-generic-ingestion-1.0-SNAPSHOT.jar",
    name=spark_app_name,
    conn_id="spark_default",
    files=file_path,
    application_args=["ingestion-configuration-parquet-to-mysql.yml"],
    dag=dag,
    java_class="com.br.datahub.generic.ingestion.Main",
    jars=mysql_jar_path,
)

fetch_task = PythonOperator(
    dag=dag,
    task_id="fetch_records_from_mysql",
    python_callable=fetch_records,
)

end = DummyOperator(task_id="end", dag=dag)

start >> create_database >> datahub_job >> fetch_task >> end
