# DataHub Generic Ingestion Orchestration

This project was based on a github fork (https://github.com/cordon-thiago/airflow-spark), to configure the airflow cluster follow the steps creating by the author: https://medium.com/data-arena/building-a-spark-and-airflow-development-environment-with-docker-f0b9b625edd8

## Dags

### - datahub_ingestion_csv_to_mysql_datapipeline
To test this dag alter the aiflow connection: mysql_default with parameters:
 - Host: docker_mysql_1
 - Login: root
 - Password: admin123
 - Port: 3306

This dag use yaml configuration from spark/resources/configuration/ingestion-configuration-csv-to-mysql.yml, read spark/resources/data/bronze/movies.csv file and write on MySQL table, on "fetch_records_from_mysql" task, a top 10 records are printed

### - datahub_ingestion_avro_to_mongodb_datapipeline
To test this dag alter the aiflow connection: mongo_default with parameters:
 - Host: docker_mongo_1
 - Schema: test
 - Login: root
 - Password: admin
 - Port: 27017
 - Extra: {"authSource":"admin"}

This dag use yaml configuration from spark/resources/configuration/ingestion-configuration-avro-to-mongodb.yml, read avro files from spark/resources/data/bronze/avro_orders and write on MongoDB collection, on "fetch_records_from_mongodb" task, a count of records are printed

### - datahub_ingestion_parquet_to_mysql_datapipeline
To test this dag alter the aiflow connection: mysql_default with parameters:
 - Host: docker_mysql_1
 - Login: root
 - Password: admin123
 - Port: 3306

This dag use yaml configuration from spark/resources/configuration/ingestion-configuration-parquet-to-mysql.yml, read parquet files from spark/resources/data/bronze/parquet_orders and write on MySQL table, on "fetch_records_from_mysql" task, a top 10 records are printed

### - datahub_ingestion_mysql_to_mongodb_datapipeline
To test this dag alter the aiflow connection: mysql_default with parameters:
 - Host: docker_mysql_1
 - Login: root
 - Password: admin123
 - Port: 3306

And mongo_default with parameters:
 - Host: docker_mongo_1
 - Schema: test
 - Login: root
 - Password: admin
 - Port: 27017
 - Extra: {"authSource":"admin"}

This dag use yaml configuration from spark/resources/configuration/ingestion-configuration-mysql-to-mongodb.yml, read MySql table Sales and write on MongoDB collection, on "fetch_records_from_mongodb" task, a count of records are printed