from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SPARK_APP_DIR = "/opt/lakehouse/scripts/spark"

SPARK_CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": "nessie",
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.type": "rest",
    "spark.sql.catalog.nessie.uri": "http://nessie:19120/iceberg/main/",
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.nessie.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.nessie.s3.path-style-access": "true",
    "spark.sql.catalog.nessie.s3.access-key-id": "minioadmin",
    "spark.sql.catalog.nessie.s3.secret-access-key": "minioadmin",
    "spark.executorEnv.AWS_ACCESS_KEY_ID": "minioadmin",
    "spark.executorEnv.AWS_SECRET_ACCESS_KEY": "minioadmin",
    "spark.executorEnv.AWS_REGION": "us-east-1",
    "spark.sql.session.timeZone": "Asia/Ho_Chi_Minh",
}

ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.11.0"
)

SPARK_RESOURCES = {
    "deploy_mode": "client",
    "driver_memory": "1g",
    "executor_memory": "1g",
    "num_executors": 1,
    "executor_cores": 2,
}

SPARK_ENV = {
    "PYTHONPATH": SPARK_APP_DIR,
}


with DAG(
    dag_id="electricity_lakehouse_pipeline",
    description="Vietnam Electricity Lakehouse Pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 9, 22),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(hours=2),
    tags=["lakehouse", "electricity"],
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="bronze_ingestion"):

        # bronze_evn = SparkSubmitOperator(
        #     task_id="ingest_evn",
        #     application=f"{SPARK_APP_DIR}/bronze/evn.py",
        #     conn_id="spark_default",
        #     name="bronze_evn",
        #     conf=SPARK_CONF,
        #     packages=ICEBERG_PACKAGES,
        #     env_vars=SPARK_ENV,
        #     **SPARK_RESOURCES,
        #     execution_timeout=timedelta(minutes=45),
        #     verbose=True,
        # )

        # bronze_hydro = SparkSubmitOperator(
        #     task_id="ingest_hydro",
        #     application=f"{SPARK_APP_DIR}/bronze/hydro.py",
        #     conn_id="spark_default",
        #     name="bronze_hydro",
        #     conf=SPARK_CONF,
        #     packages=ICEBERG_PACKAGES,
        #     env_vars=SPARK_ENV,
        #     **SPARK_RESOURCES,
        #     execution_timeout=timedelta(minutes=45),
        #     verbose=True,
        # )
        
        # bronze_hydro = SparkSubmitOperator(
        #     task_id="ingest_nsmo",
        #     application=f"{SPARK_APP_DIR}/bronze/nsmo.py",
        #     conn_id="spark_default",
        #     name="bronze_nsmo",
        #     conf=SPARK_CONF,
        #     packages=ICEBERG_PACKAGES,
        #     env_vars=SPARK_ENV,
        #     **SPARK_RESOURCES,
        #     execution_timeout=timedelta(minutes=45),
        #     verbose=True,
        # )
        
        bronze_open_meteo = SparkSubmitOperator(
            task_id="ingest_open_meteo",
            application=f"{SPARK_APP_DIR}/bronze/open_meteo.py",
            conn_id="spark_default",
            name="bronze_open_meteo",
            conf=SPARK_CONF,
            packages=ICEBERG_PACKAGES,
            env_vars=SPARK_ENV,
            **SPARK_RESOURCES,
            execution_timeout=timedelta(minutes=45),
            verbose=True,
        )

    end = EmptyOperator(task_id="end")

    start >> bronze_open_meteo >> end
