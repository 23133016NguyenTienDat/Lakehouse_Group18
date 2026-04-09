from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


BRONZE_DATASETS = [
    ("events.csv", "events"),
    ("customers.csv", "customers"),
    ("orders.csv", "orders"),
    ("order_items.csv", "order_items"),
    ("products.csv", "products"),
    ("reviews.csv", "reviews"),
    ("sessions.csv", "sessions"),
]


SPARK_SUBMIT_BASE = " ".join(
    [
        "spark-submit",
        "--master spark://spark-master:7077",
        "--jars /opt/project/drivers/delta-core_2.12-2.4.0.jar,/opt/project/drivers/delta-storage-2.4.0.jar",
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.HDFSLogStore",
    ]
)


with DAG(
    dag_id="medallion_pipeline_skeleton",
    description="Skeleton DAG for Bronze -> Silver -> Gold pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "medallion", "skeleton"],
    render_template_as_native_obj=True,
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="bronze_ingestion") as bronze_ingestion:
        for csv_file, dataset_name in BRONZE_DATASETS:
            BashOperator(
                task_id=f"ingest_{dataset_name}",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/ingest_bronze.py "
                    "{{ ds }} "
                    f"{csv_file} "
                    f"{dataset_name}"
                ),
            )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=(
            "echo '[TODO] Run Silver jobs for ingest_date={{ ds }}' "
            "&& echo 'Replace this task with spark-submit or python entrypoint later.'"
        ),
    )

    gold_transform = BashOperator(
        task_id="gold_transform",
        bash_command=(
            "echo '[TODO] Run Gold jobs for ingest_date={{ ds }}' "
            "&& echo 'Replace this task with mart/aggregation job later.'"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> bronze_ingestion >> silver_transform >> gold_transform >> end