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

SILVER_JOBS = [
    ("clean_events.py", "events"),
    ("clean_customers.py", "customers"),
    ("clean_orders.py", "orders_and_items"),
    ("clean_products.py", "products"),
    ("clean_reviews.py", "reviews"),
    ("clean_sessions.py", "sessions"),
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
                    "/opt/project/scripts/bronze/ingest_bronze.py "
                    "{{ ds }} "
                    f"{csv_file} "
                    f"{dataset_name}"
                ),
            )

    with TaskGroup(group_id="silver_transform") as silver_transform:
        for script_name, task_suffix in SILVER_JOBS:
            BashOperator(
                task_id=f"clean_{task_suffix}",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    f"/opt/project/scripts/silver/{script_name} "
                    "{{ ds }}"
                ),
            )

    with TaskGroup(group_id="gold_transform") as gold_transform:
        with TaskGroup(group_id="dim_static") as gold_dim_static:
            build_dim_country = BashOperator(
                task_id="build_dim_country",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "dim_country "
                    "incremental"
                ),
            )

            build_dim_source = BashOperator(
                task_id="build_dim_source",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "dim_source "
                    "incremental"
                ),
            )

            build_dim_device = BashOperator(
                task_id="build_dim_device",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "dim_device "
                    "incremental"
                ),
            )

            build_dim_payment_method = BashOperator(
                task_id="build_dim_payment_method",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "dim_payment_method "
                    "incremental"
                ),
            )

            build_dim_date = BashOperator(
                task_id="build_dim_date",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "dim_date "
                    "incremental"
                ),
            )

            build_dim_category = BashOperator(
                task_id="build_dim_category",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "dim_category "
                    "incremental"
                ),
            )

        with TaskGroup(group_id="dim_scd2") as gold_dim_scd2:
            build_dim_customers = BashOperator(
                task_id="build_dim_customers",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "dim_customers "
                    "incremental"
                ),
            )

            build_dim_products = BashOperator(
                task_id="build_dim_products",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "dim_products "
                    "incremental"
                ),
            )

            build_dim_country >> build_dim_customers
            build_dim_category >> build_dim_products

        with TaskGroup(group_id="fact_tables") as gold_facts:
            build_fact_order = BashOperator(
                task_id="build_fact_order",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "fact_order "
                    "incremental"
                ),
            )

            build_fact_order_item = BashOperator(
                task_id="build_fact_order_item",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "fact_order_item "
                    "incremental"
                ),
            )

            build_fact_web_events = BashOperator(
                task_id="build_fact_web_events",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "fact_web_events "
                    "incremental"
                ),
            )

            build_fact_review = BashOperator(
                task_id="build_fact_review",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "fact_review "
                    "incremental"
                ),
            )

            build_fact_session = BashOperator(
                task_id="build_fact_session",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "fact_session "
                    "incremental"
                ),
            )

            build_fact_customer_funnel = BashOperator(
                task_id="build_fact_customer_funnel",
                bash_command=(
                    f"{SPARK_SUBMIT_BASE} "
                    "/opt/project/scripts/build_gold.py "
                    "{{ ds }} "
                    "fact_customer_funnel "
                    "incremental"
                ),
            )

        update_gold_watermark = BashOperator(
            task_id="update_gold_watermark",
            bash_command=(
                f"{SPARK_SUBMIT_BASE} "
                "/opt/project/scripts/build_gold.py "
                "{{ ds }} "
                "update_watermark "
                "incremental"
            ),
        )

        gold_dim_static >> gold_dim_scd2 >> gold_facts >> update_gold_watermark

    end = EmptyOperator(task_id="end")

    start >> bronze_ingestion >> silver_transform >> gold_transform >> end

