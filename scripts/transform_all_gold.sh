#!/bin/bash

PROCESS_DATE=${1:-$(date +%Y-%m-%d)}
MODE=${2:-incremental}

echo "=============================================="
echo "Gold Layer Transformation"
echo "Process Date: $PROCESS_DATE"
echo "Mode: $MODE"
echo "=============================================="
echo ""
echo "--- Running: build_gold.py (table orchestration) ---"

TABLES=(
    "dim_country"
    "dim_source"
    "dim_device"
    "dim_payment_method"
    "dim_date"
    "dim_category"
    "dim_customers"
    "dim_products"
    "fact_order"
    "fact_order_item"
    "fact_web_events"
    "fact_review"
    "fact_session"
    "fact_customer_funnel"
    "update_watermark"
)

SUCCESS_COUNT=0
FAIL_COUNT=0

for TABLE_NAME in "${TABLES[@]}"; do
    echo ""
    echo "--- Running: build_gold.py ($TABLE_NAME) ---"

    MSYS_NO_PATHCONV=1 docker exec lakehouse-spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages io.delta:delta-spark_2.12:3.3.0 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.HDFSLogStore \
        /opt/project/scripts/build_gold.py \
        "$PROCESS_DATE" \
        "$TABLE_NAME" \
        "$MODE"

    if [ $? -eq 0 ]; then
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        echo "[OK] $TABLE_NAME"
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        echo "[FAILED] $TABLE_NAME"
    fi
done

echo ""
echo "=============================================="
echo "Summary: $SUCCESS_COUNT success, $FAIL_COUNT failed"
echo "=============================================="

if [ $FAIL_COUNT -gt 0 ]; then
    exit 1
fi

exit 0

