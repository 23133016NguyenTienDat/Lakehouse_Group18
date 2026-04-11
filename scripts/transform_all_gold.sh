#!/bin/bash
# Transform ALL datasets from Silver to Gold Layer
# Usage: bash transform_all_gold.sh [PROCESS_DATE] [MODE]

PROCESS_DATE=${1:-$(date +%Y-%m-%d)}
MODE=${2:-incremental}

echo "=============================================="
echo "Gold Layer Transformation"
echo "Process Date: $PROCESS_DATE"
echo "Mode: $MODE"
echo "=============================================="
echo ""
echo "--- Running: build_gold.py ---"

MSYS_NO_PATHCONV=1 docker exec lakehouse-spark-master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.3.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.HDFSLogStore \
    /opt/project/scripts/build_gold.py \
    "$PROCESS_DATE" \
    "$MODE"

if [ $? -eq 0 ]; then
    echo ""
    echo "=============================================="
    echo "Summary: 1 success, 0 failed"
    echo "=============================================="
    exit 0
else
    echo ""
    echo "=============================================="
    echo "Summary: 0 success, 1 failed"
    echo "=============================================="
    exit 1
fi
