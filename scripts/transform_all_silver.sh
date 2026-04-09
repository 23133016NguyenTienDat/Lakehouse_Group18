#!/bin/bash
# Transform ALL datasets from Bronze to Silver Layer
# Usage: bash transform_all_silver.sh [PROCESS_DATE]

PROCESS_DATE=${1:-$(date +%Y-%m-%d)}

echo "=============================================="
echo "Silver Layer Transformation"
echo "Process Date: $PROCESS_DATE"
echo "=============================================="

SCRIPTS=("clean_events.py" "clean_customers.py" "clean_orders.py" "clean_products.py" "clean_reviews.py" "clean_sessions.py")
SUCCESS=0
FAIL=0

for SCRIPT in "${SCRIPTS[@]}"; do
    echo ""
    echo "--- Running: $SCRIPT ---"
    
    MSYS_NO_PATHCONV=1 docker exec lakehouse-spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages io.delta:delta-spark_2.12:3.3.0 \
        /opt/project/scripts/$SCRIPT \
        "$PROCESS_DATE"
    
    if [ $? -eq 0 ]; then
        echo "SUCCESS: $SCRIPT"
        ((SUCCESS++))
    else
        echo "FAILED: $SCRIPT"
        ((FAIL++))
    fi
done

echo ""
echo "=============================================="
echo "Summary: $SUCCESS success, $FAIL failed"
echo "=============================================="

[ $FAIL -gt 0 ] && exit 1 || exit 0
