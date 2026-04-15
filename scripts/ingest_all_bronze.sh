#!/bin/bash
##############################################################################
# Ingest ALL CSV files to Bronze Layer
# 
# Usage:
#   bash ingest_all_bronze.sh [INGEST_DATE]
#
# Example:
#   bash ingest_all_bronze.sh 2026-04-06
##############################################################################

# Lấy ingest_date từ argument hoặc dùng ngày hôm nay
INGEST_DATE=${1:-$(date +%Y-%m-%d)}

echo "=============================================================================="
echo "Batch Ingestion: ALL CSV Files to Bronze Layer"
echo "=============================================================================="
echo "Ingest Date: $INGEST_DATE"
echo "=============================================================================="
echo ""

# Danh sách 7 files CSV cần ingest
declare -a CSV_FILES=(
    "events.csv:events"
    "customers.csv:customers"
    "orders.csv:orders"
    "order_items.csv:order_items"
    "products.csv:products"
    "reviews.csv:reviews"
    "sessions.csv:sessions"
)

SUCCESS_COUNT=0
FAIL_COUNT=0

# Loop qua từng file
for item in "${CSV_FILES[@]}"; do
    # Split filename:dataset_name
    IFS=':' read -r CSV_FILE DATASET_NAME <<< "$item"
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Processing: $CSV_FILE → $DATASET_NAME"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Chạy spark-submit (với MSYS_NO_PATHCONV=1 cho Git Bash trên Windows)
    MSYS_NO_PATHCONV=1 docker exec lakehouse-spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/project/scripts/bronze/ingest_bronze.py \
        "$INGEST_DATE" \
        "$CSV_FILE" \
        "$DATASET_NAME"
    
    # Check exit code
    if [ $? -eq 0 ]; then
        echo "SUCCESS: $CSV_FILE ingested to bronze/$DATASET_NAME"
        ((SUCCESS_COUNT++))
    else
        echo "FAILED: $CSV_FILE ingestion failed"
        ((FAIL_COUNT++))
    fi
done

echo ""
echo "=============================================================================="
echo "Batch Ingestion Summary"
echo "=============================================================================="
echo "Success: $SUCCESS_COUNT files"
echo "Failed:  $FAIL_COUNT files"
echo "Ingest Date: $INGEST_DATE"
echo "=============================================================================="

# Exit với code 1 nếu có file fail
if [ $FAIL_COUNT -gt 0 ]; then
    exit 1
else
    exit 0
fi
