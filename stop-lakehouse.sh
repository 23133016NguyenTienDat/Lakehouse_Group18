#!/bin/bash

echo "=== Dừng Lakehouse Architecture ==="

# 1. Dừng nhanh tất cả services (Giữ lại container)
# Dùng stop để đóng các kết nối êm đẹp trước
docker-compose stop

echo "=== Đã dừng tất cả services! ==="

# 2. Lựa chọn dọn dẹp (Clean up)
echo "----------------------------------------------------"
echo "CẢNH BÁO: Xóa volumes (-v) sẽ làm mất toàn bộ dữ liệu HDFS và MySQL!"
read -p "Bạn có muốn XÓA containers và VOLUMES không? (y/n): " confirm
echo ""

if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
    echo "Dọn dẹp triệt để (Remove Containers & Volumes)..."
    docker-compose down -v
    echo "Đã xóa sạch sẽ!"
else
    echo "Dọn dẹp nhẹ (Chỉ xóa Containers, giữ lại Data)..."
    docker-compose down
    echo "Đã xóa containers. Dữ liệu trong Volumes vẫn an toàn."
fi