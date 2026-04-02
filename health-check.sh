#!/bin/bash

echo "=== Kiểm tra sức khỏe Lakehouse System ==="
echo ""

# Kiểm tra containers
echo "1. Trạng thái containers:"
docker-compose ps
echo ""

# Kiểm tra HDFS
echo "2. Kiểm tra HDFS:"
curl -s http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus | grep -o '"State":"[^"]*"' | head -1
echo ""

# Kiểm tra Spark Master
echo "3. Kiểm tra Spark Master:"
curl -s http://localhost:8080/json/ | grep -o '"status":"[^"]*"' | head -1
echo ""

# Kiểm tra MySQL
echo "4. Kiểm tra MySQL:"
docker-compose exec -T mysql mysqladmin ping -h localhost -u root -prootpassword
echo ""

# Kiểm tra Thrift Server
echo "5. Kiểm tra Thrift Server:"
timeout 5s bash -c 'cat < /dev/null > /dev/tcp/localhost/10000' && echo "Thrift Server: OK" || echo "Thrift Server: FAILED"
echo ""

# Kiểm tra Superset
echo "6. Kiểm tra Superset:"
curl -s -o /dev/null -w "%{http_code}" http://localhost:8088/health | grep -q "200" && echo "Superset: OK" || echo "Superset: FAILED"
echo ""

echo "=== Hoàn tất kiểm tra! ==="