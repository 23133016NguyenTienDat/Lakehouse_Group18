# 🏗️ LAKEHOUSE ARCHITECTURE - Final Project

## 🎯 **Tổng quan**

Dự án lakehouse architecture hoàn chỉnh với:
- **Apache Spark** (Processing layer)
- **HDFS** (Storage layer)
- **Hive Metastore** (Schema management)
- **Spark Thrift Server** (SQL Gateway)
- **Apache Superset** (Visualization layer)
- **MySQL** (Metadata storage)

## ⚡ **Quick Start**

### 🚀 Khởi động hệ thống

**PowerShell (Windows):**
```powershell
# Khởi động tất cả services
docker-compose up -d

# Kiểm tra trạng thái
.\hdfs-status.ps1

# Mở Spark shell
.\spark-shell.ps1
```

**Git Bash (Linux/Mac/Windows):**
```bash
# Khởi động tất cả services
docker-compose up -d

# Hoặc sử dụng script với logic kiểm tra
./start-lakehouse.sh

# Chờ các services sẵn sàng (2-3 phút)
./health-check.sh

# Chạy demo
./demo.sh
```

**📚 Hướng dẫn chi tiết cho PowerShell**: Xem [POWERSHELL-GUIDE.md](./POWERSHELL-GUIDE.md)

### 📊 Truy cập Web UIs
- **Spark Master**: http://localhost:8080
- **Spark Worker**: http://localhost:8081
- **HDFS NameNode**: http://localhost:9870
- **HDFS DataNode**: http://localhost:9864
- **Superset**: http://localhost:8088 (admin/admin)
- **Thrift Server**: localhost:10000

## 🧪 **Demo Scenarios**

### 1. **Quick System Check**
```bash
./demo.sh
```

### 2. **Complete Data Pipeline**
```bash
./demo-pipeline.sh
```
Flows: CSV Upload → HDFS → Spark Processing → Parquet → Superset

### 3. **Manual Data Processing**
```bash
# Upload custom data
docker cp your-data.csv lakehouse-namenode:/tmp/
docker exec lakehouse-namenode hdfs dfs -put /tmp/your-data.csv /user/data/

# Process with Spark SQL
docker exec lakehouse-spark-master /opt/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  -e "SELECT * FROM parquet.\`hdfs://namenode:8020/user/data/your-data.csv\`"
```

## 🛠️ **System Management**

### 📈 Monitoring
```bash
# Check status
docker-compose ps

# View logs
docker-compose logs -f [service-name]

# Health check
./health-check.sh
```

### 🔄 Restart Services
```bash
# Restart all
docker-compose restart

# Restart specific service
docker-compose restart spark-master
```

### 🛑 Stop System
```bash
# Stop all containers
docker-compose down

# Remove everything (including data volumes)
docker-compose down -v
```

## 💼 **For Your Final Project**

### 🎓 **Use Cases**
1. **Big Data Processing**: Upload large CSV/JSON datasets
2. **Data Lake Storage**: Multi-format data storage (CSV, Parquet, JSON)
3. **Real-time Analytics**: Stream processing with Spark
4. **Business Intelligence**: Interactive dashboards with Superset
5. **Data Warehousing**: Schema management with Hive Metastore

### 📊 **Demo Architecture**
```
Data Sources → HDFS Storage → Spark Processing → Superset Visualization
     ↓              ↓              ↓                    ↓
   CSV/JSON    Distributed     SQL Engine         Dashboards
   Files       Storage        Analytics            Charts
```

### 🔧 **Customization**
- Modify `docker-compose.yml` for different configurations
- Add more Spark workers: `docker-compose up -d --scale spark-worker=3`
- Connect external data sources via Superset
- Create custom Spark applications

## 📁 **Project Structure**

```
FinalProject/
├── docker-compose.yml          # Main orchestration file
├── config/                     # Configuration files
│   ├── spark-defaults.conf     # Spark settings
│   ├── hive-site.xml           # Hive configuration
│   └── superset_config.py      # Superset settings
├── demo.sh                     # System demo script
├── demo-pipeline.sh            # Data pipeline demo
├── health-check.sh             # Health monitoring
├── start-lakehouse.sh          # Startup script
├── stop-lakehouse.sh           # Shutdown script
├── Makefile                    # Make commands
└── README.md                   # This file
```

## 🎯 **Success Criteria**

✅ **Core Components Running**
- [x] HDFS cluster (NameNode + DataNode)
- [x] Spark cluster (Master + Worker)
- [x] Hive Metastore with MySQL
- [x] Spark Thrift Server (SQL Gateway)
- [x] Superset BI platform

✅ **Functionality Demonstrated**
- [x] Data upload to HDFS
- [x] Spark SQL processing
- [x] Multi-format support (CSV, Parquet)
- [x] Web UI access and monitoring
- [x] End-to-end data pipeline

✅ **Production Ready**
- [x] Docker containerization
- [x] Service dependencies managed
- [x] Health checks implemented
- [x] Networking configured
- [x] Volume persistence

## 🚀 **Next Steps**

1. **Run the demos** to understand the architecture
2. **Upload your own datasets** for processing
3. **Create custom Spark jobs** for your use case
4. **Build dashboards** in Superset for visualization
5. **Document your findings** for the final report

## 🎉 **Congratulations!**

You now have a **complete lakehouse architecture** ready for your final project. The system demonstrates modern big data processing capabilities with:

- **Scalable storage** (HDFS)
- **Distributed processing** (Spark)
- **Schema management** (Hive)
- **SQL interface** (Thrift Server)
- **Business intelligence** (Superset)

**Perfect for demonstrating enterprise-grade data architecture concepts!** 🎓

---

## 🔧 **Các vấn đề đã được khắc phục**

✅ **HDFS persistence** - Hệ thống không format lại HDFS mỗi lần khởi động, dữ liệu được bảo toàn
✅ **Hive Metastore** - Đã cấu hình MySQL connector đúng cách
✅ **DataNode connectivity** - DataNode kết nối đúng với NameNode
✅ **Spark Thrift Server** - SQL interface hoạt động ổn định
✅ **PowerShell support** - Hỗ trợ đầy đủ cho Windows PowerShell
✅ **Spark version compatibility** - Đã điều chỉnh version phù hợp giữa Spark và Hive

---

### 📞 **Support**
If you encounter issues:
1. Check `docker-compose logs [service-name]`
2. Verify all ports are available
3. Ensure Docker has sufficient resources (8GB+ RAM)
4. Run `./health-check.sh` for diagnostics

**Good luck with your final project!** 🍀