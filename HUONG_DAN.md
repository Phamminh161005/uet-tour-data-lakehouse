# Hướng dẫn Vận hành và Phát triển Hệ thống Data Lakehouse

Đây là tài liệu hướng dẫn chi tiết về cách vận hành, kiểm tra và phát triển các thành phần trong hệ thống data lakehouse cho dự án UET Tour.

## 1. Tổng quan Kiến trúc

Hệ thống bao gồm các thành phần chính sau:
- **Luồng dữ liệu (Data Ingestion):** Kafka và Zookeeper chịu trách nhiệm nhận và đệm dữ liệu sự kiện từ `data_generator.py`.
- **Lưu trữ (Storage):**
    - **Cold Storage:** MinIO (tương thích S3) lưu trữ dữ liệu thô (raw data) dưới định dạng Parquet.
    - **Hot Storage (Kho phân tích):** ClickHouse lưu trữ dữ liệu đã qua xử lý, sẵn sàng cho việc truy vấn và phân tích nhanh.
- **Xử lý (Processing):** Apache Spark Streaming đọc dữ liệu từ Kafka, làm giàu (enrichment), và ghi vào cả MinIO và ClickHouse.
- **Trực quan hóa (Visualization):** Metabase kết nối với ClickHouse để tạo dashboard và biểu đồ phân tích.

Tất cả các dịch vụ được quản lý và điều phối bởi Docker Compose.

## 2. Hướng dẫn Vận hành

### 2.1. Yêu cầu
- Docker Desktop đã được cài đặt và đang chạy.
- Git đã được cài đặt.
- Python 3.x.

### 2.2. Khởi động hệ thống
Mở terminal và thực hiện các lệnh sau từ thư mục gốc của dự án:

```bash
# 1. Build và khởi chạy tất cả các dịch vụ (lần đầu tiên hoặc sau khi có thay đổi)
docker-compose -f infrastructure/docker-compose.yml up -d --build

# 2. (Tùy chọn) Chỉ khởi chạy các dịch vụ nếu không có gì thay đổi
docker-compose -f infrastructure/docker-compose.yml up -d
```

### 2.3. Kiểm tra các dịch vụ
- **Kafka UI:** Truy cập `http://localhost:8088` để xem các topic và message.
- **Spark Master UI:** Truy cập `http://localhost:8081` để xem trạng thái của cluster và các job.
- **MinIO Console:** Truy cập `http://localhost:9001` (user: `admin`, pass: `password123`) để xem các bucket và file dữ liệu.
- **Metabase:** Truy cập `http://localhost:3000` để xây dựng dashboard.

## 3. Quy trình Dữ liệu (End-to-End)

### 3.1. Tạo Topic Kafka
Nếu topic `tour_events` chưa tồn tại, bạn có thể tạo nó qua Kafka UI hoặc bằng lệnh:
```bash
docker-compose -f infrastructure/docker-compose.yml exec kafka kafka-topics --create --topic tour_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3.2. Chạy Data Generator
Script này sẽ giả lập việc người dùng tương tác và gửi dữ liệu vào Kafka.
```bash
python ingestion-and-storage/data_generator.py
```

### 3.3. Chạy Spark Streaming Job
Script này sẽ xử lý dữ liệu từ Kafka và ghi vào các nơi lưu trữ.
```bash
docker-compose -f infrastructure/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.clickhouse:clickhouse-jdbc:0.4.6,org.apache.hadoop:hadoop-aws:3.3.4 \
/opt/spark/work-dir/streaming_job.py
```

## 4. Những gì đã làm được (Feature: analysis-enrichment)

Trong nhánh `feature/analysis-enrichment`, các công việc sau đã được hoàn thành:

1.  **Ổn định hệ thống:**
    -   Giải quyết hàng loạt các lỗi liên quan đến dependencies (Kafka, ClickHouse, S3), quyền ghi trong Docker (Ivy cache), và cấu hình tài nguyên của Spark.
    -   Xây dựng một `Dockerfile` tùy chỉnh cho Spark, cài đặt các thư viện Python cần thiết và tạo user có đủ quyền để chạy các tác vụ.
    -   Tinh chỉnh `docker-compose.yml` để cấp phát đủ tài nguyên cho Spark Worker, tránh tình trạng job bị từ chối.

2.  **Làm giàu dữ liệu (Data Enrichment):**
    -   Sửa đổi Spark Job (`streaming_job.py`) để thêm 2 cột thông tin mới vào mỗi sự kiện:
        -   `processing_timestamp`: Ghi lại thời điểm chính xác dữ liệu được xử lý bởi Spark.
        -   `is_high_value`: Phân loại tour là "Yes" (giá > 1,000,000 VNĐ) hoặc "No", giúp dễ dàng phân tích các tour/giao dịch có giá trị cao.
    -   Cập nhật schema và logic ghi dữ liệu vào ClickHouse và MinIO để bao gồm các cột mới này.

3.  **Tạo nhánh phát triển riêng:** Toàn bộ các thay đổi được thực hiện trên nhánh `feature/analysis-enrichment`, đảm bảo không ảnh hưởng đến nhánh `main`.

## 5. Hướng phát triển tiếp theo

1.  **Hoàn thiện Dashboard Metabase:** Xây dựng các biểu đồ chi tiết hơn dựa trên dữ liệu đã được làm giàu (ví dụ: phân tích các tour giá trị cao theo thời gian, theo khu vực).
2.  **Tích hợp Delta Lake:** Thay thế định dạng Parquet trên MinIO bằng Delta Lake để hỗ trợ các giao dịch ACID, cập nhật và xóa dữ liệu, cũng như du hành thời gian (time travel).
3.  **Triển khai Airflow:** Sử dụng Apache Airflow để tự động hóa và quản lý toàn bộ quy trình dữ liệu (tự động chạy data generator, spark job theo lịch).
4.  **Mở rộng Data Model:** Thêm các nguồn dữ liệu mới (ví dụ: thông tin chi tiết về khách sạn, đánh giá của người dùng) và tích hợp vào mô hình dữ liệu chung.