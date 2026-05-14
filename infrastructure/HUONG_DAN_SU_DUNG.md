# Tài liệu Hướng dẫn Vận hành - Hệ thống Phát hiện Gian lận

## 1. Tổng quan

Hệ thống này được thiết kế để phát hiện các hành vi gian lận (cụ thể là tấn công bot) trong luồng dữ liệu sự kiện theo thời gian thực.

**Luồng dữ liệu hoạt động như sau:**
1.  **Data Generator (`data_generator.py`)**: Một script Python mô phỏng việc tạo ra dữ liệu sự kiện người dùng và các cuộc tấn công bot, sau đó gửi vào Kafka.
2.  **Kafka**: Đóng vai trò là hàng đợi tin nhắn (message broker), tiếp nhận và lưu trữ tạm thời luồng dữ liệu sự kiện.
3.  **Spark Streaming (`fraud_job.py`)**: "Bộ não" của hệ thống, đọc dữ liệu từ Kafka, áp dụng logic cửa sổ trượt để phát hiện các IP có số lượng sự kiện bất thường (>50 sự kiện/phút).
4.  **ClickHouse**: Một cơ sở dữ liệu phân tích hiệu năng cao, dùng để lưu trữ các cảnh báo gian lận do Spark phát hiện.
5.  **Metabase**: Công cụ trực quan hóa, kết nối với ClickHouse để hiển thị và phân tích các cảnh báo một cách thân thiện.
6.  **MinIO**: Dịch vụ lưu trữ tương thích S3, được Spark sử dụng để lưu trữ các điểm kiểm tra (checkpoint) cho streaming job.
7.  **Docker Compose**: Công cụ để điều phối và quản lý toàn bộ các container dịch vụ trên.

## 2. Yêu cầu hệ thống

*   Docker
*   Docker Compose

## 3. Cài đặt và Khởi động

Do một vấn đề đặc thù về cấu hình bộ nhớ của Metabase khi chạy qua Docker Compose trên một số môi trường, quá trình khởi động sẽ gồm 2 bước chính.

**Bước 1: Khởi động các dịch vụ nền (trừ Metabase)**

Mở terminal và di chuyển đến thư mục `infrastructure`, sau đó chạy lệnh:

```bash
# Lệnh này sẽ khởi động tất cả các dịch vụ được định nghĩa trong docker-compose.yml
# ngoại trừ Metabase (do --scale metabase=0)
docker-compose up -d --scale metabase=0
```

**Bước 2: Khởi động Metabase thủ công với cấu hình bộ nhớ**

Sau khi các dịch vụ nền đã chạy, khởi động Metabase trong một terminal khác bằng lệnh `docker run`. Lệnh này đảm bảo Metabase nhận đủ 1GB RAM để hoạt động ổn định.

```bash
# Chạy Metabase với tên container, kết nối vào mạng chung, map cổng và cấp 1GB RAM
docker run -d --name uet-metabase --network uet-data-network -p 3001:3000 -e JAVA_OPTS="-Xmx1g" metabase/metabase:latest
```

Sau khi chạy 2 lệnh trên, toàn bộ hệ thống đã sẵn sàng.

## 4. Hướng dẫn sử dụng

### 4.1. Cấu hình Metabase (Làm lần đầu tiên)

1.  Truy cập Metabase qua trình duyệt tại: **http://localhost:3001**
2.  Làm theo các bước hướng dẫn để tạo tài khoản quản trị.
3.  Tại bước **"Add your data"**, chọn **"ClickHouse"** và điền thông tin kết nối như sau:
    *   **Host**: `clickhouse`
    *   **Port**: `8123`
    *   **Database name**: `uet_tour`
    *   **Username**: `default`
    *   **Password**: (Để trống)
4.  Lưu lại và hoàn tất quá trình cài đặt.

### 4.2. Chạy tác vụ Phát hiện Gian lận

Mở một terminal mới và chạy lệnh sau để gửi tác vụ xử lý của Spark:

```bash
# Lệnh này thực thi spark-submit bên trong container của spark-master
docker exec uet-spark-master /opt/spark/bin/spark-submit \
  --master spark://uet-spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.clickhouse:clickhouse-jdbc:0.5.0 \
  /opt/spark/work-dir/fraud_job.py
```

**Lưu ý:**
*   Script `data_generator.py` đã tự động chạy và đang mô phỏng một cuộc tấn công từ IP `192.168.99.99`.
*   Tác vụ Spark sẽ chạy liên tục để xử lý dữ liệu.

### 4.3. Xem kết quả

1.  Chờ khoảng 2-3 phút để Spark xử lý và ghi dữ liệu vào ClickHouse.
2.  Truy cập lại Metabase tại **http://localhost:3001**.
3.  Vào mục **"Browse data"** -> chọn database **"Uet Tour"**.
4.  Nhấp vào bảng **`alerts_fraud`**. Bạn sẽ thấy các bản ghi cảnh báo về IP `192.168.99.99` đã được ghi lại.
5.  Từ đây, bạn có thể sử dụng các tính năng của Metabase để tạo biểu đồ, dashboard theo dõi.

## 5. Quản lý và Gỡ lỗi

### 5.1. Xem log của một dịch vụ

Để kiểm tra log của một container bất kỳ (ví dụ: `uet-spark-master`), sử dụng lệnh:

```bash
docker logs uet-spark-master
```

### 5.2. Dừng hệ thống

Do Metabase được khởi động thủ công, quá trình dừng cũng cần 2 bước:

```bash
# 1. Dừng các dịch vụ do docker-compose quản lý
docker-compose down

# 2. Dừng và xóa container Metabase
docker stop uet-metabase
docker rm uet-metabase
```