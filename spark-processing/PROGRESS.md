# BIGDATA PROJECT - PROGRESS REPORT

## 🧑‍💻 Người phụ trách hiện tại
TV2 (Spark Processing / Integration Debugging)

## 📅 Ngày cập nhật
2026-04-29

# Note: File này do AI gen, cần fact check thêm

---

# 1. 🎯 TỔNG QUAN HỆ THỐNG ĐÃ XÂY DỰNG

Hệ thống theo kiến trúc:

Kafka → Spark Streaming → MinIO (Data Lake) + ClickHouse (Data Warehouse) → Metabase

Các thành phần đã triển khai:

- Kafka (ingestion layer)
- Spark Structured Streaming (processing layer)
- MinIO (data lake storage - Parquet)
- ClickHouse (OLAP warehouse)
- Metabase (BI dashboard)

---

# 2. ✅ NHỮNG GÌ ĐÃ HOÀN THÀNH

---

## 2.2 Spark Streaming (TV2 - hiện tại)

✔ Đã làm:

### Streaming pipeline:
- Read stream từ Kafka (`kafka:29092`)
- Parse JSON bằng schema chuẩn
- Normalize data (lowercase, trim)
- Map event types:
  - view_tour → view_tour_detail
  - payment → booking_success
- Add derived fields:
  - event_date
  - event_hour
- Fill NULL cho numeric fields

Lệnh chạy Job Spark:

```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
--jars /opt/spark/jars/clickhouse-jdbc-0.9.8.jar \
--conf spark.jars.ivy=/tmp/.ivy \
streaming_job.py
```

---

### Data Sink:
✔ Data được ghi ra 3 nơi:
- MinIO (raw parquet)
- MinIO (processed parquet)
- ClickHouse (gold data via JDBC)

---

### ClickHouse integration:
- JDBC writeStream foreachBatch
- Append mode

---

# 3. ❗ VẤN ĐỀ ĐÃ GẶP (CRITICAL ISSUE)

## 3.1 ClickHouse Insert Failure

### Error:

Cannot set null to non-nullable column [tour_id String]

---

## 3.2 Root cause

Event stream có tính chất:

| event_type        | tour_id |
|------------------|--------|
| search/filter     | NULL   |
| view_tour         | OK     |
| booking_success   | OK     |

👉 Nhưng ClickHouse schema đang:

- tour_id = String (NOT NULL)
- tour_name = String (NOT NULL)
- tour_price = UInt32 (NOT NULL)

---

## 3.3 Kết luận

❌ Schema ClickHouse chưa phù hợp với event-driven data  
❌ Thiết kế đang giả định tất cả event có cùng structure

---

# 4. 🔧 GIẢI PHÁP ĐỀ XUẤT

## Option A (NHANH - recommended để chạy demo)

Sửa ClickHouse:

```sql
tour_id Nullable(String),
tour_name Nullable(String),
tour_price Nullable(UInt32)
```