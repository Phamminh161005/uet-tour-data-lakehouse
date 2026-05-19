"""
fraud_job.py — Thành viên 3: Fraud Detection (Sliding Window)
=============================================================
Nhiệm vụ:
  1. Đọc luồng sự kiện từ Kafka topic "tour_events"
  2. Áp dụng Sliding Window 1 phút / trượt 30 giây
  3. Phát hiện IP spam > 50 sự kiện trong cửa sổ đó
  4. Ghi cảnh báo vào ClickHouse bảng uet_tour.alerts_fraud
     để Metabase hiển thị Dashboard thời gian thực
"""

import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType
)

# ── Kế thừa bảng điều khiển trung tâm ──────────────────────────────────────
import config

# ==========================================
# 1. CẤU HÌNH HỆ ĐIỀU HÀNH (Chống lỗi Winutils — giống streaming_job.py)
# ==========================================
if sys.platform.startswith("win"):
    os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", "D:\\hadoop")
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")


# ==========================================
# 2. KHỞI TẠO SPARK SESSION
# ==========================================
def create_spark_session() -> SparkSession:
    print(f"[FRAUD-{config.NODE_ID}] Khởi tạo SparkSession cho Fraud Detection Job...")

    spark = SparkSession.builder \
        .appName(f"FraudDetection_{config.NODE_ID}") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.clickhouse:clickhouse-jdbc:0.4.6"
        ) \
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==========================================
# 3. SCHEMA SỰ KIỆN (chỉ lấy các trường cần thiết cho fraud)
# ==========================================
def get_event_schema() -> StructType:
    """
    Chỉ khai báo các trường thực sự cần cho fraud detection.
    Spark sẽ bỏ qua các trường còn lại — giảm tải bộ nhớ.
    """
    return StructType([
        StructField("event_id",    StringType(),  True),
        StructField("timestamp",   StringType(),  True),  # ISO-8601 string → parse thành event_time
        StructField("user_id",     StringType(),  True),
        StructField("session_id",  StringType(),  True),
        StructField("event_type",  StringType(),  True),
        StructField("geo_ip",      StringType(),  True),  # ← trường chính để phát hiện bot
        StructField("geo_country", StringType(),  True),
        StructField("geo_city",    StringType(),  True),
        StructField("tour_id",     StringType(),  True),
        StructField("tour_price",  LongType(),    True),
    ])


# ==========================================
# 4. ĐỌC VÀ LÀM SẠCH LUỒNG KAFKA
# ==========================================
def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Đọc raw bytes từ Kafka → parse JSON → trích event_time → lọc null.
    """
    schema = get_event_schema()

    print(f"[FRAUD-{config.NODE_ID}] Đang đăng ký lắng nghe Topic: {config.KAFKA_TOPIC_NAME} "
          f"tại Broker: {config.KAFKA_BROKER}")

    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BROKER) \
        .option("subscribe", config.KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Cast value bytes → chuỗi JSON → parse theo schema
    df_parsed = df_raw \
        .selectExpr("CAST(value AS STRING) AS json_str") \
        .select(from_json(col("json_str"), schema).alias("d")) \
        .select("d.*")

    # Chuyển chuỗi timestamp → kiểu DateTime (Spark dùng để tính window)
    df_clean = df_parsed \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .filter(
            col("event_id").isNotNull() &
            col("geo_ip").isNotNull() &
            col("event_time").isNotNull()
        )

    return df_clean


# ==========================================
# 5. PHÁT HIỆN GIAN LẬN — SLIDING WINDOW
# ==========================================
def detect_fraud(df_clean: DataFrame) -> DataFrame:
    """
    Áp dụng Sliding Window:
      - Cửa sổ: 1 phút
      - Bước trượt: 30 giây  (→ mỗi IP được kiểm tra 2 lần/phút)
      - Watermark: 2 phút    (cho phép dữ liệu trễ tối đa 2 phút)
      - Ngưỡng cảnh báo: > 50 sự kiện trong một cửa sổ

    Kết quả: DataFrame chứa các IP vi phạm cùng khung thời gian.
    """
    fraud_df = df_clean \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "1 minute", "30 seconds"),  # sliding window
            col("geo_ip")
        ) \
        .count() \
        .filter(col("count") > 50)  # Ngưỡng phát hiện Bot

    return fraud_df


# ==========================================
# 6. GHI CẢNH BÁO VÀO CLICKHOUSE (foreachBatch)
# ==========================================
def write_fraud_to_clickhouse(batch_df, batch_id: int):
    """
    Callback của foreachBatch:
      - Tách window.start / window.end thành 2 cột riêng
      - Đổi tên cột count → event_count cho khớp schema bảng
      - Ghi vào uet_tour.alerts_fraud qua JDBC
    """
    if batch_df.isEmpty():
        print(f"[FRAUD-{config.NODE_ID} | Batch {batch_id}] Không có IP bất thường trong lô này.")
        return

    # Tách struct window { start, end } → 2 cột DateTime riêng
    alerts_df = batch_df \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end",   col("window.end")) \
        .withColumnRenamed("count", "event_count") \
        .select("window_start", "window_end", "geo_ip", "event_count")

    # Log ra console để kiểm tra trực quan
    print(f"\n🚨 [FRAUD-{config.NODE_ID} | Batch {batch_id}] Phát hiện {alerts_df.count()} IP bất thường!")
    alerts_df.show(truncate=False)

    # Ghi vào ClickHouse
    alerts_df.write \
        .format("jdbc") \
        .option("url", config.CLICKHOUSE_URL) \
        .option("dbtable", "uet_tour.alerts_fraud") \
        .option("user", config.CLICKHOUSE_USER) \
        .option("password", config.CLICKHOUSE_PASSWORD) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()
    print(f"✅ [FRAUD-{config.NODE_ID} | Batch {batch_id}] Đã ghi cảnh báo vào ClickHouse!")


# ==========================================
# 7. MAIN — KHỞI CHẠY STREAMING QUERY
# ==========================================
def main():
    spark = create_spark_session()

    # Bước 1: Đọc & làm sạch luồng Kafka
    df_clean = read_kafka_stream(spark)

    # Bước 2: Áp dụng sliding window, lọc IP spam
    fraud_df = detect_fraud(df_clean)

    # ====================================================================================
    # TODO 3: Ghi kết quả vào console để debug
    # ====================================================================================
    console_query = fraud_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="30 seconds") \
        .start()

    # ====================================================================================
    # TODO 4: Ghi kết quả vào ClickHouse
    # ====================================================================================
    clickhouse_query = fraud_df \
        .writeStream \
        .foreachBatch(write_fraud_to_clickhouse) \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()

    spark.streams.awaitAnyTermination()
    # ====================================================================================
    # KẾT THÚC
    # ====================================================================================


if __name__ == "__main__":
    main()