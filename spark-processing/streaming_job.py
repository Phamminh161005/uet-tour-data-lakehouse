import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lower, trim, to_date, hour, when
)
from pyspark.sql.types import *

# Nhúng bảng điều khiển trung tâm
import config

# ==========================================
# 1. CẤU HÌNH HỆ ĐIỀU HÀNH (Chống lỗi Winutils)
# ==========================================
# Chỉ set HADOOP_HOME nếu máy đang chạy là Windows
if sys.platform.startswith('win'):
    # Ưu tiên lấy từ .env, nếu không có thì mặc định D:\hadoop
    os.environ['HADOOP_HOME'] = os.getenv("HADOOP_HOME", "D:\\hadoop")
    os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], "bin")

def create_spark_session() -> SparkSession:
    print(f"[{config.NODE_ID}] Khởi tạo SparkSession, kết nối MinIO: {config.MINIO_ENDPOINT}")

    spark = SparkSession.builder \
        .appName(f"TourStreaming_{config.NODE_ID}") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.clickhouse:clickhouse-jdbc:0.4.6") \
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN") 
    return spark

def get_tour_event_schema() -> StructType:
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("device_os", StringType(), True),
        StructField("device_browser", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("geo_ip", StringType(), True),
        StructField("geo_country", StringType(), True),
        StructField("geo_city", StringType(), True),
        StructField("search_destination", StringType(), True),
        # Đã SỬA: Chuyển IntegerType -> LongType để tránh tràn số VND
        StructField("search_min_budget", LongType(), True),
        StructField("search_max_budget", LongType(), True),
        StructField("search_guests", IntegerType(), True),
        StructField("search_date_from", StringType(), True),
        StructField("search_date_to", StringType(), True),
        StructField("tour_id", StringType(), True),
        StructField("tour_name", StringType(), True),
        StructField("tour_price", LongType(), True),
        StructField("current_url", StringType(), True),
        StructField("referrer_url", StringType(), True)
    ])

def extract_and_transform_stream(spark: SparkSession, kafka_broker: str, topic: str):
    schema = get_tour_event_schema()
    print(f"[{config.NODE_ID}] Đang nghe luồng Kafka từ {kafka_broker}, Topic: {topic}")
    
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .load()

    df_value = df_raw.selectExpr("CAST(value AS STRING)")
    df_parsed = df_value.select(from_json(col("value"), schema).alias("data")).select("data.*")

    df_clean = df_parsed \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .filter(
            col("event_id").isNotNull() &
            col("user_id").isNotNull() &
            col("event_time").isNotNull() &
            col("event_type").isNotNull()
        )

    df_clean = df_clean \
        .withColumn("event_type", lower(trim(col("event_type")))) \
        .withColumn("platform", lower(trim(col("platform")))) \
        .withColumn("geo_city", lower(trim(col("geo_city")))) \
        .withColumn("search_destination", lower(trim(col("search_destination"))))

    df_clean = df_clean.withColumn(
        "event_type",
        when(col("event_type") == "view_tour", "view_tour_detail")
        .when(col("event_type") == "add_to_cart", "initiate_booking")
        .when(col("event_type") == "checkout", "initiate_booking")
        .when(col("event_type") == "payment", "booking_success")
        .otherwise(col("event_type"))
    )

    df_clean = df_clean \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("event_hour", hour(col("event_time")))

    df_clean = df_clean.fillna({
        "search_min_budget": 0,
        "search_max_budget": 0,
        "search_guests": 0,
        "tour_price": 0
    })

    return df_clean

def process_batch_sink(batch_df, batch_id):
    if batch_df.isEmpty():
        return
        
    print(f"[{config.NODE_ID} - Batch {batch_id}] Xử lý lô dữ liệu mới...")

    # 1. GHI VÀO MINIO
    minio_path = "s3a://raw-tour-data/processed/events/"
    try:
        batch_df.write.mode("append").partitionBy("event_date").parquet(minio_path)
    except Exception as e:
         print(f"❌ Lỗi ghi MinIO: {e}")

    # 2. GHI VÀO CLICKHOUSE
    gold_df = batch_df.select(
        "event_id", "event_type", "event_date", "event_time",
        "user_id", "session_id", "platform", "device_os", 
        "device_browser", "device_type", "geo_ip", "geo_country", "geo_city",
        "search_destination", "search_min_budget", "search_max_budget", 
        "search_guests", "search_date_from", "search_date_to",
        "tour_id", "tour_name", "tour_price", "current_url", "referrer_url"
    )

    try:
        gold_df.write \
            .format("jdbc") \
            .option("url", config.CLICKHOUSE_URL) \
            .option("dbtable", "tour_events") \
            .option("user", config.CLICKHOUSE_USER) \
            .option("password", config.CLICKHOUSE_PASSWORD) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()
    except Exception as e:
         print(f"❌ Lỗi ghi ClickHouse: {e}")

def main():
    spark = create_spark_session()
    
    # Sử dụng toàn bộ biến từ config.py
    streaming_df = extract_and_transform_stream(spark, config.KAFKA_BROKER, config.KAFKA_TOPIC_NAME)

    print(f"[{config.NODE_ID}] Kích hoạt Data Stream. Nhấn Ctrl+C để dừng...")
    query = streaming_df.writeStream \
        .foreachBatch(process_batch_sink) \
        .option("checkpointLocation", "s3a://raw-tour-data/checkpoints/streaming_job/") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()