import os
import sys
import logging
from datetime import datetime
import config

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, when,
    sum as _sum, count as _count,
    current_timestamp, date_format,
    round as _round, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

# ==========================================
# 1. CẤU HÌNH HỆ ĐIỀU HÀNH
# ==========================================
if sys.platform.startswith('win'):
    os.environ['HADOOP_HOME'] = os.getenv("HADOOP_HOME", "D:\\hadoop")
    os.environ['PATH'] += os.pathsep + os.path.join(
        os.environ['HADOOP_HOME'], "bin"
    )

# ==========================================
# 2. LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ==========================================
# 3. SCHEMA KAFKA
# Giống trending_job, chỉ lấy trường cần thiết
# ==========================================
event_schema = StructType([
    StructField("event_id",   StringType(),    True),
    StructField("timestamp",  TimestampType(), True),
    StructField("session_id", StringType(),    True),
    StructField("event_type", StringType(),    True),
    StructField("tour_id",    StringType(),    True),
    StructField("tour_name",  StringType(),    True),
])

# ==========================================
# 4. SPARK SESSION
# Tên app khác trending_job để phân biệt
# trên Spark UI
# ==========================================
def get_spark_session():
    return SparkSession.builder \
        .appName(f"HotTour_Popular_{config.NODE_ID}") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ) \
        .config("spark.hadoop.fs.s3a.endpoint",
                config.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key",
                config.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key",
                config.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.host",        "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.network.timeout",            "120s") \
        .getOrCreate()

# ==========================================
# 5. SINK - GHI VÀO CLICKHOUSE
# Ghi vào bảng popular_tours
# khác với trending_job ghi vào trending_tours
# ==========================================
def process_batch_sink(df_batch, batch_id):
    try:
        if df_batch.isEmpty():
            logger.info(f"⏭️  Batch {batch_id} trống, bỏ qua.")
            return

        # Format DateTime đúng chuẩn ClickHouse
        df_to_write = df_batch \
            .withColumn(
                "updated_at",
                date_format(
                    current_timestamp(),
                    "yyyy-MM-dd HH:mm:ss"
                )
            ) \
            .withColumn(
                "window_start",
                date_format(
                    col("window_start"),
                    "yyyy-MM-dd HH:mm:ss"
                )
            ) \
            .withColumn(
                "window_end",
                date_format(
                    col("window_end"),
                    "yyyy-MM-dd HH:mm:ss"
                )
            ) \
            .withColumn("node_id", lit(config.NODE_ID))

        # Log bảng xếp hạng Popular ra terminal
        logger.info(
            f"🌟 ===== BATCH {batch_id} "
            f"- BẢNG XẾP HẠNG POPULAR TOUR ====="
        )
        df_batch \
            .orderBy(col("popular_score").desc()) \
            .limit(5) \
            .select(
                "tour_name",
                "total_views",
                "total_checkouts",
                "total_payments",
                "popular_score",
                "checkout_rate",
                "payment_rate"
            ) \
            .show(truncate=False)

        # Ghi vào bảng popular_tours
        df_to_write.write \
            .format("jdbc") \
            .option("url",      config.CLICKHOUSE_URL) \
            .option("dbtable",  "popular_tours") \
            .option("user",     config.CLICKHOUSE_USER) \
            .option("password", config.CLICKHOUSE_PASSWORD) \
            .option("driver",
                    "ru.yandex.clickhouse.ClickHouseDriver") \
            .mode("append") \
            .save()

        total = df_batch.count()
        logger.info(
            f"✅ Batch {batch_id}: "
            f"Ghi {total} tour phổ biến "
            f"lên ClickHouse thành công."
        )

    except Exception as e:
        logger.error(
            f"❌ Lỗi ghi ClickHouse Batch {batch_id}: {e}"
        )

# ==========================================
# 6. CORE - THUẬT TOÁN POPULAR TOUR
# Khác trending_job ở 3 điểm chính:
# 1. Window 1 giờ thay vì 5 phút
# 2. Slide 10 phút thay vì 1 phút
# 3. Watermark 5 phút thay vì 2 phút
# 4. Ghi vào popular_tours, cột popular_score
# 5. group.id khác để đọc độc lập với trending
# ==========================================
def run_popular_analytics(spark):
    logger.info(
        f"🌟 [{config.NODE_ID}] "
        f"Khởi chạy Popular Tour Detection Engine..."
    )

    # --- ĐỌC TỪ KAFKA ---
    # group.id khác trending_job:
    # "popular_tour_group" ≠ "hot_tour_group"
    #
    # Tại sao dùng group.id KHÁC?
    # popular_job cần đọc TOÀN BỘ data
    # để tính tổng hợp 1 giờ
    # Nếu dùng cùng group với trending_job:
    # → Kafka chia partition giữa 2 job
    # → Mỗi job chỉ thấy 50% data
    # → Cả 2 đều tính sai
    #
    # Dùng group khác:
    # → popular_group đọc độc lập 100% data
    # → hot_tour_group đọc độc lập 100% data
    # → Cả 2 đều tính đúng
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                config.KAFKA_BROKER) \
        .option("subscribe",
                config.KAFKA_TOPIC_NAME) \
        .option("kafka.group.id",
                "popular_tour_group") \
        .option("failOnDataLoss",  "false") \
        .option("startingOffsets", "latest") \
        .load()

    # --- PARSE JSON ---
    df_parsed = df_raw \
        .selectExpr("CAST(value AS STRING)") \
        .select(
            from_json(col("value"), event_schema).alias("data")
        ) \
        .select("data.*") \
        .filter(
            col("tour_id").isNotNull() &
            (col("tour_id") != "") &
            col("timestamp").isNotNull() &
            col("tour_name").isNotNull() &
            (col("tour_name") != "")
        )

    # --- SLIDING WINDOW 1 GIỜ ---
    # Window 1 giờ: nhìn lại 1 giờ vừa rồi
    # Slide 10 phút: cập nhật mỗi 10 phút
    # → Kết quả thay đổi chậm, ổn định hơn trending
    #
    # Watermark 5 phút:
    # → Popular window rộng hơn (1 giờ)
    # → Chấp nhận data trễ nhiều hơn trending (2 phút)
    windowed_df = df_parsed \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy(
            window(col("timestamp"), "1 hour", "10 minutes"),
            col("tour_id"),
            col("tour_name")
        ) \
        .agg(
            _sum(
                when(
                    col("event_type") == "view_tour_detail",
                    1
                ).otherwise(0)
            ).alias("total_views"),

            _sum(
                when(
                    col("event_type") == "initiate_booking",
                    1
                ).otherwise(0)
            ).alias("total_checkouts"),

            _sum(
                when(
                    col("event_type") == "booking_success",
                    1
                ).otherwise(0)
            ).alias("total_payments"),

            _count("session_id").alias("total_interactions")
        )

    # --- TÍNH ĐIỂM POPULAR ---
    # Trọng số giống trending_job
    # Nhưng ngưỡng phân loại khác vì:
    # Window 1 giờ → tích lũy nhiều data hơn
    # → Score cao hơn nhiều so với window 5 phút
    #
    # Ví dụ:
    # trending (5 phút): Sapa score ≈ 15-20
    # popular  (1 giờ):  Sapa score ≈ 180-240
    # → Ngưỡng HOT popular phải cao hơn
    scored_df = windowed_df \
        .withColumn(
            "popular_score",
            _round(
                (col("total_views")     * 0.3) +
                (col("total_checkouts") * 0.5) +
                (col("total_payments")  * 1.0),
                2
            )
        ) \
        .withColumn(
            # Tỷ lệ chuyển đổi view → checkout
            "checkout_rate",
            _round(
                when(
                    col("total_views") > 0,
                    col("total_checkouts") /
                    col("total_views") * 100
                ).otherwise(0),
                1
            )
        ) \
        .withColumn(
            # Tỷ lệ chuyển đổi checkout → payment
            "payment_rate",
            _round(
                when(
                    col("total_checkouts") > 0,
                    col("total_payments") /
                    col("total_checkouts") * 100
                ).otherwise(0),
                1
            )
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "tour_id",
            "tour_name",
            "total_views",
            "total_checkouts",
            "total_payments",
            "total_interactions",
            "popular_score",
            "checkout_rate",
            "payment_rate"
        )

    # --- CHECKPOINT & KHỞI ĐỘNG STREAM ---
    # Dùng tên khác trending_job
    checkpoint_path = (
        f"file:///D:/BTL bigdata/spark-processing/"
        f"checkpoints/popular_job_{config.NODE_ID}/"
    )
    logger.info(f"📁 Checkpoint: {checkpoint_path}")

    query = scored_df.writeStream \
        .foreachBatch(process_batch_sink) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("update") \
        .start()

    query.awaitTermination()


# ==========================================
# 7. ENTRY POINT
# ==========================================
if __name__ == "__main__":
    spark_session = get_spark_session()
    run_popular_analytics(spark_session)