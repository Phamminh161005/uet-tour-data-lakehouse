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
# Chỉ lấy đúng những trường cần thiết
# để tiết kiệm RAM và tăng tốc xử lý
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
# ==========================================
def get_spark_session():
    """
    Khởi tạo Spark Session.
    Dùng config.py thay vì hardcode credentials
    để cả nhóm dùng chung được.
    """
    return SparkSession.builder \
        .appName(f"HotTour_Trending_{config.NODE_ID}") \
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
# ==========================================
def process_batch_sink(df_batch, batch_id):
    """
    Được gọi mỗi 1 phút khi Spark hoàn thành
    tính toán 1 micro-batch.
    Ghi kết quả ranking vào ClickHouse.
    """
    try:
        # Bỏ qua batch trống
        # Xảy ra khi không có event nào trong window
        if df_batch.isEmpty():
            logger.info(f"⏭️  Batch {batch_id} trống, bỏ qua.")
            return

        # Format DateTime đúng chuẩn ClickHouse
        # ClickHouse DateTime không chấp nhận nanoseconds
        # nên phải format về dạng yyyy-MM-dd HH:mm:ss
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

        # Log bảng xếp hạng ra terminal
        # Giúp theo dõi kết quả trực tiếp khi demo
        logger.info(
            f"🏆 ===== BATCH {batch_id} "
            f"- BẢNG XẾP HẠNG HOT TOUR ====="
        )
        df_batch \
            .orderBy(col("trending_score").desc()) \
            .limit(5) \
            .select(
                "tour_name",
                "total_views",
                "total_checkouts",
                "total_payments",
                "trending_score",
                "hot_level"
            ) \
            .show(truncate=False)

        # Ghi vào ClickHouse
        df_to_write.write \
            .format("jdbc") \
            .option("url",      config.CLICKHOUSE_URL) \
            .option("dbtable",  "trending_tours") \
            .option("user",     config.CLICKHOUSE_USER) \
            .option("password", config.CLICKHOUSE_PASSWORD) \
            .option("driver",
                    "ru.yandex.clickhouse.ClickHouseDriver") \
            .mode("append") \
            .save()

        # Log tóm tắt kết quả
        hot_count = df_batch \
            .filter(col("hot_level") == "🔥 HOT") \
            .count()
        total = df_batch.count()

        logger.info(
            f"✅ Batch {batch_id}: "
            f"Ghi {total} tour ({hot_count} HOT) "
            f"lên ClickHouse thành công."
        )

    except Exception as e:
        logger.error(
            f"❌ Lỗi ghi ClickHouse Batch {batch_id}: {e}"
        )

# ==========================================
# 6. CORE - THUẬT TOÁN HOT TOUR
# ==========================================
def run_trending_analytics(spark):
    logger.info(
        f"🚀 [{config.NODE_ID}] "
        f"Khởi chạy Hot Tour Detection Engine..."
    )

    # --- ĐỌC TỪ KAFKA ---
    # kafka.group.id: Quan trọng nhất
    # Đảm bảo 3 node không đọc trùng data
    # Kafka tự chia partition cho từng node
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                config.KAFKA_BROKER) \
        .option("subscribe",
                config.KAFKA_TOPIC_NAME) \
        .option("kafka.group.id",
                "hot_tour_group") \
        .option("failOnDataLoss",  "false") \
        .option("startingOffsets", "latest") \
        .load()

    # --- PARSE JSON ---
    # Chỉ lấy 6 trường cần thiết từ JSON
    # Lọc bỏ các event không liên quan đến tour
    df_parsed = df_raw \
        .selectExpr("CAST(value AS STRING)") \
        .select(
            from_json(col("value"), event_schema).alias("data")
        ) \
        .select("data.*") \
        .filter(
            # Lọc bỏ null và string rỗng
            col("tour_id").isNotNull() &
            (col("tour_id") != "") &
            col("timestamp").isNotNull() &
            col("tour_name").isNotNull() &
            (col("tour_name") != "")
        )

    # --- SLIDING WINDOW + ĐẾM SỰ KIỆN ---
    # Window 5 phút: nhìn lại 5 phút vừa rồi
    # Slide 1 phút: cập nhật mỗi 1 phút
    # Watermark 2 phút: chấp nhận data đến trễ
    #                   tối đa 2 phút
    windowed_df = df_parsed \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            col("tour_id"),
            col("tour_name")
        ) \
        .agg(
            # Đếm từng loại sự kiện quan trọng
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

            # Tổng tất cả tương tác
            # Dùng để đo độ phủ của tour
            _count("session_id").alias("total_interactions")
        )

    # --- TÍNH ĐIỂM VÀ PHÂN LOẠI ---
    scored_df = windowed_df \
        .withColumn(
            # Công thức tính điểm:
            # View có trọng số thấp nhất (chỉ xem thôi)
            # Checkout có trọng số trung bình (có ý định mua)
            # Payment có trọng số cao nhất (đã chốt đơn)
            "trending_score",
            _round(
                (col("total_views")     * 0.3) +
                (col("total_checkouts") * 0.5) +
                (col("total_payments")  * 1.0),
                2
            )
        ) \
        .withColumn(
            # Phân loại mức độ HOT
            # Ngưỡng dựa trên lượng data generator tạo ra:
            # 3 node × ~1 event/giây × 5 phút = ~900 events
            # Chia 7 tour → ~128 events/tour
            # → score >= 80 là đang thực sự HOT
            "hot_level",
            when(col("trending_score") >= 20.0, "🔥 HOT")
            .when(col("trending_score") >= 10.0,  "⬆️ RISING")
            .when(col("trending_score") >= 5.0,  "➡️ NORMAL")
            .otherwise(                           "⬇️ COLD")
        ) \
        .withColumn(
            # Tỷ lệ chuyển đổi view → checkout
            # Đo mức độ hấp dẫn của tour
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
            # Đo mức độ tin tưởng khi đặt cọc
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
            "trending_score",
            "hot_level",
            "checkout_rate",
            "payment_rate"
        )

    # --- CHECKPOINT & KHỞI ĐỘNG STREAM ---
    checkpoint_path = (
        f"file:///D:/BTL bigdata/spark-processing/"
        f"checkpoints/trending_job_{config.NODE_ID}/"
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
    run_trending_analytics(spark_session)