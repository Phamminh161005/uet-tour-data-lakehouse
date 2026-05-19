import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    lower,
    trim,
    expr
)
from pyspark.sql.types import *

import config

# ==========================================
# 1. CẤU HÌNH WINDOWS (Winutils)
# ==========================================
if sys.platform.startswith('win'):
    os.environ['HADOOP_HOME'] = os.getenv("HADOOP_HOME", "D:\\hadoop")
    os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], "bin")


# ==========================================
# 2. KHỞI TẠO SPARK SESSION
# ==========================================
def create_spark_session() -> SparkSession:

    print(f"[{config.NODE_ID}] Khởi tạo Dropoff Detection Job...")

    spark = SparkSession.builder \
        .appName(f"DropoffDetection_{config.NODE_ID}") \
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "com.clickhouse:clickhouse-jdbc:0.4.6"
            ])
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
# 3. SCHEMA EVENT
# ==========================================
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


# ==========================================
# 4. ĐỌC STREAM TỪ KAFKA
# ==========================================
def extract_stream(spark: SparkSession):

    schema = get_tour_event_schema()

    print(
        f"[{config.NODE_ID}] "
        f"Đang lắng nghe Kafka: {config.KAFKA_BROKER}"
    )

    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BROKER) \
        .option("subscribe", config.KAFKA_TOPIC_NAME) \
        .option("failOnDataLoss", "false") \
        .load()

    df_value = df_raw.selectExpr("CAST(value AS STRING)")

    df_parsed = df_value.select(
        from_json(col("value"), schema).alias("data")
    ).select("data.*")

    df_clean = df_parsed \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .filter(
            col("event_id").isNotNull() &
            col("session_id").isNotNull() &
            col("event_time").isNotNull() &
            col("event_type").isNotNull()
        )

    # Normalize
    df_clean = df_clean \
        .withColumn("event_type", lower(trim(col("event_type"))))

    return df_clean


# ==========================================
# 5. GHI CLICKHOUSE
# ==========================================
def process_dropoff_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print(
        f"[{config.NODE_ID} - Batch {batch_id}] "
        f"Phát hiện khách hàng bỏ checkout..."
    )

    try:

        batch_df.write \
            .format("jdbc") \
            .option("url", config.CLICKHOUSE_URL) \
            .option("dbtable", "alerts_dropoff") \
            .option("user", config.CLICKHOUSE_USER) \
            .option("password", config.CLICKHOUSE_PASSWORD) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

        print(
            f"[{config.NODE_ID}] "
            f"Đã ghi {batch_df.count()} alerts vào ClickHouse"
        )

    except Exception as e:
        print(f"❌ Lỗi ghi ClickHouse: {e}")


# ==========================================
# 6. MAIN LOGIC
# ==========================================
def main():

    spark = create_spark_session()

    df_clean = extract_stream(spark)

    # ======================================
    # TÁCH LUỒNG
    # ======================================

    df_checkout = df_clean.filter(
        col("event_type") == "initiate_booking"
    )

    df_payment = df_clean.filter(
        col("event_type") == "booking_success"
    )

    # ======================================
    # WATERMARK
    # ======================================

    df_checkout = df_checkout.withWatermark(
        "event_time",
        "10 minutes"
    )

    df_payment = df_payment.withWatermark(
        "event_time",
        "10 minutes"
    )

    # ======================================
    # STREAM-STREAM JOIN
    # ======================================

    joined_df = df_checkout.alias("c").join(
        df_payment.alias("p"),
        expr("""
            c.session_id = p.session_id AND
            p.event_time >= c.event_time AND
            p.event_time <= c.event_time + interval 5 minutes
        """),
        "leftOuter"
    )

    # ======================================
    # LỌC DROPOFF
    # ======================================

    dropoff_df = joined_df.filter(
        col("p.session_id").isNull()
    )

    # ======================================
    # SELECT OUTPUT
    # ======================================

    alerts_df = dropoff_df.select(
        col("c.session_id").alias("session_id"),
        col("c.user_id").alias("user_id"),
        col("c.tour_id").alias("tour_id"),
        col("c.event_time").alias("checkout_time"),
        col("c.tour_price").alias("tour_price")
    )

    # ======================================
    # START STREAM
    # ======================================

    print(
        f"[{config.NODE_ID}] "
        f"Kích hoạt Drop-off Detection Stream..."
    )

    query = alerts_df.writeStream \
        .foreachBatch(process_dropoff_batch) \
        .outputMode("append") \
        .option(
            "checkpointLocation",
            f"s3a://raw-tour-data/checkpoints/dropoff_detection_{config.NODE_ID}/"
        ) \
        .start()

    query.awaitTermination()


# ==========================================
# ENTRYPOINT
# ==========================================
if __name__ == "__main__":
    main()