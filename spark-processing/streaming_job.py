import os
from dotenv import load_dotenv
os.environ['HADOOP_HOME'] = "D:\\hadoop"
os.environ['PATH'] += os.pathsep + "D:\\hadoop\\bin"
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lower, trim, to_date, hour, when, window, count
)
from pyspark.sql.types import *

# Load biến môi trường từ file .env
load_dotenv()

def create_spark_session() -> SparkSession:
    """
    Khởi tạo SparkSession và cấu hình kết nối MinIO (S3).
    """
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000") # Sửa lại localhost cho hợp lý khi chạy local test
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "password123")

    print(f"Bắt đầu khởi tạo SparkSession, kết nối tới MinIO: {minio_endpoint}")

    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming_UET") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.clickhouse:clickhouse-jdbc:0.4.6") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN") # Giảm bớt log rác của Spark
    return spark

def get_tour_event_schema() -> StructType:
    """
    Định nghĩa Schema chuẩn cho một bản ghi sự kiện (Tour Event) từ Kafka.
    """
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
        StructField("search_min_budget", IntegerType(), True),
        StructField("search_max_budget", IntegerType(), True),
        StructField("search_guests", IntegerType(), True),
        StructField("search_date_from", StringType(), True),
        StructField("search_date_to", StringType(), True),

        StructField("tour_id", StringType(), True),
        StructField("tour_name", StringType(), True),
        StructField("tour_price", IntegerType(), True),

        StructField("current_url", StringType(), True),
        StructField("referrer_url", StringType(), True)
    ])

def extract_and_transform_stream(spark: SparkSession, kafka_broker: str, topic: str):
    """
    Đọc dữ liệu từ Kafka, phân tích cú pháp JSON và thực hiện làm sạch cơ bản.
    """
    schema = get_tour_event_schema()

    # 1. Kết nối Kafka (Extract)
    print(f"Đang kết nối tới Kafka: {kafka_broker}, Topic: {topic}")
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .load()

    # Chuyển đổi dữ liệu nhị phân thành chuỗi
    df_value = df_raw.selectExpr("CAST(value AS STRING)")

    # 2. Phân tích cú pháp (Parse JSON)
    df_parsed = df_value.select(
        from_json(col("value"), schema).alias("data")
    ).select("data.*")

    # 3. Làm sạch và Chuẩn hóa (Transform)
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

    # Ánh xạ lại tên Event cho khớp với yêu cầu của Metabase/BI (Thành viên 3)
    df_clean = df_clean.withColumn(
        "event_type",
        when(col("event_type") == "view_tour", "view_tour_detail")
        .when(col("event_type") == "add_to_cart", "initiate_booking")
        .when(col("event_type") == "checkout", "initiate_booking")
        .when(col("event_type") == "payment", "booking_success")
        .otherwise(col("event_type"))
    )

    # Thêm các cột phân tích thời gian
    df_clean = df_clean \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("event_hour", hour(col("event_time")))

    # Điền giá trị mặc định cho các cột kiểu Số (Tránh lỗi Null trong ClickHouse)
    df_clean = df_clean.fillna({
        "search_min_budget": 0,
        "search_max_budget": 0,
        "search_guests": 0,
        "tour_price": 0
    })

    return df_clean

def process_batch_sink(batch_df, batch_id):
    """
    Hàm này xử lý việc lưu trữ từng lô dữ liệu (batch) vào MinIO và ClickHouse.
    """
    # Nếu batch trống (không có dữ liệu mới), bỏ qua để tiết kiệm tài nguyên
    if batch_df.isEmpty():
        return
        
    print(f"[{batch_id}] Bắt đầu xử lý Batch mới...")

    # 1. GHI VÀO MINIO (Kho lạnh - Data Lake)
    # Lưu dưới định dạng Parquet, phân vùng theo ngày để sau này query nhanh
    minio_path = "s3a://raw-tour-data/processed/events/"
    try:
        batch_df.write \
            .mode("append") \
            .partitionBy("event_date") \
            .parquet(minio_path)
    except Exception as e:
         print(f"Lỗi ghi vào MinIO: {e}")

    # 2. CHUẨN BỊ DỮ LIỆU "VÀNG" CHO CLICKHOUSE (Kho nóng - Data Warehouse)
    # Cần select đúng thứ tự và số lượng cột khớp với bảng đã tạo trong ClickHouse
    gold_df = batch_df.select(
        "event_id", "event_type", "event_date", "event_time",
        "user_id", "session_id", "platform", "device_os", 
        "device_browser", "device_type", "geo_ip", "geo_country", "geo_city",
        "search_destination", "search_min_budget", "search_max_budget", 
        "search_guests", "search_date_from", "search_date_to",
        "tour_id", "tour_name", "tour_price", "current_url", "referrer_url"
    )

    clickhouse_url = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/uet_tour") # Sửa lại localhost cho chạy local
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_pass = os.getenv("CLICKHOUSE_PASSWORD", "")

    # 3. GHI VÀO CLICKHOUSE
    try:
        gold_df.write \
            .format("jdbc") \
            .option("url", clickhouse_url) \
            .option("dbtable", "tour_events") \
            .option("user", clickhouse_user) \
            .option("password", clickhouse_pass) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()
    except Exception as e:
         print(f"Lỗi ghi vào ClickHouse: {e}")

def main():
    """
    Hàm thực thi chính của ứng dụng Spark Streaming.
    """
    # Thông tin kết nối Kafka
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
    KAFKA_TOPIC = "tour_events" # Khớp với thông tin Thành viên 1 đã bàn giao

    # 1. Khởi tạo
    spark = create_spark_session()

    # 2. Xử lý luồng
    streaming_df = extract_and_transform_stream(spark, KAFKA_BROKER, KAFKA_TOPIC)

    # 3. Kích hoạt luồng và đưa vào Sink
    print("Bắt đầu khởi động Data Stream...")
    query = streaming_df.writeStream \
        .foreachBatch(process_batch_sink) \
        .option("checkpointLocation", "s3a://raw-tour-data/checkpoints/streaming_job/") \
        .outputMode("append") \
        .start()

    # Giữ cho tiến trình chạy liên tục
    query.awaitTermination()

if __name__ == "__main__":
    # Điểm bắt đầu của chương trình Python
    main()