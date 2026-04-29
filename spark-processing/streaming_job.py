from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    lower, trim,
    to_date, hour,
    when,
    window, count,
    coalesce, lit
)
from pyspark.sql.types import *

# ======================
# 1. SPARK SESSION
# ======================
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ======================
# 2. SCHEMA (MATCH TV3)
# ======================
schema = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("platform", StringType()),

    StructField("device_os", StringType()),
    StructField("device_browser", StringType()),
    StructField("device_type", StringType()),

    StructField("geo_ip", StringType()),
    StructField("geo_country", StringType()),
    StructField("geo_city", StringType()),

    StructField("search_destination", StringType()),
    StructField("search_min_budget", IntegerType()),
    StructField("search_max_budget", IntegerType()),
    StructField("search_guests", IntegerType()),
    StructField("search_date_from", StringType()),
    StructField("search_date_to", StringType()),

    StructField("tour_id", StringType()),
    StructField("tour_name", StringType()),
    StructField("tour_price", IntegerType()),

    StructField("current_url", StringType()),
    StructField("referrer_url", StringType())
])

# ======================
# 3. READ FROM KAFKA
# ======================
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "tour-events") \
    .load()

df_value = df_kafka.selectExpr("CAST(value AS STRING)")

df_parsed = df_value.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ======================
# 4. PARSE TIME
# ======================
df_parsed = df_parsed.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

# ======================
# 5. CLEAN + NORMALIZE
# ======================
df_clean = df_parsed.filter(
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

# ======================
# 6. MAP EVENT TYPE (MATCH TV3)
# ======================
df_clean = df_clean.withColumn(
    "event_type",
    when(col("event_type") == "view_tour", "view_tour_detail")
    .when(col("event_type") == "add_to_cart", "initiate_booking")
    .when(col("event_type") == "checkout", "initiate_booking")
    .when(col("event_type") == "payment", "booking_success")
    .otherwise(col("event_type"))
)

# ======================
# 7. ENRICHMENT
# ======================
df_clean = df_clean \
    .withColumn("event_date", to_date(col("event_time"))) \
    .withColumn("event_hour", hour(col("event_time")))

# ======================
# 8. FILL NULL (CLICKHOUSE SAFE)
# ======================
df_clean = df_clean.fillna({
    "search_min_budget": 0,
    "search_max_budget": 0,
    "search_guests": 0,
    "tour_price": 0
})

# ======================
# 9. FOREACH BATCH
# ======================
def process_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id}")

    # ===== RAW (DATA LAKE) =====
    batch_df.write \
        .mode("append") \
        .partitionBy("event_date") \
        .parquet("s3a://raw-tour-data/raw/events/")

    # ===== PROCESSED =====
    batch_df.write \
        .mode("append") \
        .partitionBy("event_date") \
        .parquet("s3a://raw-tour-data/processed/events/")

    # ===== GOLD DATA (MATCH CLICKHOUSE) =====
    gold_df = batch_df.select(
        "event_id",
        "event_type",
        "event_date",
        "event_time",

        "user_id",
        "session_id",

        "platform",
        "device_os",
        "device_browser",
        "device_type",

        "geo_ip",
        "geo_country",
        "geo_city",

        "search_destination",
        "search_min_budget",
        "search_max_budget",
        "search_guests",
        "search_date_from",
        "search_date_to",

        "tour_id",
        "tour_name",
        "tour_price",

        "current_url",
        "referrer_url"
    )

    # ===== WRITE TO CLICKHOUSE =====
    gold_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/uet_tour") \
        .option("dbtable", "tour_events") \
        .option("user", "default") \
        .option("password", "") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

    # ===== AGG (OPTIONAL) =====
    agg = batch_df.groupBy(
        window(col("event_time"), "1 minute")
    ).agg(
        count("*").alias("event_count")
    )

    agg.write \
        .mode("append") \
        .parquet("s3a://raw-tour-data/agg/events_per_minute/")

# ======================
# 10. START STREAM
# ======================
query = df_clean.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "s3a://raw-tour-data/checkpoints/v3/") \
    .outputMode("append") \
    .start()

query.awaitTermination()