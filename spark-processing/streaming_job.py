from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# schema giống sample_data.json
schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("event_type", StringType())

# đọc từ Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "tour-events") \
    .load()

# parse JSON
df_value = df_kafka.selectExpr("CAST(value AS STRING)")

df_parsed = df_value.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# filter
df_clean = df_parsed.filter(
    col("event_id").isNotNull() &
    col("user_id").isNotNull()
)

# output ra console (test trước)
query = df_clean.writeStream \
    .format("parquet") \
    .option("path", "s3a://tour-data/events/") \
    .option("checkpointLocation", "s3a://tour-data/checkpoints/") \
    .outputMode("append") \
    .start()

query.awaitTermination()