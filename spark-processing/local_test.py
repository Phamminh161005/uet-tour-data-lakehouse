from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp

# 1. Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("LocalTest-TourEvents") \
    .getOrCreate()

# 2. Định nghĩa schema JSON (RẤT QUAN TRỌNG)
schema = StructType([
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
    StructField("referrer_url", StringType(), True),
])

# 3. Đọc dữ liệu từ file JSON mẫu
df_raw = spark.read \
    .schema(schema) \
    .json("sample_data.json")

print("=== RAW DATA ===")
df_raw.show(truncate=False)

# 4. Clean & Transform

df_clean = df_raw \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .drop("timestamp") \
    .filter(col("event_id").isNotNull()) \
    .filter(col("user_id").isNotNull()) \
    .filter(col("search_destination").isNotNull())

# 5. Chọn các cột cần thiết (Gold Data cho ClickHouse)

df_gold = df_clean.select(
    "event_id",
    "event_time",
    "user_id",
    "session_id",
    "event_type",
    "platform",

    "device_os",
    "device_browser",
    "device_type",

    "geo_country",
    "geo_city",

    "search_destination",
    "search_min_budget",
    "search_max_budget",
    "search_guests",

    "tour_id",
    "tour_name",
    "tour_price"
)

print("=== CLEANED DATA ===")
df_gold.show(truncate=False)

# 6. Một số phân tích đơn giản (giống bước chuẩn bị cho BI)

print("=== EVENT COUNT BY TYPE ===")
df_gold.groupBy("event_type").count().show()

print("=== TOP DESTINATIONS ===")
df_gold.groupBy("search_destination").count().orderBy(col("count").desc()).show()

# 7. Kết thúc
spark.stop()