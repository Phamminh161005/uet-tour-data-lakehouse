import os
from dotenv import load_dotenv

# Tải các biến môi trường từ file .env lên hệ thống
load_dotenv()

# 1. Định danh Node
NODE_ID = os.getenv("NODE_ID", "NODE_UNKNOWN")

# 2. Cấu hình Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "tour_events")

# 3. Cấu hình MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

# 4. Cấu hình ClickHouse
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/uet_tour")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")