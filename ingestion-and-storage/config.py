import os
from dotenv import load_dotenv

# Tải các biến môi trường từ file .env lên hệ thống
load_dotenv()

# Gom nhóm cấu hình lại thành các biến hằng số (CONSTANT)
NODE_ID = os.getenv("NODE_ID", "NODE_UNKNOWN") # Bổ sung thêm dòng này
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "tour_events")

# Test thử xem file config đọc được chưa
if __name__ == "__main__":
    print("--- KIỂM TRA CẤU HÌNH ---")
    print(f"Mã Định Danh (Node ID): {NODE_ID}")
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Topic Name: {KAFKA_TOPIC_NAME}")