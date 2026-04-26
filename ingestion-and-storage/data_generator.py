import json
import uuid
import random
import time
import logging
from datetime import datetime, timedelta, timezone
from faker import Faker
from kafka import KafkaProducer

# Import cấu hình từ file config.py của bạn (Bước 1.4)
import config

# Cài đặt Hệ thống Logging chuẩn doanh nghiệp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

fake = Faker()

# ==========================================
# 1. KHO DỮ LIỆU MẪU
# ==========================================
DESTINATIONS = ["Sapa", "Ha Giang", "Halong", "Ninh Binh", "Danang", "Dalat", "Phu Quoc"]
TOUR_CATALOG = {
    "Sapa": [{"id": "T-SAPA-01", "name": "Sapa Misty Morning", "price": 3500000}],
    "Halong": [{"id": "T-HL-01", "name": "Halong Luxury Cruise", "price": 4500000}],
    "Dalat": [{"id": "T-DL-01", "name": "Dalat Pine Forest", "price": 3000000}]
}

# ==========================================
# 2. CLASS QUẢN LÝ PHIÊN KHÁCH HÀNG
# ==========================================
class UserSession:
    def __init__(self):
        self.session_id = f"S-{uuid.uuid4().hex[:10]}"
        self.user_id = f"U-{fake.random_number(digits=5, fix_len=True)}"
        self.platform = "web"
        self.device_type = random.choice(["desktop", "mobile"])
        self.device_os = random.choice(["Windows", "MacOS"]) if self.device_type == "desktop" else random.choice(["Android", "iOS"])
        self.device_browser = random.choice(["Chrome", "Edge", "Safari"]) if self.device_type == "desktop" else random.choice(["Chrome Mobile", "Safari Mobile"])
        self.geo_ip = fake.ipv4()
        self.geo_country = "VN"
        self.geo_city = random.choice(["Hanoi", "HCM", "Danang", "Cantho", "Haiphong"])
        
        self.current_time = datetime.now(timezone.utc)
        self.current_url = "http://localhost:3000/home"
        self.referrer_url = ""
        
        self.search_destination, self.search_date_from, self.search_date_to = "", "", ""
        self.search_min_budget, self.search_max_budget, self.search_guests = 0, 0, 0
        self.tour_id, self.tour_name, self.tour_price = "", "", 0

    def _advance_time(self, min_sec=10, max_sec=120):
        self.current_time += timedelta(seconds=random.randint(min_sec, max_sec))

    def _build_event(self, event_type):
        return {
            "event_id": f"evt_{uuid.uuid4().hex[:12]}", "timestamp": self.current_time.isoformat(),
            "user_id": self.user_id, "session_id": self.session_id, "event_type": event_type,
            "platform": self.platform, "device_os": self.device_os, "device_browser": self.device_browser,
            "device_type": self.device_type, "geo_ip": self.geo_ip, "geo_country": self.geo_country,
            "geo_city": self.geo_city, "search_destination": self.search_destination,
            "search_min_budget": self.search_min_budget, "search_max_budget": self.search_max_budget,
            "search_guests": self.search_guests, "search_date_from": self.search_date_from,
            "search_date_to": self.search_date_to, "tour_id": self.tour_id,
            "tour_name": self.tour_name, "tour_price": self.tour_price,
            "current_url": self.current_url, "referrer_url": self.referrer_url
        }

    # CÁC HÀNH ĐỘNG
    def do_page_view(self):
        return self._build_event("page_view")

    def do_search(self):
        self._advance_time(15, 60)
        self.referrer_url = self.current_url
        self.search_destination = random.choice(DESTINATIONS)
        self.search_guests = random.randint(1, 5)
        self.search_min_budget = random.choice([0, 1000000])
        self.search_max_budget = self.search_min_budget + random.choice([2000000, 5000000])
        self.current_url = f"http://localhost:3000/tours?dest={self.search_destination}"
        return self._build_event("filter_tours")

    def do_view_detail(self):
        self._advance_time(30, 120)
        self.referrer_url = self.current_url
        catalog = TOUR_CATALOG.get(self.search_destination, TOUR_CATALOG["Dalat"])
        tour = random.choice(catalog)
        self.tour_id, self.tour_name, self.tour_price = tour["id"], tour["name"], tour["price"]
        self.current_url = f"http://localhost:3000/tours/{self.tour_id}"
        return self._build_event("view_tour_detail")

    def do_checkout(self):
        self._advance_time(10, 45)
        self.referrer_url = self.current_url
        self.current_url = f"http://localhost:3000/checkout/{self.tour_id}"
        return self._build_event("initiate_booking")

    def do_payment_success(self):
        self._advance_time(30, 90)
        self.referrer_url = self.current_url
        self.current_url = "http://localhost:3000/success"
        return self._build_event("booking_success")


# ==========================================
# 3. KAFKA PRODUCER & LOGIC ĐIỀU PHỐI (CORE BƯỚC 3)
# ==========================================

def delivery_report(err, msg):
    """Hàm Callback: Được gọi khi Kafka nhận được JSON hoặc xảy ra lỗi"""
    if err is not None:
        logger.error(f"Giao hàng thất bại: {err}")
    else:
        # Tắt log này đi nếu gửi quá nhanh để tránh rác màn hình, nhưng hiện tại cứ để để quan sát
        pass 

def simulate_user_journey(producer, topic):
    """Giả lập 1 phiên hành vi của khách hàng với Phễu xác suất"""
    user = UserSession()
    
    # 1. Luôn luôn bắt đầu bằng Page View
    event = user.do_page_view()
    producer.send(topic, event).add_errback(on_send_error)
    
    # 2. Xác suất 70% đi tìm tour (30% rớt phễu thoát trang)
    if random.random() <= 0.70:
        event = user.do_search()
        producer.send(topic, event).add_errback(on_send_error)
        
        # 3. Xác suất 60% click vào xem chi tiết tour
        if random.random() <= 0.60:
            event = user.do_view_detail()
            producer.send(topic, event).add_errback(on_send_error)
            
            # 4. Xác suất 40% bấm nút Đặt Tour (Checkout)
            if random.random() <= 0.40:
                event = user.do_checkout()
                producer.send(topic, event).add_errback(on_send_error)
                # 5. Xác suất 80% thanh toán thành công (20% xót tiền bỏ giỏ hàng)
                if random.random() <= 0.80:
                    event = user.do_payment_success()
                    producer.send(topic, event).add_errback(on_send_error)
                    logger.info(f"💰 CHỐT ĐƠN: Khách {user.user_id} đã mua tour {user.tour_name}!")
                else:
                    logger.info(f"🛒 RỚT: Khách {user.user_id} bỏ giỏ hàng tour {user.tour_name}.")
            else:
                logger.info(f"👀 XEM: Khách {user.user_id} chỉ xem tour {user.tour_name} rồi thoát.")
        else:
            logger.info(f"🔍 TÌM KIẾM: Khách {user.user_id} tìm {user.search_destination} nhưng không ưng.")
    else:
        logger.info(f"❌ THOÁT NHANH: Khách {user.user_id} vào trang chủ rồi thoát luôn.")

except Exception as e:
        # Bắt mọi lỗi xảy ra khi gửi và in ra thay vì làm sập chương trình
        logger.error(f"⚠️ LỖI GỬI DỮ LIỆU LÊN KAFKA: {e}")
# ==========================================
# 4. VÒNG LẶP CHẠY HỆ THỐNG
# ==========================================
if __name__ == "__main__":
    logger.info("Khởi động Máy phát dữ liệu (Data Generator)...")
    
    # Khởi tạo Kafka Producer với cấu hình từ file config.py
    producer = KafkaProducer(
        bootstrap_servers=[config.KAFKA_BROKER],
        # Tự động chuyển Dict Python thành chuỗi JSON và mã hóa UTF-8
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    topic_name = config.KAFKA_TOPIC_NAME
    
    logger.info(f"Đã kết nối Kafka Broker: {config.KAFKA_BROKER}")
    logger.info(f"Bắt đầu bắn dữ liệu vào Topic: {topic_name}")
    
    try:
        # Vòng lặp vô hạn sinh dữ liệu
        while True:
            simulate_user_journey(producer, topic_name)
            
            # Đợi 1 - 3 giây rồi sinh khách hàng tiếp theo (Tránh quá tải máy)
            time.sleep(random.uniform(1.0, 3.0))
            
    except KeyboardInterrupt:
        logger.info("Đã nhận lệnh Dừng (Ctrl+C). Đang dọn dẹp hệ thống...")
    finally:
        # Bắt buộc phải Flush (Đẩy nốt hàng tồn đọng) trước khi tắt
        logger.info("Đang chờ Kafka gửi nốt các bản tin cuối cùng...")
        producer.flush()
        logger.info("Đã tắt an toàn!")