import json
import uuid
import random
import time
import logging
from datetime import datetime, timedelta, timezone
from faker import Faker
from kafka import KafkaProducer

# ==========================================
# 1. KHỞI TẠO CẤU HÌNH & LOGGING
# ==========================================
# Gọi cấu hình từ file config.py do bạn thiết kế
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
# 2. KHO DỮ LIỆU MẪU
# ==========================================
TRENDING_CONFIG = {
    "Sapa": 28,       # Ứng cử viên số 1
    "Dalat": 25,      # Bám rất sát Sapa
    "Danang": 22,     # Ngựa ô, sẵn sàng vượt lên nếu nhiều người bấm Payment
    "Halong": 15,     # Tầm trung, thỉnh thoảng lọt Top 3
    "Ha Giang": 4,    
    "Ninh Binh": 3,
    "Phu Quoc": 3
}

CURRENT_FLASH_SALE_DEST = None
FLASH_SALE_END_TIME = 0

TOUR_CATALOG = {
    "Sapa": [{"id": "T-SAPA-01", "name": "Sapa Misty Morning", "price": 3500000}],
    "Halong": [{"id": "T-HL-01", "name": "Halong Luxury Cruise", "price": 4500000}],
    "Dalat": [{"id": "T-DL-01", "name": "Dalat Pine Forest", "price": 3000000}],
    "Ha Giang": [{"id": "T-HG-01", "name": "Ha Giang Loop Adventure", "price": 4000000}],
    "Ninh Binh": [{"id": "T-NB-01", "name": "Ninh Binh Trang An", "price": 1500000}],
    "Danang": [{"id": "T-DN-01", "name": "Danang Ba Na Hills", "price": 2500000}],
    "Phu Quoc": [{"id": "T-PQ-01", "name": "Phu Quoc Island Escape", "price": 5500000}]
}


# ==========================================
# 3. CLASS QUẢN LÝ PHIÊN KHÁCH HÀNG
# ==========================================
class UserSession:
    def __init__(self):
        # Sử dụng config.NODE_ID để chống trùng lặp dữ liệu
        self.session_id = f"S_{config.NODE_ID}_{uuid.uuid4().hex[:10]}"
        self.user_id = f"U_{config.NODE_ID}_{fake.random_number(digits=5, fix_len=True)}"
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
            # Khóa chính (Primary Key) an toàn khi đẩy vào ClickHouse
            "event_id": f"evt_{config.NODE_ID}_{uuid.uuid4().hex[:12]}", 
            "timestamp": self.current_time.isoformat(),
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

        global CURRENT_FLASH_SALE_DEST, FLASH_SALE_END_TIME
        
        # Nếu đang trong thời gian Giờ Vàng, 80% khách hàng sẽ đổ xô vào Tour đang giảm giá
        if time.time() < FLASH_SALE_END_TIME and random.random() < 0.8:
            self.search_destination = CURRENT_FLASH_SALE_DEST
        else:
            # Nếu không, dùng trọng số bình thường
            dests = list(TRENDING_CONFIG.keys())
            weights = list(TRENDING_CONFIG.values())
            self.search_destination = random.choices(dests, weights=weights, k=1)[0]
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
# 4. KAFKA PRODUCER & LOGIC ĐIỀU PHỐI
# ==========================================

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Giao hàng thất bại: {err}")
    else:
        pass 

def simulate_user_journey(producer, topic):
    user = UserSession()
    
    try:
        event = user.do_page_view()
        producer.send(topic, value=event)
        
        if random.random() <= 0.70:
            event = user.do_search()
            producer.send(topic, value=event)
            
            if random.random() <= 0.60:
                event = user.do_view_detail()
                producer.send(topic, value=event)
                
                if random.random() <= 0.40:
                    event = user.do_checkout()
                    producer.send(topic, value=event)
                    
                    if random.random() <= 0.80:
                        event = user.do_payment_success()
                        producer.send(topic, value=event)
                        logger.info(f"💰 [TRẠM {config.NODE_ID}] CHỐT ĐƠN: Khách {user.user_id} đã mua tour {user.tour_name}!")
                    else:
                        logger.info(f"🛒 [TRẠM {config.NODE_ID}] RỚT: Khách {user.user_id} bỏ giỏ hàng tour {user.tour_name}.")
                else:
                    logger.info(f"👀 [TRẠM {config.NODE_ID}] XEM: Khách {user.user_id} chỉ xem tour {user.tour_name} rồi thoát.")
            else:
                logger.info(f"🔍 [TRẠM {config.NODE_ID}] TÌM KIẾM: Khách {user.user_id} tìm {user.search_destination} nhưng không ưng.")
        else:
            logger.info(f"❌ [TRẠM {config.NODE_ID}] THOÁT NHANH: Khách {user.user_id} vào trang chủ rồi thoát luôn.")
            
    except Exception as e:
        logger.error(f"⚠️ LỖI GỬI DỮ LIỆU LÊN KAFKA: {e}")

# ==========================================
# 5. HÀM GIẢ LẬP TẤN CÔNG BOT (TV3 - FRAUD DETECTION)
# ==========================================

def generate_random_event() -> dict:
    """Tạo một sự kiện ngẫu nhiên đơn lẻ (dùng cho bot attack)."""
    user = UserSession()
    return user.do_page_view()

def simulate_bot_attack(producer, topic: str):
    """
    Giả lập một cuộc tấn công Bot:
    - Cố định IP = 192.168.99.99
    - Bắn 60 sự kiện liên tục không nghỉ vào Kafka
    - Đủ để vượt ngưỡng 50 event/phút của fraud_job.py
    """
    BOT_IP = "192.168.99.99"
    BOT_EVENT_COUNT = 60  # > 50 → sẽ bị phát hiện bởi Spark

    logger.warning(f"🚨 [TRẠM {config.NODE_ID}] CẢNH BÁO: Bot {BOT_IP} đang tấn công mạng!")

    start_time = datetime.now(timezone.utc)
    for i in range(BOT_EVENT_COUNT):
        bot_event = generate_random_event()
        bot_event["geo_ip"] = BOT_IP  # Ghi đè IP thành địa chỉ của Bot
        bot_event["event_type"] = "page_view"  # Bot chỉ spam page_view
        
        # TẠO TIMESTAMP TĂNG DẦN CHO BOT
        # Giả lập 60 event trong vòng 30 giây -> 1 event mỗi 0.5 giây
        bot_event["timestamp"] = (start_time + timedelta(seconds=i * 0.5)).isoformat()

        try:
            producer.send(topic, value=bot_event)
        except Exception as e:
            logger.error(f"⚠️ Lỗi gửi bot event #{i+1}: {e}")

    producer.flush()  # Đảm bảo toàn bộ 60 event được gửi ngay lập tức
    logger.warning(f"🚨 [TRẠM {config.NODE_ID}] Bot đã bắn xong {BOT_EVENT_COUNT} sự kiện từ IP {BOT_IP}!")


# ==========================================
# 6. VÒNG LẶP CHẠY HỆ THỐNG
# ==========================================
if __name__ == "__main__":
    logger.info(f"Khởi động Trạm phát dữ liệu - Định danh: {config.NODE_ID}")
    
    # Kết nối sử dụng biến từ config.py
    producer = KafkaProducer(
        bootstrap_servers=[config.KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 6, 0)
    )
    
    logger.info(f"Đã kết nối Kafka Broker tại: {config.KAFKA_BROKER}")
    logger.info(f"Bắt đầu bơm dữ liệu vào Topic: {config.KAFKA_TOPIC_NAME}")

    # ── Xác suất kích hoạt Bot mỗi vòng lặp ──────────────────────────
    # Cứ ~10 lần gửi dữ liệu bình thường thì có 1 lần Bot xuất hiện
    BOT_ATTACK_PROBABILITY = 0.10  # 10%

    try:
        loop_count = 0
        while True:
            loop_count += 1
            # Mỗi vòng: 10% cơ hội xuất hiện Bot
            if random.random() < BOT_ATTACK_PROBABILITY:
                logger.info(f"--- Vòng #{loop_count}: Bot được triệu hồi! ---")
                simulate_bot_attack(producer, config.KAFKA_TOPIC_NAME)
                # Sau đợt tấn công, nghỉ ngắn để tránh Kafka quá tải
                time.sleep(random.uniform(2.0, 4.0))
            else:
                simulate_user_journey(producer, config.KAFKA_TOPIC_NAME)
                time.sleep(random.uniform(1.0, 3.0))
            
            # Có 2% cơ hội (hoặc khoảng 1-2 phút 1 lần) xảy ra sự kiện Flash Sale kéo dài 30 giây
            if time.time() > FLASH_SALE_END_TIME and random.random() < 0.02:
                # Chọn một tour ngẫu nhiên ở nhóm đáy bảng để bơm traffic
                CURRENT_FLASH_SALE_DEST = random.choice(["Phu Quoc", "Ninh Binh", "Ha Giang"])
                FLASH_SALE_END_TIME = time.time() + 30 # Sự kiện kéo dài 30 giây
                logger.warning(f"🚀 [FLASH SALE] Bùng nổ traffic! Giảm giá 50% cho tour {CURRENT_FLASH_SALE_DEST} trong 30 giây tới!")
            
            simulate_user_journey(producer, config.KAFKA_TOPIC_NAME)
            time.sleep(random.uniform(0.8, 2.0))
            
    except KeyboardInterrupt:
        logger.info("Đã nhận lệnh Dừng (Ctrl+C). Đang dọn dẹp hệ thống...")
    finally:
        logger.info("Đang chờ Kafka gửi nốt các bản tin cuối cùng...")
        producer.flush()
        logger.info("Đã tắt an toàn!")