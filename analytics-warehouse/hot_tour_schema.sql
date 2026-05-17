-- ============================================================
-- FILE: hot_tour_schema.sql
-- MỤC ĐÍCH: Tạo bảng cho tính năng Hot Tour Ranking
--           gồm 2 bảng:
--           1. trending_tours: Tour đang hot trong 5 phút
--           2. popular_tours:  Tour phổ biến trong 1 giờ
-- CHẠY: Dán vào ClickHouse Play UI tại
--       http://localhost:8123/play
-- ============================================================


-- ================================================================
-- PHẦN 1: BẢNG TRENDING TOURS
-- Lưu kết quả từ trending_job.py
-- Cập nhật mỗi 1 phút
-- Phản ánh tour đang HOT ngay lúc này
-- ================================================================

CREATE TABLE IF NOT EXISTS uet_tour.trending_tours
(
    -- Cửa sổ thời gian tính toán
    window_start        DateTime,       -- Thời điểm bắt đầu window 5 phút
    window_end          DateTime,       -- Thời điểm kết thúc window 5 phút

    -- Thông tin tour
    tour_id             String,
    tour_name           String,

    -- Số liệu đếm trong window
    total_views         UInt32,         -- Số lượt xem chi tiết tour
    total_checkouts     UInt32,         -- Số lượt bắt đầu đặt tour
    total_payments      UInt32,         -- Số lượt thanh toán thành công
    total_interactions  UInt32,         -- Tổng số tương tác

    -- Điểm số và phân loại
    trending_score      Float32,        -- Điểm trending tổng hợp
    hot_level           String,         -- HOT / RISING / NORMAL / COLD

    -- Tỷ lệ chuyển đổi
    checkout_rate       Float32,        -- % view → checkout
    payment_rate        Float32,        -- % checkout → payment

    -- Metadata
    node_id             String,         -- Node nào ghi (NODE1/NODE2/NODE3)
    updated_at          DateTime        -- Thời điểm ghi vào DB
)
-- ReplacingMergeTree tự động deduplicate theo updated_at
-- Nếu cùng (window_start, tour_id, node_id) thì giữ bản mới nhất
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (window_start, tour_id, node_id)
PARTITION BY toDate(window_start);


-- ================================================================
-- PHẦN 2: BẢNG POPULAR TOURS
-- Lưu kết quả từ popular_job.py
-- Cập nhật mỗi 10 phút
-- Phản ánh tour được yêu thích trong 1 giờ qua
-- ================================================================

CREATE TABLE IF NOT EXISTS uet_tour.popular_tours
(
    -- Cửa sổ thời gian tính toán
    window_start        DateTime,       -- Thời điểm bắt đầu window 1 giờ
    window_end          DateTime,       -- Thời điểm kết thúc window 1 giờ

    -- Thông tin tour
    tour_id             String,
    tour_name           String,

    -- Số liệu đếm trong window
    total_views         UInt32,         -- Số lượt xem chi tiết tour
    total_checkouts     UInt32,         -- Số lượt bắt đầu đặt tour
    total_payments      UInt32,         -- Số lượt thanh toán thành công
    total_interactions  UInt32,         -- Tổng số tương tác

    -- Điểm số
    popular_score       Float32,        -- Điểm popular tổng hợp

    -- Tỷ lệ chuyển đổi
    checkout_rate       Float32,        -- % view → checkout
    payment_rate        Float32,        -- % checkout → payment

    -- Metadata
    node_id             String,         -- Node nào ghi (NODE1/NODE2/NODE3)
    updated_at          DateTime        -- Thời điểm ghi vào DB
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (window_start, tour_id, node_id)
PARTITION BY toDate(window_start);


-- ================================================================
-- KIỂM TRA SAU KHI TẠO
-- ================================================================

-- Xem danh sách bảng trong database uet_tour
SHOW TABLES FROM uet_tour;

-- Xem cấu trúc bảng trending_tours
DESCRIBE uet_tour.trending_tours;

-- Xem cấu trúc bảng popular_tours
DESCRIBE uet_tour.popular_tours;


-- ================================================================
-- DỌN DẸP NẾU CẦN CHẠY LẠI TỪ ĐẦU
-- Bỏ comment khi cần reset
-- ================================================================

-- DROP TABLE IF EXISTS uet_tour.trending_tours;
-- DROP TABLE IF EXISTS uet_tour.popular_tours;