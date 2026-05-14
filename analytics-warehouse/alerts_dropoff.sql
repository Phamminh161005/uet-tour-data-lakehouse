-- ============================================================
-- FILE: alerts_dropoff.sql
-- MỤC ĐÍCH: Tập hợp các câu SQL liên quan đến Dropoff Detection cho ClickHouse
--            Mỗi query có chú thích rõ dùng để vẽ biểu đồ gì
--            trên Metabase, và tại sao dùng hàm đó.
-- ============================================================
--
-- ================================================================
-- PHẦN A: Tạo bảng alerts_dropoff để lưu thông tin các phiên có khả năng dropoff
-- ================================================================
CREATE TABLE IF NOT EXISTS uet_tour.alerts_dropoff (
    session_id String,
    user_id String,
    tour_id String,
    checkout_time DateTime,
    tour_price UInt64,
    alert_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (checkout_time, session_id);
-- ================================================================
-- PHẦN B: Câu truy vấn phân tích liên quan đến các phiên dropoff
-- ================================================================
-- B.1: Tổng doanh thu bị mất do dropoff trong ngày hôm nay
-- ================================================================
SELECT sum(tour_price) AS estimated_revenue_loss
FROM uet_tour.alerts_dropoff
WHERE toDate(alert_time) = today();
-- ================================================================
-- B.2: Số lượng dropoff theo từng tour, sắp xếp giảm dần để vẽ biểu đồ cột
-- ================================================================
SELECT tour_id,
    count(*) AS drop_count,
    sum(tour_price) AS lost_revenue
FROM uet_tour.alerts_dropoff
GROUP BY tour_id
ORDER BY drop_count DESC;
-- ================================================================
-- B.3: Các phiên dropoff gần đây nhất để xem chi tiết
-- ================================================================
SELECT user_id,
    tour_id,
    checkout_time,
    alert_time,
    tour_price
FROM uet_tour.alerts_dropoff
ORDER BY alert_time DESC
LIMIT 50;