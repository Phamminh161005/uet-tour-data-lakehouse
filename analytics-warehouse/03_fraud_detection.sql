-- ============================================================
-- FILE: 03_fraud_detection.sql
-- MỤC ĐÍCH: Thành viên 3 — Fraud Detection
--   1. Tạo bảng alerts_fraud để nhận cảnh báo từ Spark
--   2. Câu lệnh kiểm tra / Metabase Dashboard queries
-- CHẠY: DBeaver / ClickHouse Play UI tại http://localhost:8123/play
-- ============================================================


-- ----------------------------------------------------------------
-- MODULE 1 — TẠO BẢNG LƯU CẢNH BÁO
-- ----------------------------------------------------------------
-- MergeTree cơ bản: lưu khung giờ bị spam, IP thủ phạm, số click ảo.
-- alert_time tự động ghi thời điểm Spark phát hiện.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS uet_tour.alerts_fraud
(
    window_start DateTime,       -- Thời điểm bắt đầu cửa sổ 1 phút
    window_end   DateTime,       -- Thời điểm kết thúc cửa sổ 1 phút
    geo_ip       String,         -- Địa chỉ IP bị cảnh báo (thủ phạm)
    event_count  UInt32,         -- Tổng số sự kiện trong cửa sổ đó
    alert_time   DateTime DEFAULT now()  -- Thời điểm Spark ghi cảnh báo
)
ENGINE = MergeTree()
ORDER BY (window_start, geo_ip);


-- ================================================================
-- MODULE 4 — METABASE DASHBOARD QUERIES
-- Dán từng câu SQL vào Metabase → New Question → Native Query
-- ================================================================


-- ----------------------------------------------------------------
-- QUERY 1: Bảng cảnh báo tổng hợp (Table / Gauge trên Metabase)
-- Hiển thị: IP thủ phạm | số lần bị bắt | max click/phút | lần cuối bị phát hiện
-- Gợi ý Metabase: loại biểu đồ "Table", highlight cột event_count màu đỏ
-- ----------------------------------------------------------------
SELECT
    geo_ip                              AS "IP Địa chỉ",
    count()                             AS "Số lần bị phát hiện",
    max(event_count)                    AS "Max sự kiện/phút",
    max(alert_time)                     AS "Lần cuối bị bắt"
FROM uet_tour.alerts_fraud
GROUP BY geo_ip
ORDER BY "Max sự kiện/phút" DESC;


-- ----------------------------------------------------------------
-- QUERY 2: Timeline cảnh báo theo thời gian (Line Chart)
-- Hiển thị: số cảnh báo phát sinh theo từng phút → thấy spike của bot
-- Gợi ý Metabase: loại "Line", trục X = alert_minute, trục Y = total_alerts
-- ----------------------------------------------------------------
SELECT
    toStartOfMinute(alert_time)         AS "Thời điểm (phút)",
    count()                             AS "Số cảnh báo"
FROM uet_tour.alerts_fraud
GROUP BY "Thời điểm (phút)"
ORDER BY "Thời điểm (phút)" ASC;


-- ----------------------------------------------------------------
-- QUERY 3: Top IP spam — dùng cho Gauge / Scorecard
-- Hiển thị số click cao nhất của IP bị nghi ngờ nhất
-- Gợi ý Metabase: loại "Gauge", target = 50 (ngưỡng cảnh báo)
-- ----------------------------------------------------------------
SELECT
    geo_ip                              AS "IP Nguy hiểm nhất",
    max(event_count)                    AS "Số sự kiện tối đa"
FROM uet_tour.alerts_fraud
WHERE geo_ip = '192.168.99.99'   -- IP Bot mô phỏng
GROUP BY geo_ip;


-- ----------------------------------------------------------------
-- QUERY 4: Toàn bộ log cảnh báo (raw) — debug / bảo vệ đồ án
-- ----------------------------------------------------------------
SELECT
    formatDateTime(window_start, '%H:%i:%S') AS "Từ",
    formatDateTime(window_end,   '%H:%i:%S') AS "Đến",
    geo_ip                                   AS "IP Tấn công",
    event_count                              AS "Số sự kiện",
    formatDateTime(alert_time,   '%Y-%m-%d %H:%i:%S') AS "Phát hiện lúc"
FROM uet_tour.alerts_fraud
ORDER BY alert_time DESC
LIMIT 100;


-- ----------------------------------------------------------------
-- TIỆN ÍCH: Reset bảng để test lại
-- ----------------------------------------------------------------
-- TRUNCATE TABLE uet_tour.alerts_fraud;
