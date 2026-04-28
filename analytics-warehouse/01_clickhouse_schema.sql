-- ============================================================
-- FILE: 01_clickhouse_schema.sql
-- MỤC ĐÍCH: Tạo database và bảng MergeTree để đón dữ liệu
--            được đẩy lên từ Spark Streaming
-- CHẠY: Dán từng khối lệnh vào ClickHouse client hoặc
--        giao diện Play UI tại http://localhost:8123/play
-- ============================================================


-- ----------------------------------------------------------------
-- BƯỚC 1: Tạo database riêng cho dự án
-- ----------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS uet_tour;


-- ----------------------------------------------------------------
-- BƯỚC 2: Bảng chính - lưu toàn bộ sự kiện thô từ Spark
-- ----------------------------------------------------------------
-- Engine MergeTree + ORDER BY (event_date, event_type) giúp
-- ClickHouse sắp xếp dữ liệu vật lý, tăng tốc truy vấn lọc
-- theo ngày và loại sự kiện.
-- PARTITION BY event_date giúp DROP partition nhanh khi cleanup.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS uet_tour.tour_events
(
    -- Định danh sự kiện
    event_id        String,
    event_type      LowCardinality(String),   -- enum-like: page_view, filter_tours, ...
    event_date      Date,                      -- dùng để PARTITION
    event_time      DateTime,                  -- timestamp đầy đủ (UTC)

    -- Người dùng & phiên
    user_id         String,
    session_id      String,

    -- Thiết bị & nền tảng
    platform        LowCardinality(String),
    device_os       LowCardinality(String),
    device_browser  LowCardinality(String),
    device_type     LowCardinality(String),

    -- Địa lý
    geo_ip          String,
    geo_country     LowCardinality(String),
    geo_city        LowCardinality(String),

    -- Tìm kiếm (điền khi event_type = 'filter_tours')
    search_destination  String,
    search_min_budget   UInt32,
    search_max_budget   UInt32,
    search_guests       UInt8,
    search_date_from    String,
    search_date_to      String,

    -- Tour (điền khi event_type IN ('view_tour_detail','initiate_booking','booking_success'))
    tour_id         String,
    tour_name       String,
    tour_price      UInt32,

    -- Điều hướng
    current_url     String,
    referrer_url    String
)
ENGINE = MergeTree()
PARTITION BY event_date
ORDER BY (event_date, event_type, user_id, event_time)
SETTINGS index_granularity = 8192;


-- ----------------------------------------------------------------
-- BƯỚC 3: Bảng Materialized View - tổng hợp booking theo ngày
-- Spark đẩy vào tour_events → MV tự cập nhật booking_daily_mv
-- Metabase truy vấn MV này để hiện KPI nhanh mà không cần
-- GROUP BY lại toàn bộ bảng chính.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS uet_tour.booking_daily_mv
(
    event_date      Date,
    tour_id         String,
    tour_name       String,
    total_bookings  UInt64,
    total_revenue   UInt64
)
ENGINE = SummingMergeTree((total_bookings, total_revenue))
ORDER BY (event_date, tour_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS uet_tour.mv_booking_daily
TO uet_tour.booking_daily_mv
AS
SELECT
    event_date,
    tour_id,
    tour_name,
    countIf(event_type = 'booking_success')  AS total_bookings,
    sumIf(tour_price, event_type = 'booking_success') AS total_revenue
FROM uet_tour.tour_events
GROUP BY event_date, tour_id, tour_name;


-- ----------------------------------------------------------------
-- BƯỚC 4: Bảng Materialized View - số sự kiện theo giờ (heatmap)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS uet_tour.hourly_events_mv
(
    event_date  Date,
    event_hour  UInt8,
    event_type  LowCardinality(String),
    cnt         UInt64
)
ENGINE = SummingMergeTree(cnt)
ORDER BY (event_date, event_hour, event_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS uet_tour.mv_hourly_events
TO uet_tour.hourly_events_mv
AS
SELECT
    event_date,
    toHour(event_time) AS event_hour,
    event_type,
    count()            AS cnt
FROM uet_tour.tour_events
GROUP BY event_date, event_hour, event_type;


-- ----------------------------------------------------------------
-- BƯỚC 5 (TÙY CHỌN): Xóa dữ liệu test, bắt đầu lại sạch
-- ----------------------------------------------------------------
-- TRUNCATE TABLE uet_tour.tour_events;
-- TRUNCATE TABLE uet_tour.booking_daily_mv;
-- TRUNCATE TABLE uet_tour.hourly_events_mv;


-- ----------------------------------------------------------------
-- BƯỚC 6 (KIỂM TRA): Xem cấu trúc bảng đã tạo
-- ----------------------------------------------------------------
-- DESCRIBE uet_tour.tour_events;
-- SHOW TABLES FROM uet_tour;
