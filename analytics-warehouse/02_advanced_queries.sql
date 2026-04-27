-- ============================================================
-- FILE: 02_advanced_queries.sql
-- MỤC ĐÍCH: Tập hợp các câu SQL phân tích nâng cao cho ClickHouse
--            Mỗi query có chú thích rõ dùng để vẽ biểu đồ gì
--            trên Metabase, và tại sao dùng hàm đó.
-- ============================================================


-- ================================================================
-- PHẦN A: KPI TỔNG QUAN (Dùng cho "Number" cards trên Dashboard)
-- ================================================================

-- A1. Tổng doanh thu và số booking hôm nay
SELECT
    countIf(event_type = 'booking_success')           AS total_bookings_today,
    sumIf(tour_price, event_type = 'booking_success') AS total_revenue_today,
    countIf(event_type = 'page_view')                 AS total_page_views_today,
    uniqIf(user_id, event_type = 'page_view')         AS unique_users_today
FROM uet_tour.tour_events
WHERE event_date = today();


-- A2. Tỷ lệ chuyển đổi (Conversion Rate) = booking / unique users vào trang
SELECT
    uniq(user_id)                                                        AS total_visitors,
    uniqIf(user_id, event_type = 'booking_success')                      AS converted_users,
    round(uniqIf(user_id, event_type = 'booking_success')
          / uniq(user_id) * 100, 2)                                      AS conversion_rate_pct
FROM uet_tour.tour_events
WHERE event_date >= today() - 7;  -- 7 ngày gần nhất


-- ================================================================
-- PHẦN B: PHÂN TÍCH PHỄU (FUNNEL) - windowFunnel
-- Dùng để vẽ biểu đồ Bar chart dạng Funnel trên Metabase
-- Hiển thị: bao nhiêu user đi qua từng bước trong luồng đặt tour
--
-- Luồng: page_view → filter_tours → view_tour_detail
--      → initiate_booking → booking_success
-- ================================================================

-- B1. Phễu chuyển đổi toàn bộ trong 7 ngày gần nhất
-- windowFunnel(3600)(timestamp, cond1, cond2, ...) đếm user đi được
-- tối đa bao nhiêu bước liên tiếp trong cửa sổ 3600 giây (1 tiếng)
SELECT
    level,
    count() AS users_reached
FROM (
    SELECT
        user_id,
        windowFunnel(3600)(
            toUnixTimestamp(event_time),
            event_type = 'page_view',
            event_type = 'filter_tours',
            event_type = 'view_tour_detail',
            event_type = 'initiate_booking',
            event_type = 'booking_success'
        ) AS level
    FROM uet_tour.tour_events
    WHERE event_date >= today() - 7
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;


-- B2. Phễu chi tiết từng bước (flatten ra để Metabase dễ đọc)
WITH funnel_data AS (
    SELECT
        user_id,
        windowFunnel(3600)(
            toUnixTimestamp(event_time),
            event_type = 'page_view',
            event_type = 'filter_tours',
            event_type = 'view_tour_detail',
            event_type = 'initiate_booking',
            event_type = 'booking_success'
        ) AS max_level
    FROM uet_tour.tour_events
    WHERE event_date >= today() - 7
    GROUP BY user_id
)
SELECT
    step_name,
    countIf(max_level >= step_num) AS users
FROM funnel_data
CROSS JOIN (
    SELECT 1 AS step_num, 'Xem trang chủ'         AS step_name
    UNION ALL SELECT 2, 'Lọc tour'
    UNION ALL SELECT 3, 'Xem chi tiết tour'
    UNION ALL SELECT 4, 'Bắt đầu đặt tour'
    UNION ALL SELECT 5, 'Đặt tour thành công'
) AS steps
GROUP BY step_name, step_num
ORDER BY step_num;


-- ================================================================
-- PHẦN C: PHÂN TÍCH THEO ĐIỂM ĐẾN (DESTINATION)
-- Dùng để vẽ Bar chart: Top điểm đến được tìm kiếm nhiều nhất
-- ================================================================

-- C1. Top 10 điểm đến được tìm kiếm nhiều nhất
SELECT
    search_destination,
    count()                                                  AS total_searches,
    countIf(event_type = 'booking_success')                  AS total_bookings,
    sumIf(tour_price, event_type = 'booking_success')        AS total_revenue
FROM uet_tour.tour_events
WHERE event_date >= today() - 30
  AND search_destination != ''
GROUP BY search_destination
ORDER BY total_searches DESC
LIMIT 10;


-- C2. Top 10 tour được xem nhiều nhất (view_tour_detail)
SELECT
    tour_id,
    tour_name,
    tour_price,
    countIf(event_type = 'view_tour_detail')    AS total_views,
    countIf(event_type = 'booking_success')     AS total_bookings,
    avgIf(tour_price, event_type = 'booking_success') AS avg_booking_price
FROM uet_tour.tour_events
WHERE event_date >= today() - 30
  AND tour_id != ''
GROUP BY tour_id, tour_name, tour_price
ORDER BY total_views DESC
LIMIT 10;


-- ================================================================
-- PHẦN D: PHÂN TÍCH THIẾT BỊ & NỀN TẢNG
-- Dùng để vẽ Pie chart / Donut chart trên Metabase
-- ================================================================

-- D1. Phân bố loại thiết bị
SELECT
    device_type,
    count()                                                    AS total_events,
    uniq(user_id)                                              AS unique_users,
    round(count() / sum(count()) OVER () * 100, 1)            AS pct
FROM uet_tour.tour_events
WHERE event_date >= today() - 7
GROUP BY device_type
ORDER BY total_events DESC;


-- D2. Phân bố trình duyệt
SELECT
    device_browser,
    uniq(user_id)   AS unique_users,
    count()         AS total_events
FROM uet_tour.tour_events
WHERE event_date >= today() - 7
GROUP BY device_browser
ORDER BY unique_users DESC;


-- D3. Phân bố hệ điều hành
SELECT
    device_os,
    uniq(user_id)   AS unique_users,
    count()         AS total_events
FROM uet_tour.tour_events
WHERE event_date >= today() - 7
GROUP BY device_os
ORDER BY unique_users DESC;


-- ================================================================
-- PHẦN E: PHÂN TÍCH THEO THỜI GIAN
-- Dùng để vẽ Line chart / Area chart trên Metabase
-- ================================================================

-- E1. Số sự kiện và booking theo ngày (30 ngày gần nhất)
SELECT
    event_date,
    count()                                            AS total_events,
    uniq(user_id)                                      AS unique_users,
    countIf(event_type = 'booking_success')            AS total_bookings,
    sumIf(tour_price, event_type = 'booking_success')  AS total_revenue
FROM uet_tour.tour_events
WHERE event_date >= today() - 30
GROUP BY event_date
ORDER BY event_date ASC;


-- E2. Heatmap: Số sự kiện theo giờ trong ngày
-- Dùng để nhận biết giờ cao điểm người dùng online
SELECT
    toHour(event_time)  AS hour_of_day,
    toDayOfWeek(event_time) AS day_of_week,  -- 1=Mon, 7=Sun
    count()             AS total_events
FROM uet_tour.tour_events
WHERE event_date >= today() - 30
GROUP BY hour_of_day, day_of_week
ORDER BY day_of_week, hour_of_day;


-- E3. Doanh thu theo ngày, chia theo điểm đến (dùng vẽ Stacked Area)
SELECT
    event_date,
    search_destination,
    sumIf(tour_price, event_type = 'booking_success') AS revenue
FROM uet_tour.tour_events
WHERE event_date >= today() - 30
  AND search_destination != ''
GROUP BY event_date, search_destination
ORDER BY event_date, revenue DESC;


-- ================================================================
-- PHẦN F: PHÂN TÍCH HÀNH VI NGƯỜI DÙNG (User Behavior)
-- ================================================================

-- F1. Số session trung bình mỗi user (độ trung thành)
SELECT
    round(count(DISTINCT session_id) / count(DISTINCT user_id), 2) AS avg_sessions_per_user,
    round(count() / count(DISTINCT session_id), 2)                 AS avg_events_per_session
FROM uet_tour.tour_events
WHERE event_date >= today() - 30;


-- F2. avgIf - Giá tour trung bình cho các booking thành công theo thành phố
SELECT
    geo_city,
    countIf(event_type = 'booking_success')                     AS total_bookings,
    avgIf(tour_price, event_type = 'booking_success')           AS avg_tour_price,
    sumIf(tour_price, event_type = 'booking_success')           AS total_revenue
FROM uet_tour.tour_events
WHERE event_date >= today() - 30
GROUP BY geo_city
ORDER BY total_bookings DESC
LIMIT 20;


-- F3. Người dùng quay lại (Returning Users) vs Người dùng mới
-- Logic: user có event_date < min(event_date) của họ trong 7 ngày qua
--         → đây là returning user
WITH user_first_seen AS (
    SELECT
        user_id,
        min(event_date) AS first_seen_date
    FROM uet_tour.tour_events
    GROUP BY user_id
)
SELECT
    CASE
        WHEN first_seen_date < today() - 7 THEN 'Returning User'
        ELSE 'New User'
    END                AS user_type,
    count(DISTINCT t.user_id) AS total_users,
    countIf(t.event_type = 'booking_success') AS bookings
FROM uet_tour.tour_events t
JOIN user_first_seen u ON t.user_id = u.user_id
WHERE t.event_date >= today() - 7
GROUP BY user_type;


-- ================================================================
-- PHẦN G: PHÂN TÍCH NGÂN SÁCH TÌM KIẾM
-- Dùng để vẽ Histogram hoặc Bar chart phân bố budget
-- ================================================================

-- G1. Phân bố nhóm ngân sách mà user tìm kiếm
SELECT
    CASE
        WHEN search_max_budget <= 1000000  THEN 'Dưới 1 triệu'
        WHEN search_max_budget <= 3000000  THEN '1 - 3 triệu'
        WHEN search_max_budget <= 5000000  THEN '3 - 5 triệu'
        WHEN search_max_budget <= 10000000 THEN '5 - 10 triệu'
        ELSE 'Trên 10 triệu'
    END                         AS budget_range,
    count()                     AS search_count,
    uniq(user_id)               AS unique_users
FROM uet_tour.tour_events
WHERE event_type = 'filter_tours'
  AND search_max_budget > 0
  AND event_date >= today() - 30
GROUP BY budget_range
ORDER BY search_count DESC;


-- G2. Số khách (search_guests) phổ biến nhất
SELECT
    search_guests,
    count()         AS search_count,
    uniq(user_id)   AS unique_users
FROM uet_tour.tour_events
WHERE event_type = 'filter_tours'
  AND search_guests > 0
GROUP BY search_guests
ORDER BY search_count DESC;


-- ================================================================
-- PHẦN H: KIỂM TRA DỮ LIỆU (Data Quality Check)
-- Chạy sau khi Spark đẩy data vào để xác nhận pipeline hoạt động
-- ================================================================

-- H1. Xem 10 dòng mới nhất
SELECT *
FROM uet_tour.tour_events
ORDER BY event_time DESC
LIMIT 10;


-- H2. Đếm theo event_type để kiểm tra phân bổ
SELECT
    event_type,
    count()                 AS cnt,
    min(event_time)         AS earliest,
    max(event_time)         AS latest
FROM uet_tour.tour_events
GROUP BY event_type
ORDER BY cnt DESC;


-- H3. Kiểm tra có event_id trùng không (dữ liệu duplicate)
SELECT
    event_id,
    count() AS cnt
FROM uet_tour.tour_events
GROUP BY event_id
HAVING cnt > 1
LIMIT 20;
