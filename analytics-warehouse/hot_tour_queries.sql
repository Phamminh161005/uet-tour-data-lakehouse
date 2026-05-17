-- ============================================================
-- FILE: hot_tour_queries.sql
-- MỤC ĐÍCH: Các câu query dùng trực tiếp trên Metabase
--           cho tính năng Hot Tour Ranking
--           Mỗi query = 1 card trên Dashboard
-- LƯU Ý: Dùng argMax thay vì sum/FINAL
--        vì mỗi window_start khác nhau
--        sẽ tạo ra nhiều dòng cho cùng 1 tour
-- ============================================================


-- ================================================================
-- CARD 1: Bảng xếp hạng Trending Tours
-- Loại biểu đồ: Table
-- Cập nhật: mỗi 1 phút
-- Ý nghĩa: Tour đang HOT trong 5 phút gần nhất
-- ================================================================
SELECT
    tour_name,
    argMax(total_views,       updated_at) AS views,
    argMax(total_checkouts,   updated_at) AS checkouts,
    argMax(total_payments,    updated_at) AS payments,
    argMax(trending_score,    updated_at) AS score,
    argMax(hot_level,         updated_at) AS hot_level,
    argMax(checkout_rate,     updated_at) AS checkout_rate,
    argMax(payment_rate,      updated_at) AS payment_rate,
    max(updated_at)                       AS cap_nhat_luc
FROM uet_tour.trending_tours
WHERE window_start >= now() - INTERVAL 5 MINUTE
GROUP BY tour_name
ORDER BY score DESC
LIMIT 10;


-- ================================================================
-- CARD 2: Bảng xếp hạng Popular Tours
-- Loại biểu đồ: Table
-- Cập nhật: mỗi 10 phút
-- Ý nghĩa: Tour được yêu thích trong 1 giờ gần nhất
-- ================================================================
SELECT
    tour_name,
    argMax(total_views,     updated_at) AS views,
    argMax(total_checkouts, updated_at) AS checkouts,
    argMax(total_payments,  updated_at) AS payments,
    argMax(popular_score,   updated_at) AS score,
    argMax(checkout_rate,   updated_at) AS checkout_rate,
    argMax(payment_rate,    updated_at) AS payment_rate,
    max(updated_at)                     AS cap_nhat_luc
FROM uet_tour.popular_tours
WHERE window_start >= now() - INTERVAL 1 HOUR
GROUP BY tour_name
ORDER BY score DESC
LIMIT 10;


-- ================================================================
-- CARD 3: So sánh Trending vs Popular
-- Loại biểu đồ: Bar chart grouped
-- Ý nghĩa: Thấy ngay sự khác biệt
--          Tour đang HOT chưa chắc là Popular và ngược lại
--          Ví dụ: Ha Giang Flash Sale → HOT nhưng chưa Popular
-- ================================================================
SELECT
    tour_name,
    argMax(trending_score, updated_at) AS trending_score,
    0                                  AS popular_score
FROM uet_tour.trending_tours
WHERE window_start >= now() - INTERVAL 5 MINUTE
GROUP BY tour_name

UNION ALL

SELECT
    tour_name,
    0                                 AS trending_score,
    argMax(popular_score, updated_at) AS popular_score
FROM uet_tour.popular_tours
WHERE window_start >= now() - INTERVAL 1 HOUR
GROUP BY tour_name

ORDER BY tour_name;


-- ================================================================
-- CARD 4: Tỷ lệ chuyển đổi theo tour (Trending)
-- Loại biểu đồ: Bar chart horizontal
-- Ý nghĩa: Tour nào đang chuyển đổi tốt nhất
--          checkout_rate cao = tour hấp dẫn
--          payment_rate cao  = khách tin tưởng
-- ================================================================
SELECT
    tour_name,
    argMax(checkout_rate, updated_at) AS checkout_rate,
    argMax(payment_rate,  updated_at) AS payment_rate,
    argMax(hot_level,     updated_at) AS hot_level
FROM uet_tour.trending_tours
WHERE window_start >= now() - INTERVAL 5 MINUTE
GROUP BY tour_name
ORDER BY checkout_rate DESC
LIMIT 10;


-- ================================================================
-- CARD 5: Lịch sử trending score theo thời gian
-- Loại biểu đồ: Line chart
-- Ý nghĩa: Thấy được xu hướng tăng/giảm của từng tour
--          Đặc biệt rõ khi có Flash Sale
-- ================================================================
SELECT
    window_start,
    tour_name,
    trending_score
FROM uet_tour.trending_tours
WHERE window_start >= now() - INTERVAL 30 MINUTE
ORDER BY window_start ASC, trending_score DESC;


-- ================================================================
-- KIỂM TRA DATA (Chạy khi debug)
-- ================================================================

-- Xem data mới nhất trong trending_tours
SELECT
    tour_name,
    trending_score,
    hot_level,
    window_start,
    window_end,
    updated_at,
    node_id
FROM uet_tour.trending_tours
ORDER BY updated_at DESC
LIMIT 20;

-- Xem data mới nhất trong popular_tours
SELECT
    tour_name,
    popular_score,
    window_start,
    window_end,
    updated_at,
    node_id
FROM uet_tour.popular_tours
ORDER BY updated_at DESC
LIMIT 20;

-- Đếm số bản ghi theo tour
-- Nếu 1 tour có quá nhiều dòng → cần dùng argMax
SELECT
    tour_name,
    count() AS so_ban_ghi
FROM uet_tour.trending_tours
GROUP BY tour_name
ORDER BY so_ban_ghi DESC;