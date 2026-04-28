# TV3 - Báo cáo Tiến độ (Analytics Warehouse)

**Thành viên:** TV3 — Analytics Engineer & BI  
**Ngày:** 2026-04-27  
**Lãnh địa:** `analytics-warehouse/`

---

## ✅ Đã làm được

### 1. `01_clickhouse_schema.sql` — Thiết kế Schema ClickHouse

| Thành phần | Chi tiết |
|---|---|
| Database | `uet_tour` |
| Bảng chính | `tour_events` — MergeTree, partition by `event_date` |
| Các cột | Đầy đủ 23 trường khớp 100% với JSON schema từ TV1 |
| Kiểu dữ liệu | Tối ưu: `LowCardinality` cho enum-like, `UInt32/UInt8` cho số |
| Materialized View 1 | `mv_booking_daily` → `booking_daily_mv` (SummingMergeTree) — tổng hợp booking & revenue theo ngày/tour |
| Materialized View 2 | `mv_hourly_events` → `hourly_events_mv` (SummingMergeTree) — đếm sự kiện theo giờ |
| Query kiểm tra | Có lệnh `DESCRIBE`, `SHOW TABLES`, `TRUNCATE` để tiện dùng |

**Tại sao dùng MergeTree + PARTITION BY?**  
→ ClickHouse đọc nhanh nhất khi lọc theo partition key (`event_date`). Với dữ liệu streaming liên tục, partition theo ngày giúp query "hôm nay" chỉ scan 1 partition thay vì quét toàn bảng.

**Tại sao có Materialized View?**  
→ Spark đẩy dữ liệu raw vào `tour_events`. MV tự động tính toán aggregate và ghi vào bảng phụ. Metabase chỉ cần đọc bảng phụ → dashboard nhanh hơn nhiều, không tốn CPU mỗi lần load.

---

### 2. `02_advanced_queries.sql` — Bộ câu truy vấn phân tích

| Phần | Tên | Hàm đặc biệt | Dùng để vẽ |
|---|---|---|---|
| A | KPI tổng quan | `countIf`, `sumIf`, `uniqIf` | Number cards |
| B | Phễu chuyển đổi | **`windowFunnel`** | Bar chart (Funnel) |
| C | Top điểm đến & tour | `countIf`, `avgIf` | Horizontal bar |
| D | Phân bố thiết bị | Window `sum() OVER ()` | Pie/Donut chart |
| E | Phân tích theo thời gian | `toHour`, `toDayOfWeek` | Line, Heatmap |
| F | Hành vi người dùng | **`avgIf`**, JOIN | Bar chart, Metric |
| G | Phân bố ngân sách | CASE WHEN | Bar chart |
| H | Kiểm tra dữ liệu | — | Bảng debug |

**Tổng cộng: 17 câu SQL**, bao gồm các hàm phức tạp đặc trưng của ClickHouse:
- `windowFunnel()` — phân tích phễu hành vi người dùng
- `avgIf()`, `sumIf()`, `countIf()` — aggregate có điều kiện, tránh subquery
- `uniq()` — HyperLogLog approximate distinct count, nhanh hơn `COUNT(DISTINCT)` nhiều lần
- `LowCardinality` — dictionary encoding tự động, tiết kiệm RAM

---

### 3. `dashboard_notes.md` — Hướng dẫn Metabase

- Hướng dẫn cài driver ClickHouse cho Metabase (file `.jar`)
- Thiết kế layout 2 tab Dashboard (7 card Tab 1, 7 card Tab 2)
- Hướng dẫn tạo từng loại biểu đồ cụ thể (Funnel, Heatmap, Line+Bar combo)
- Danh sách 4 filter toàn Dashboard
- Cài đặt auto-refresh 1 phút và alert
- Ghi chú kỹ thuật về port, timezone, cách test thủ công

---

## ⏳ Còn thiếu (cần làm thêm)

| # | Việc còn thiếu | Lý do chưa làm |
|---|---|---|
| 1 | Tải file `clickhouse.metabase-driver.jar` | Cần internet + mount vào docker-compose |
| 2 | Kết nối Metabase → ClickHouse thực tế | Cần Docker stack đang chạy |
| 3 | Tạo Dashboard trực tiếp trên Metabase | Cần dữ liệu thật từ TV2 Spark |
| 4 | Screenshot/export Dashboard | Cần Metabase đang chạy |
| 5 | Test end-to-end với dữ liệu thật | Phụ thuộc TV1 (data generator) và TV2 (Spark job) |

---

## 🗂️ File cần làm thêm theo docx (nếu cần)

Theo tài liệu `BTL_Bigdata.docx`, TV3 cần đúng 3 file:

```
analytics-warehouse/
├── 01_clickhouse_schema.sql   ✅ ĐÃ CÓ
├── 02_advanced_queries.sql    ✅ ĐÃ CÓ  
└── dashboard_notes.md         ✅ ĐÃ CÓ
```

**Tất cả 3 file đã hoàn thành về nội dung.** Phần còn lại là thực thi trên môi trường Docker thật và tạo dashboard UI.

---

## 🔗 Phụ thuộc vào TV1 & TV2

TV3 cần các thứ sau từ các thành viên khác:
- **Từ TV1:** File `sample-data.json` ✅ (đã có) — dùng để insert test thủ công vào ClickHouse
- **Từ TV2:** Spark job đẩy data vào ClickHouse — cần đúng tên database `uet_tour`, bảng `tour_events`, host `clickhouse:9000` (native port)

**Lưu ý cho TV2:** Native port của ClickHouse trong Docker là `19000` (mapped từ container's `9000`). Khi Spark kết nối từ trong Docker network, dùng `clickhouse:9000` (internal). Từ máy host, dùng `localhost:19000`.
