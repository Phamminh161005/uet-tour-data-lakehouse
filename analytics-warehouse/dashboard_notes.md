# Dashboard Notes - Metabase Configuration
**Thành viên 3 | Analytics Warehouse**  
Cập nhật: 2026-04-27

---

## 1. Kết nối Metabase với ClickHouse

### Cài đặt Driver ClickHouse cho Metabase
Metabase chính thức chưa bundle sẵn driver ClickHouse.  
Cần tải file `.jar` về và mount vào container:

**Bước 1:** Tải driver tại:  
https://github.com/ClickHouse/metabase-clickhouse-driver/releases  
→ Tải file `clickhouse.metabase-driver.jar` (phiên bản mới nhất)

**Bước 2:** Thêm volume mount vào `docker-compose.yml`:
```yaml
metabase:
  volumes:
    - metabase_data:/metabase-data
    - ./drivers/clickhouse.metabase-driver.jar:/plugins/clickhouse.metabase-driver.jar
```
Sau đó tạo thư mục và copy file:
```bash
mkdir -p infrastructure/drivers
cp clickhouse.metabase-driver.jar infrastructure/drivers/
```

**Bước 3:** Vào Metabase → Admin → Databases → Add Database:
| Field | Giá trị |
|---|---|
| Database type | ClickHouse |
| Host | `clickhouse` (tên service trong Docker network) |
| Port | `8123` |
| Database name | `uet_tour` |
| Username | `default` |
| Password | *(để trống nếu chưa set)* |

---

## 2. Cấu trúc Dashboard

Dashboard gồm **2 tab chính**:

### Tab 1: 📊 Tổng quan (Executive Summary)
> Dành cho: Sếp, người quản lý — xem KPI nhanh

| Vị trí | Loại Card | Tên | Query Dùng |
|---|---|---|---|
| Hàng 1 (4 số) | Number | Tổng booking hôm nay | A1 |
| Hàng 1 | Number | Doanh thu hôm nay (VNĐ) | A1 |
| Hàng 1 | Number | Unique visitors hôm nay | A1 |
| Hàng 1 | Number | Tỷ lệ chuyển đổi (%) | A2 |
| Hàng 2 | Line Chart | Doanh thu 30 ngày | E1 |
| Hàng 3 | Bar Chart | Top 10 điểm đến | C1 |
| Hàng 3 | Bar Chart | Top tour được xem | C2 |

### Tab 2: 🔍 Phân tích hành vi (Behavior Analysis)
> Dành cho: Data team — hiểu hành vi người dùng sâu hơn

| Vị trí | Loại Card | Tên | Query Dùng |
|---|---|---|---|
| Hàng 1 | Funnel Chart | Phễu chuyển đổi | B2 |
| Hàng 2 | Pie Chart | Phân bố thiết bị | D1 |
| Hàng 2 | Pie Chart | Phân bố trình duyệt | D2 |
| Hàng 2 | Pie Chart | Hệ điều hành | D3 |
| Hàng 3 | Pivot Table | Heatmap giờ cao điểm | E2 |
| Hàng 4 | Bar Chart | Phân bố ngân sách | G1 |
| Hàng 4 | Bar Chart | Số khách phổ biến | G2 |

---

## 3. Hướng dẫn tạo từng Card cụ thể

### Card: Funnel chuyển đổi (quan trọng nhất)
1. New Question → Native query → paste query **B2** từ `02_advanced_queries.sql`
2. Visualization: **Bar chart** (Metabase chưa có Funnel built-in)
3. X-axis: `step_name` | Y-axis: `users`
4. Sort: tắt auto-sort (giữ đúng thứ tự step)
5. Display: bật **Show values on data points**
6. Color: chọn màu gradient từ xanh → cam → đỏ (dùng Custom Colors)

### Card: Heatmap giờ cao điểm
1. Native query → paste query **E2**
2. Visualization: **Pivot Table**
3. Rows: `day_of_week` | Columns: `hour_of_day` | Values: `total_events`
4. Conditional formatting: tô màu theo giá trị

> **Lưu ý**: Metabase không có heatmap chart native. Pivot Table với conditional formatting là cách tốt nhất. Hoặc dùng Superset nếu cần đẹp hơn.

### Card: Top 10 điểm đến
1. Native query → paste query **C1**
2. Visualization: **Bar chart (Horizontal)**
3. X-axis: `total_searches` | Y-axis: `search_destination`
4. Bật **Goal line** ở mức 50

### Card: Doanh thu 30 ngày
1. Native query → paste query **E1**
2. Visualization: **Line + Bar combo**
3. Left axis: `total_revenue` (Line) | Right axis: `total_bookings` (Bar)
4. X-axis: `event_date`

---

## 4. Bộ lọc (Filters) toàn Dashboard

| Filter | Type | Kết nối column | Default |
|---|---|---|---|
| **Khoảng thời gian** | Date Range | `event_date` | Last 7 days |
| **Điểm đến** | Text | `search_destination` | All |
| **Loại thiết bị** | Dropdown | `device_type` | All |
| **Thành phố** | Dropdown | `geo_city` | All |

**Cách thêm filter:** Dashboard → Edit → Add a filter → đặt tên → kết nối với từng card

---

## 5. Auto-refresh và Alert

### Auto-refresh
Dashboard → Edit → **Auto-refresh** → **1 minute**  
(Data từ Spark Streaming đổ về liên tục, cần refresh thường xuyên)

### Alert
Card **Tổng booking hôm nay** → Click 🔔 → Create alert  
Điều kiện: `total_bookings_today` vượt mục tiêu → Notify qua Email/Slack

---

## 6. Checklist hoàn thành TV3

- [x] Tạo database `uet_tour` trong ClickHouse
- [x] Tạo bảng `tour_events` với schema đúng (`01_clickhouse_schema.sql`)
- [x] Tạo Materialized View `booking_daily_mv` và `hourly_events_mv`
- [x] Viết và test các câu query phân tích (`02_advanced_queries.sql`)
- [ ] Tải và mount driver ClickHouse cho Metabase
- [ ] Kết nối Metabase → ClickHouse (test connection thành công)
- [ ] Tạo Dashboard Tab 1: Tổng quan (4 KPI + line chart + 2 bar)
- [ ] Tạo Dashboard Tab 2: Phân tích hành vi (funnel + 3 pie + heatmap + 2 bar)
- [ ] Thêm 4 bộ lọc toàn Dashboard
- [ ] Bật auto-refresh 1 phút
- [ ] Test end-to-end: TV1 data → Kafka → TV2 Spark → ClickHouse → Dashboard cập nhật

---

## 7. Ghi chú kỹ thuật

- **Port ClickHouse**: HTTP `8123` (Metabase dùng), Native `19000` (Spark dùng)
- **Default user**: `default` không có password (môi trường dev)
- **Test query nhanh**: Dùng ClickHouse Play UI tại `http://localhost:8123/play`
- **Múi giờ**: ClickHouse lưu DateTime theo UTC. Vào Metabase → Admin → Localization → set `Asia/Ho_Chi_Minh` để hiển thị giờ VN đúng
- **Dữ liệu test**: Dùng `sample-data.json` từ TV1 insert thủ công để test query trước khi Spark sẵn sàng:
```sql
-- Insert 1 dòng test thủ công vào ClickHouse
INSERT INTO uet_tour.tour_events VALUES (
  'evt_001', 'page_view', today(), now(),
  'U-test-01', 'S-test-01',
  'web', 'Windows', 'Chrome', 'desktop',
  '1.1.1.1', 'VN', 'Hanoi',
  '', 0, 0, 0, '', '',
  '', '', 0,
  'http://localhost:3000/home', ''
);
```
