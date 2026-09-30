# Lakehouse Docker Stack

Stack local de dung lakehouse voi MinIO, Spark, Apache Iceberg, Nessie,
Trino, Airflow va Metabase.

## Version matrix

| Component | Version/Image |
| --- | --- |
| MinIO | `quay.io/minio/minio:latest` |
| Spark | `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` |
| Iceberg | `1.11.0` |
| Nessie | `ghcr.io/projectnessie/nessie:0.108.4` |
| Trino | `trinodb/trino:483` |
| Airflow | `apache/airflow:3.3.1-python3.12` |
| Metabase | `metabase/metabase:v0.63.17` |
| Postgres | `postgres:16-alpine` |

Spark 4.2.0 da co release, nhung Iceberg 1.11.0 hien cung cap runtime jar
chinh thuc cho Spark 4.1/4.0/3.5. Vi vay stack pin Spark 4.1.3 de tranh
lech dependency.

Metabase dung official Starburst connection de ket noi Trino. Starburst driver
trong Metabase cung ho tro Trino, nen BI layer se query Iceberg thong qua Trino
catalog `iceberg`.

## Start

```powershell
docker compose up -d --build --remove-orphans
```

Lan dau build Airflow se cai Java, PySpark va Airflow providers nen co the mat
vai phut. Metabase khong can build rieng vi dung image official.

## UI va endpoint

| Service | URL | Login |
| --- | --- | --- |
| MinIO API | http://localhost:9000 | `minioadmin` / `minioadmin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Nessie API | http://localhost:19120 | no auth |
| Spark Master UI | http://localhost:8081 | n/a |
| Spark Worker UI | http://localhost:8083 | n/a |
| Trino | http://localhost:8082 | user `trino` |
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Metabase | http://localhost:3000 | setup lan dau tren UI |
| Postgres | `localhost:5433` | per-service users |

## Metabase

Metabase dung Postgres lam application database:

```text
postgres://metabase:metabase@postgres:5432/metabase
```

Lan dau vao http://localhost:3000, tao admin user trong UI. Sau do them database
ket noi Trino nhu sau:

```text
Database type: Starburst
Display name: Trino Iceberg
Host: trino
Port: 8080
Catalog: iceberg
Schema: de trong hoac nhap demo neu chi muon browse schema demo
Username: trino
Password: de trong
SSL: off
```

Neu UI co muc connection string, co the dung:

```text
jdbc:trino://trino:8080/iceberg
```

## Smoke test bang Spark SQL

```powershell
docker compose run --rm spark-sql -e "CREATE NAMESPACE IF NOT EXISTS nessie.demo; CREATE TABLE IF NOT EXISTS nessie.demo.events (id BIGINT, name STRING) USING iceberg; INSERT INTO nessie.demo.events VALUES (1, 'hello-lakehouse'); SELECT * FROM nessie.demo.events;"
```

Kiem tra bang Trino:

```powershell
docker compose exec trino trino --catalog iceberg --schema demo --execute "SELECT * FROM events"
```

## Airflow DAG

DAG mau `lakehouse_smoke_dag` submit PySpark job
`/opt/lakehouse/scripts/spark/iceberg_smoke.py` len Spark standalone cluster va
ghi vao `nessie.demo.airflow_events`.

## Kiến trúc Medallion & Cơ chế Tự Kiểm Chứng Dữ Liệu (Evidence-Based Lakehouse)

Dự án được thiết kế theo triết lý **bảo vệ chất lượng dữ liệu bằng cơ chế tự động và bằng chứng (Evidence-based Data Engineering)** xuyên suốt 3 lớp **Bronze $\rightarrow$ Silver $\rightarrow$ Gold** và tầng **Serving (FastAPI + MCP Server)**:

### 1. Tầng Bronze (`scripts/spark/bronze/`) — Tự Kiểm Chứng "Đã Thu Thập Đúng" & Tối Ưu Unit Cost
Thay vì chỉ kiểm tra `HTTP 200`, mọi nguồn dữ liệu đều được kiểm tra cấu trúc bên trong payload thô (`raw`) và ghi nhận vào bảng kiểm toán **`nessie.bronze.ingestion_log`** có **Grain = `(source_name, data_date)`**:

* **State Machine 5 trạng thái**:
  * `VALID`: Đạt 100% kỳ vọng điểm dữ liệu (`expected_items == actual_items`, ví dụ `816/816` điểm nhiệt độ `34 tỉnh × 24 giờ` cho `open-meteo`, `>= 24` chu kỳ phụ tải cho `nsmo`, `>= 30` hồ chứa hợp lệ cho `evn_hydro`, `>= 1` bài báo vận hành cho `evn`).
  * `INCOMPLETE`: Nguồn đã phản hồi nhưng dữ liệu bị khuyết một phần (ví dụ mới có một số giờ trong ngày hoặc nhiều hồ báo `"KXD"`).
  * `EMPTY`: Payload hợp lệ nhưng mảng dữ liệu trống (`data: []` hoặc `0` điểm hợp lệ).
  * `FAILED`: Lỗi kết nối hoặc định dạng.
  * `EXHAUSTED`: Sau $\ge 3$ lần thử (`attempt_count >= 3`) đối với các ngày trong quá khứ cách hiện tại $> 10$ ngày (như ngày nghỉ lễ/Chủ nhật không có bài báo EVN hoặc ngày lịch sử nguồn thực sự không có dữ liệu), hệ thống tự động đóng lại thành `EXHAUSTED` để **không tốn tài nguyên (Unit Cost) cào lại mãi mãi**.
* **Quy tắc bảo vệ bất biến (`merge_ingestion_log`)**: Một khi ngày `data_date` đã đạt `VALID`, không một lần chạy lỗi mạng (`FAILED`/`EMPTY`) nào về sau được phép ghi đè làm mất `bronze_key` chuẩn cũ.

### 2. Tầng Silver (`scripts/spark/silver/`) — Quality Gatekeeper, Entity Resolution & Evidence Status
* **Quality Gatekeeper (`get_valid_bronze_keys_to_process`)**: Tầng Silver chỉ cho phép các `bronze_key` đã được chứng nhận `status = 'VALID'` trong `ingestion_log` đi vào lớp làm sạch.
* **Entity Resolution (`nessie.silver.dim_grid_entities`)**: Đối sánh và hợp nhất tên hồ chứa thủy điện (`evn_hydro`), tỉnh/thành phố (`open_meteo`) và miền điện lực (`NORTH`, `CENTRAL`, `SOUTH` của `nsmo` & `evn`) về một mã định danh chuẩn duy nhất **`jc_entity_id`** (ví dụ `VN-RES-SL01` cho Thủy điện Sơn La).
* **Kiểm chứng bằng chứng (`evidence_status`)**: Trong `transform_evn.py`, nếu cấu trúc bài báo thay đổi khiến không thể trích xuất số liệu với bằng chứng rõ ràng, bản ghi được đánh dấu tường minh là `UNVERIFIABLE` (*"không thể xác định"*) thay vì suy đoán sai lệch.

### 3. Tầng Gold (`scripts/spark/gold/`) — Tín Hiệu Hành Động & Giám Sát SLA
* **`nessie.gold.sla_data_quality_summary`**: Tổng hợp SLA độ đầy đủ (`completeness_sla_pct`), độ mới (`freshness_lag_days`) và trạng thái sức khỏe pipeline (`HEALTHY`, `DEGRADED`, `BREACHED`) cho từng nguồn.
* **`nessie.gold.fact_daily_grid_health`**: Bảng tổng hợp toàn cảnh Thời tiết + Hồ chứa thủy điện + Phụ tải NSMO + Báo cáo vận hành EVN theo ngày.
* **`nessie.gold.fact_actionable_signals`**: Phát hiện các tín hiệu vận hành quan trọng kèm mã bằng chứng (`evidence_bronze_key`):
  * `LOW_RESERVOIR_HEADROOM`: Hồ chứa về gần mực nước chết ($\le 2.0\text{ m}$).
  * `SPILLWAY_FLOOD_DISCHARGE`: Hồ chứa đang mở cửa xả lũ (`open_spillway_gates > 0`).
  * `CROSS_SOURCE_LOAD_DISCREPANCY`: Phát hiện độ lệch công suất đỉnh giữa API NSMO và báo cáo EVN $> 10\%$.

### 4. Tầng Phục Vụ & AI Agent Integration (`serving/api_and_mcp.py`)
Cung cấp **FastAPI REST API** (`/api/v1/sla`, `/api/v1/signals`, `/api/v1/entities/{jc_entity_id}`) và **MCP Server (`/mcp`)** theo chuẩn Model Context Protocol, cho phép các AI Agent (**Claude, Cursor**) truy vấn trực tiếp dữ liệu Lakehouse thông qua Trino kèm trích dẫn bằng chứng gốc.

### Truy vấn SQL kiểm tra nhanh chất lượng dữ liệu trên Trino / Spark SQL

```sql
SELECT
    source_name,
    status,
    COUNT(*) AS total_days,
    MIN(data_date) AS min_date,
    MAX(data_date) AS max_date
FROM nessie.bronze.ingestion_log
GROUP BY source_name, status
ORDER BY source_name, status;
```

## Project layout

```text
airflow/dags/     Airflow DAG điều phối khép kín Bronze -> Silver -> Gold
config/           Runtime config cho Spark và Trino
infra/docker/     Dockerfiles và image requirements
infra/postgres/   Postgres bootstrap scripts
scripts/spark/    PySpark jobs chia theo 3 tầng: bronze/, silver/, gold/
serving/          FastAPI Data API & Model Context Protocol (MCP) Server
```

## Stop/reset

```powershell
docker compose down
```

Xoa ca data volumes:

```powershell
docker compose down -v
```
