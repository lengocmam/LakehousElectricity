from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


BRONZE_NAMESPACE = "nessie.bronze"
INGESTION_LOG_TABLE = f"{BRONZE_NAMESPACE}.ingestion_log"

BRONZE_RAW_SCHEMA = StructType([
    StructField("bronze_key", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("source_url", StringType(), False),
    StructField("source_data_date", StringType(), True),
    StructField("source_data_start_date", StringType(), True),
    StructField("source_data_end_date", StringType(), True),
    StructField("batch_id", StringType(), False),
    StructField("ingestion_timestamp", StringType(), False),
    StructField("ingest_date", StringType(), False),
    StructField("raw", StringType(), False),
])

INGESTION_LOG_SCHEMA = StructType([
    StructField("source_name", StringType(), False),
    StructField("data_date", StringType(), False),
    StructField("bronze_key", StringType(), True),
    StructField("batch_id", StringType(), False),
    StructField("status", StringType(), False),
    StructField("expected_items", IntegerType(), True),
    StructField("actual_items", IntegerType(), True),
    StructField("attempt_count", IntegerType(), False),
    StructField("request_latency_ms", IntegerType(), True),
    StructField("payload_bytes", IntegerType(), True),
    StructField("unit_cost_ms_per_item", DoubleType(), True),
    StructField("note", StringType(), True),
    StructField("checked_at", StringType(), False),
])

INGESTION_LOG_COST_COLUMNS = (
    ("request_latency_ms", "INT"),
    ("payload_bytes", "INT"),
    ("unit_cost_ms_per_item", "DOUBLE"),
)


def ensure_ingestion_log_table(
    spark: SparkSession,
    log_table: str = INGESTION_LOG_TABLE,
) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {BRONZE_NAMESPACE}")
    if not spark.catalog.tableExists(log_table):
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {log_table} (
                source_name STRING,
                data_date STRING,
                bronze_key STRING,
                batch_id STRING,
                status STRING,
                expected_items INT,
                actual_items INT,
                attempt_count INT,
                request_latency_ms INT,
                payload_bytes INT,
                unit_cost_ms_per_item DOUBLE,
                note STRING,
                checked_at STRING
            )
            USING iceberg
            PARTITIONED BY (source_name)
            """
        )
    else:
        existing_cols = {f.name for f in spark.table(log_table).schema.fields}
        missing = [
            f"{col_name} {col_type}"
            for col_name, col_type in INGESTION_LOG_COST_COLUMNS
            if col_name not in existing_cols
        ]
        if missing:
            spark.sql(f"ALTER TABLE {log_table} ADD COLUMNS ({', '.join(missing)})")


def get_dates_to_crawl(
    spark: SparkSession,
    source_name: str,
    start_date: date,
    end_date: date,
    recent_days: int = 0,
    log_table: str = INGESTION_LOG_TABLE,
) -> list[date]:
    """
    Trả về danh sách các ngày trong khoảng [start_date, end_date] cần thu thập:
    - Chưa xuất hiện trong bảng nessie.bronze.ingestion_log, HOẶC
    - Có status IN ('INCOMPLETE', 'EMPTY', 'FAILED') (tức chưa VALID và chưa EXHAUSTED),
    - Cộng thêm các ngày chưa VALID trong `recent_days` gần nhất.
    """
    if start_date > end_date:
        return []

    ensure_ingestion_log_table(spark, log_table=log_table)

    total_days = (end_date - start_date).days + 1
    all_dates = {
        start_date + timedelta(days=offset)
        for offset in range(total_days)
    }

    rows = (
        spark.table(log_table)
        .filter(f"source_name = '{source_name}'")
        .select("data_date", "status")
        .collect()
    )

    settled_dates: set[date] = set()
    valid_dates: set[date] = set()

    for row in rows:
        try:
            d = date.fromisoformat(row["data_date"])
        except (TypeError, ValueError):
            continue

        status = (row["status"] or "").upper()
        if status == "VALID":
            valid_dates.add(d)
            settled_dates.add(d)
        elif status == "EXHAUSTED":
            settled_dates.add(d)

    dates_to_crawl = all_dates - settled_dates

    if recent_days > 0:
        recent_start = max(start_date, end_date - timedelta(days=recent_days - 1))
        recent_span = (end_date - recent_start).days + 1
        for offset in range(recent_span):
            d = recent_start + timedelta(days=offset)
            if d not in valid_dates:
                dates_to_crawl.add(d)

    return sorted(dates_to_crawl)


def merge_ingestion_log(
    spark: SparkSession,
    log_rows: list[dict],
    log_table: str = INGESTION_LOG_TABLE,
    max_attempts: int = 3,
    exhausted_after_days: int = 10,
) -> None:
    """
    Cập nhật kết quả kiểm chứng từng ngày vào bảng `nessie.bronze.ingestion_log`
    theo khóa (source_name, data_date):
    - Nếu đạt VALID: lưu status = 'VALID' và bronze_key vừa cào.
    - Nếu chưa đạt (INCOMPLETE, EMPTY, FAILED): tăng attempt_count + 1.
      Nếu attempt_count >= max_attempts và data_date cách ngày hiện tại > exhausted_after_days
      thì chuyển sang EXHAUSTED.
    - Bảo vệ dữ liệu: Nếu ngày đó trong bảng Log đã VALID từ trước thì không bao giờ
      cho phép một lần chạy lỗi/rỗng ghi đè làm mất trạng thái VALID cũ.
    """
    if not log_rows:
        return

    ensure_ingestion_log_table(spark, log_table=log_table)

    # Khử trùng lặp trong bộ nhớ theo (source_name, data_date), ưu tiên bản ghi VALID
    status_priority = {
        "VALID": 5,
        "INCOMPLETE": 4,
        "EXHAUSTED": 3,
        "EMPTY": 2,
        "FAILED": 1,
    }
    deduped: dict[tuple[str, str], dict] = {}
    for item in log_rows:
        key = (str(item["source_name"]), str(item["data_date"]))
        actual_val = (
            int(item["actual_items"])
            if item.get("actual_items") is not None
            else 0
        )
        latency_val = (
            int(item["request_latency_ms"])
            if item.get("request_latency_ms") is not None
            else None
        )
        bytes_val = (
            int(item["payload_bytes"])
            if item.get("payload_bytes") is not None
            else None
        )
        unit_cost_val = item.get("unit_cost_ms_per_item")
        if unit_cost_val is None and latency_val is not None and actual_val > 0:
            unit_cost_val = round(float(latency_val) / float(actual_val), 4)
        elif unit_cost_val is not None:
            unit_cost_val = float(unit_cost_val)

        normalized = {
            "source_name": str(item["source_name"]),
            "data_date": str(item["data_date"]),
            "bronze_key": item.get("bronze_key"),
            "batch_id": str(item["batch_id"]),
            "status": str(item["status"]).upper(),
            "expected_items": (
                int(item["expected_items"])
                if item.get("expected_items") is not None
                else None
            ),
            "actual_items": actual_val,
            "attempt_count": int(item.get("attempt_count", 1)),
            "request_latency_ms": latency_val,
            "payload_bytes": bytes_val,
            "unit_cost_ms_per_item": unit_cost_val,
            "note": item.get("note"),
            "checked_at": str(item["checked_at"]),
        }
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = normalized
        else:
            p_new = status_priority.get(normalized["status"], 0)
            p_old = status_priority.get(existing["status"], 0)
            if (p_new, normalized["actual_items"] or 0) >= (p_old, existing["actual_items"] or 0):
                deduped[key] = normalized

    df_updates = spark.createDataFrame(list(deduped.values()), schema=INGESTION_LOG_SCHEMA)
    temp_view = "tmp_ingestion_log_updates"
    df_updates.createOrReplaceTempView(temp_view)

    merge_sql = f"""
    MERGE INTO {log_table} AS t
    USING {temp_view} AS s
    ON t.source_name = s.source_name AND t.data_date = s.data_date
    WHEN MATCHED AND t.status = 'VALID' AND s.status <> 'VALID' THEN
        UPDATE SET
            t.checked_at = s.checked_at,
            t.note = CONCAT(COALESCE(t.note, ''), ' | Kept VALID over ', s.status, ' (batch ', s.batch_id, ')')
    WHEN MATCHED AND s.status = 'VALID' THEN
        UPDATE SET
            t.bronze_key = s.bronze_key,
            t.batch_id = s.batch_id,
            t.status = 'VALID',
            t.expected_items = s.expected_items,
            t.actual_items = s.actual_items,
            t.attempt_count = COALESCE(t.attempt_count, 0) + 1,
            t.request_latency_ms = COALESCE(s.request_latency_ms, t.request_latency_ms),
            t.payload_bytes = COALESCE(s.payload_bytes, t.payload_bytes),
            t.unit_cost_ms_per_item = COALESCE(s.unit_cost_ms_per_item, t.unit_cost_ms_per_item),
            t.note = s.note,
            t.checked_at = s.checked_at
    WHEN MATCHED AND s.status <> 'VALID' THEN
        UPDATE SET
            t.bronze_key = COALESCE(s.bronze_key, t.bronze_key),
            t.batch_id = s.batch_id,
            t.status = CASE
                WHEN s.status = 'EXHAUSTED' THEN 'EXHAUSTED'
                WHEN (COALESCE(t.attempt_count, 0) + 1) >= {max_attempts}
                     AND datediff(current_date(), to_date(s.data_date)) > {exhausted_after_days}
                THEN 'EXHAUSTED'
                ELSE s.status
            END,
            t.expected_items = COALESCE(s.expected_items, t.expected_items),
            t.actual_items = s.actual_items,
            t.attempt_count = COALESCE(t.attempt_count, 0) + 1,
            t.request_latency_ms = COALESCE(s.request_latency_ms, t.request_latency_ms),
            t.payload_bytes = COALESCE(s.payload_bytes, t.payload_bytes),
            t.unit_cost_ms_per_item = COALESCE(s.unit_cost_ms_per_item, t.unit_cost_ms_per_item),
            t.note = s.note,
            t.checked_at = s.checked_at
    WHEN NOT MATCHED THEN
        INSERT (
            source_name,
            data_date,
            bronze_key,
            batch_id,
            status,
            expected_items,
            actual_items,
            attempt_count,
            request_latency_ms,
            payload_bytes,
            unit_cost_ms_per_item,
            note,
            checked_at
        )
        VALUES (
            s.source_name,
            s.data_date,
            s.bronze_key,
            s.batch_id,
            CASE
                WHEN s.status = 'VALID' THEN 'VALID'
                WHEN s.status = 'EXHAUSTED' THEN 'EXHAUSTED'
                WHEN s.attempt_count >= {max_attempts}
                     AND datediff(current_date(), to_date(s.data_date)) > {exhausted_after_days}
                THEN 'EXHAUSTED'
                ELSE s.status
            END,
            s.expected_items,
            s.actual_items,
            COALESCE(s.attempt_count, 1),
            s.request_latency_ms,
            s.payload_bytes,
            s.unit_cost_ms_per_item,
            s.note,
            s.checked_at
        )
    """
    spark.sql(merge_sql)
    print(f"Merged {len(deduped)} row(s) into {log_table}.")


def write_raw_bronze(
    spark: SparkSession,
    records: list[dict],
    table_name: str,
    ingest_date: str,
    *,
    namespace: str | None = None,
    fail_on_empty: bool = False,
) -> bool:
    if not records:
        if fail_on_empty:
            raise RuntimeError("No records were successfully crawled.")
        print("No records to write.")
        return False

    normalized_records = [
        {
            "bronze_key": r["bronze_key"],
            "source_name": r["source_name"],
            "source_url": r["source_url"],
            "source_data_date": r.get("source_data_date"),
            "source_data_start_date": r.get("source_data_start_date"),
            "source_data_end_date": r.get("source_data_end_date"),
            "batch_id": r["batch_id"],
            "ingestion_timestamp": r["ingestion_timestamp"],
            "ingest_date": r["ingest_date"],
            "raw": r["raw"],
        }
        for r in records
    ]

    df = spark.createDataFrame(normalized_records, schema=BRONZE_RAW_SCHEMA)
    print(f"Records to write: {len(normalized_records)}")
    print(f"Ingest date: {ingest_date}")

    if namespace:
        print(f"Creating namespace if needed: {namespace}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    if spark.catalog.tableExists(table_name):
        print("Table exists, appending new batch records...")
        df.writeTo(table_name).append()
    else:
        print("Table does not exist, creating and writing...")
        df.writeTo(table_name).using("iceberg").partitionedBy("ingest_date").create()

    print("Bronze ingestion completed successfully.")
    return True
